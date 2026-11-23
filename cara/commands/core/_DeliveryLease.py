"""The worker's execution lease on one delivery, and what a forced exit owes it."""

from __future__ import annotations

import time
from typing import Any

import pendulum

from cara.queues import LeaseHeartbeat


class _RunBudget:
    """When the job body started and how long it may run.

    A forced worker exit releases every lease the process still holds
    (``LeaseHeartbeat.release_all``), and releasing refunds the attempt: a
    deploy interrupted the run, the job did not fail. A run already past its
    own timeout is different: the timeout could not stop it, so it hangs, and
    refunding it meant a job that hangs deterministically was never counted
    whenever its hang met a worker stop (five releases, ``attempts`` 0) and
    never reached the crash ceiling. Such a run keeps its attempt spent.

    The window is set once, as one tuple, from the worker thread and read from
    the release helper thread; the monotonic clock keeps a wall-clock step
    from deciding it. The claim's ``lease_deadline`` (timeout plus grace, wall
    clock) also counts, for a release that comes before the body started.
    """

    def __init__(self, lease_deadline: Any | None) -> None:
        self._lease_deadline = lease_deadline
        self._window: tuple[float, float] | None = None

    def begin(self, timeout_seconds: float) -> None:
        self._window = (time.monotonic(), float(timeout_seconds))

    def outlived(self) -> bool:
        window = self._window
        if window is not None and time.monotonic() - window[0] >= window[1]:
            return True
        deadline = self._lease_deadline
        return deadline is not None and pendulum.now("UTC") >= deadline


def _hold_delivery_lease(
    delivery_store: Any,
    job_id: str,
    claim: Any,
    tracker: Any,
    db_job_id: Any,
    budget: _RunBudget,
) -> LeaseHeartbeat:
    """Keep this worker's execution lease alive while it runs the delivery.

    The claim takes a short heartbeat lease, renewed every third of it until
    the body ends: a live job keeps its row ``processing``, a dead worker's
    row lapses within one lease and the relay returns it to the outbox. A
    lost lease fences the run. Settlement rides the lease's remaining two
    thirds; the heartbeat stops BEFORE settling, so no renewal races it.

    ``release`` is the forced-exit path (``LeaseHeartbeat.release_all``): a
    job that ignored shutdown cancellation hands its row back to ``pending``
    exactly as the cancellation path does, and the broker redelivers its
    unacknowledged copy once this process's connection closes. A run that
    outlived its budget keeps its attempt (:class:`_RunBudget`), and one
    whose kept attempt reaches the crash ceiling is dead-lettered by the
    store instead of handed back.
    """
    lease_token = claim.lease_token
    deadline = claim.lease_deadline

    def _renew() -> bool:
        return bool(
            delivery_store.renew_execution(job_id, lease_token, deadline=deadline)
        )

    def _release() -> None:
        if budget.outlived():
            settled = delivery_store.abandon_execution(
                job_id, lease_token, refund_attempt=False
            )
        else:
            settled = delivery_store.abandon_execution(job_id, lease_token)
        # A kept attempt at the crash ceiling was dead-lettered and its
        # tracker failed in the store's transaction; resetting it to
        # ``pending`` here would show a dead job as waiting.
        if settled is True:
            return
        if tracker is not None and db_job_id is not None:
            tracker.update_job_status_strict(db_job_id, "pending")

    return LeaseHeartbeat(
        f"queue_job_delivery:{job_id}",
        lease_seconds=delivery_store.execution_heartbeat_seconds,
        renew=_renew,
        release=_release,
    ).start()


def _stop_heartbeat(heartbeat: LeaseHeartbeat | None) -> None:
    if heartbeat is not None:
        heartbeat.stop()
