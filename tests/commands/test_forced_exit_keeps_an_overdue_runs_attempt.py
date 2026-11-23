"""A forced worker exit refunds an interrupted run's attempt, not a hung one's.

Live defect: on a forced exit ``LeaseHeartbeat.release_all()`` calls every
live run's ``abandon_execution``, which REFUNDS the attempt (a deploy is not
a crash). It refunded a run already past its own timeout too, so a job that
hangs deterministically was never counted whenever its hang met a worker
stop (five releases left ``attempts`` at 0) and never reached the crash
ceiling. Before the fix (``git show HEAD:cara/commands/core/_JobExecution.py``
and ``HEAD:cara/queues/delivery/QueueJobDeliveryStore.py``) the release
always called ``abandon_execution(job_id, token)`` and the store had no
``refund_attempt``: the overdue tests below fail.

No database: a recording ``DB`` for the store, a recording store for the
release, and a stepped monotonic clock for the run's budget.
"""

from __future__ import annotations

import contextlib
import importlib
from types import SimpleNamespace

import pendulum
import pytest

from cara.queues import LeaseHeartbeat
from cara.queues.delivery import DeliveryClaim, QueueJobDeliveryStore

_LEASE = importlib.import_module("cara.commands.core._DeliveryLease")


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    now = [1000.0]
    monkeypatch.setattr(_LEASE, "time", SimpleNamespace(monotonic=lambda: now[0]))
    return now


def _claim(*, deadline_in_seconds: int = 600) -> DeliveryClaim:
    return DeliveryClaim(
        "claimed",
        "lease-1",
        lease_deadline=pendulum.now("UTC").add(seconds=deadline_in_seconds),
    )


# ── the run's budget ─────────────────────────────────────────────────────
def test_a_run_inside_its_timeout_has_not_outlived_it(clock) -> None:
    budget = _LEASE._RunBudget(_claim().lease_deadline)
    budget.begin(30)
    clock[0] += 29

    assert budget.outlived() is False


def test_a_run_past_its_timeout_has_outlived_it(clock) -> None:
    budget = _LEASE._RunBudget(_claim().lease_deadline)
    budget.begin(30)
    clock[0] += 30

    assert budget.outlived() is True


def test_a_run_whose_lease_deadline_passed_has_outlived_it_before_the_body(
    clock,
) -> None:
    assert _LEASE._RunBudget(_claim(deadline_in_seconds=-1).lease_deadline).outlived()
    assert not _LEASE._RunBudget(_claim().lease_deadline).outlived()


# ── the forced-exit release ──────────────────────────────────────────────
class _Store:
    execution_heartbeat_seconds = 600  # no renewal fires during a test

    def __init__(self) -> None:
        self.abandoned: list[tuple] = []

    def renew_execution(self, *_args, **_kwargs) -> bool:
        return True

    def abandon_execution(self, job_id, lease_token, **kwargs) -> None:
        self.abandoned.append((job_id, lease_token, kwargs))


class _Tracker:
    def __init__(self) -> None:
        self.statuses: list[tuple] = []

    def update_job_status_strict(self, db_job_id, status) -> None:
        self.statuses.append((db_job_id, status))


def _force_exit_with_a_run(clock, *, ran_for: float, timeout: float):
    store, tracker = _Store(), _Tracker()
    claim = _claim()
    budget = _LEASE._RunBudget(claim.lease_deadline)
    _LEASE._hold_delivery_lease(store, "job-1", claim, tracker, 91, budget)
    budget.begin(timeout)
    clock[0] += ran_for
    released = LeaseHeartbeat.release_all(timeout_seconds=5.0)
    return released, store, tracker


def test_a_forced_exit_inside_the_timeout_refunds_the_attempt(clock) -> None:
    released, store, tracker = _force_exit_with_a_run(clock, ran_for=5, timeout=30)

    assert released == 1
    assert store.abandoned == [("job-1", "lease-1", {})]
    assert tracker.statuses == [(91, "pending")]
    assert LeaseHeartbeat.live() == []


def test_a_forced_exit_past_the_timeout_keeps_the_attempt_spent(clock) -> None:
    released, store, tracker = _force_exit_with_a_run(clock, ran_for=31, timeout=30)

    assert released == 1
    assert store.abandoned == [("job-1", "lease-1", {"refund_attempt": False})]
    # Everything else about the release is unchanged.
    assert tracker.statuses == [(91, "pending")]
    assert LeaseHeartbeat.live() == []


# ── the store ────────────────────────────────────────────────────────────
class _DB:
    def __init__(self) -> None:
        self.statements: list[str] = []

    @contextlib.contextmanager
    def transaction(self):
        yield self

    def statement(self, query: str, _bindings=()) -> int:
        self.statements.append(query)
        return 1

    def select_one(self, query: str, _bindings=()):
        # The kept-attempt abandon first looks for its row at the crash
        # ceiling; none here, so the ordinary release follows (that path is
        # covered by tests/queues/test_abandon_at_the_crash_ceiling_...).
        self.statements.append(query)
        return None


def _store() -> tuple[QueueJobDeliveryStore, _DB]:
    database = _DB()
    application = SimpleNamespace(has=lambda key: key == "DB", make=lambda _k: database)
    store = QueueJobDeliveryStore(application, None, {"canonical_queues": ["sync"]})
    return store, database


def _refunds(database: _DB) -> list[str]:
    return [sql for sql in database.statements if "attempts = attempts - 1" in sql]


def test_the_store_refunds_by_default() -> None:
    store, database = _store()

    store.abandon_execution("job-1", "lease-1")

    assert len(_refunds(database)) == 1
    assert any("SET status = %s" in sql for sql in database.statements)


def test_the_store_keeps_the_attempt_when_told_and_still_releases() -> None:
    store, database = _store()

    store.abandon_execution("job-1", "lease-1", refund_attempt=False)

    assert _refunds(database) == []
    assert any("SET status = %s" in sql for sql in database.statements)
