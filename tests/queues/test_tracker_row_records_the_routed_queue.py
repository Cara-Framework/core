"""The tracker row of a dispatch records the queue the delivery was routed to.

Live defect: ``push`` routes a job to an explicit ``queue`` option over the
job's own ``queue``, but ``_create_job_record`` read ``job.queue`` first, so
a job dispatched to an explicit queue showed under its class default in
tracking (``queue:monitor``, failure lists, purge scopes). Before the fix
(``git show HEAD:cara/queues/drivers/_AMQPDelivery.py``) the first two tests
recorded ``default``.

A fake driver and tracker; the registration stops right after the tracker
write, so no ledger or broker is involved.
"""

from __future__ import annotations

import contextlib
from types import SimpleNamespace

import pytest

from cara.queues.drivers import _AMQPDelivery


class _Job:
    queue = "default"


class _Tracker:
    job_model = object()

    def __init__(self) -> None:
        self.queues: list[str] = []

    def create_job_record(self, *, queue, **_fields) -> int:
        self.queues.append(queue)
        return 9


class _StopAfterTheTrackerWrite(Exception):
    pass


class _Driver:
    def __init__(self) -> None:
        self.tracker = _Tracker()
        database = SimpleNamespace(transaction=contextlib.nullcontext)
        self._delivery_store = SimpleNamespace(
            _db=lambda: database,
            execution_timeout_for=self._stop,
        )

    @staticmethod
    def _stop(_job):
        raise _StopAfterTheTrackerWrite

    @staticmethod
    def require_canonical_queue(queue_name):
        assert queue_name in {"default", "sync"}, queue_name
        return queue_name

    def _resolve_job_tracker(self):
        return self.tracker

    _create_job_record = _AMQPDelivery._amqp_delivery_create_job_record


def test_registration_records_the_explicitly_routed_queue() -> None:
    driver = _Driver()

    with pytest.raises(_StopAfterTheTrackerWrite):
        _AMQPDelivery._amqp_delivery_register_immediate_delivery(
            driver,
            job=_Job(),
            job_id="33333333-3333-4333-8333-333333333333",
            merged_opts={},
            job_opts={"queue": "sync"},
        )

    assert driver.tracker.queues == ["sync"]


def test_the_routed_queue_wins_over_the_jobs_own() -> None:
    driver = _Driver()

    driver._create_job_record(_Job(), "job-1", {"queue": "default"}, queue="sync")

    assert driver.tracker.queues == ["sync"]


def test_without_a_routed_queue_the_jobs_own_queue_is_recorded_as_before() -> None:
    """The delayed path (``DurableDelayedJobStore.schedule``) passes none yet."""
    driver = _Driver()
    job = _Job()
    job.queue = "sync"

    driver._create_job_record(job, "job-1", {"queue": "default"})

    assert driver.tracker.queues == ["sync"]
