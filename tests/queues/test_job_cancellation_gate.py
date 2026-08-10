"""The cancellation gate must not answer "keep going" when it does not know.

An operator cancels a long-running bulk push. Two of the three layers that
carry that decision used to discard it silently:

* ``JobTracker.should_job_continue`` wrapped the whole method in
  ``except Exception: return True``, so one failed read of the jobs table
  (pool exhaustion, a transient drop, a savepoint poisoned by an earlier
  23505) answered "continue" — and the cancelled job's writes kept landing
  on the channel with a WARNING as the only trace.
* ``Trackable._validate_or_cancel`` re-raised the cancellation by comparing
  ``e.__class__.__name__`` against the literal ``"JobCancelledException"``.
  A product subclass (``class TenantJobCancelled(JobCancelledException)``)
  reports a different name, so the comparison failed, the cancellation was
  downgraded to a WARNING and the method RETURNED NORMALLY.

Absence of the feature stays permissive on purpose: no tracker model means
no cancellation record can exist, which is not the same as "unknown".
"""

from __future__ import annotations

import pytest

from cara.queues.contracts.CancellableJob import JobCancelledException
from cara.queues.tracking.JobTracker import JobTracker
from cara.queues.tracking.Trackable import Trackable


class _Record:
    def __init__(self, status: str) -> None:
        self.status = status


class _Query:
    def __init__(self, model) -> None:
        self._model = model

    def first(self):
        if self._model.read_raises is not None:
            raise self._model.read_raises
        return self._model.record


class _JobModel:
    STATUS_CANCELLED = "cancelled"

    def __init__(self, record=None, read_raises: Exception | None = None) -> None:
        self.record = record
        self.read_raises = read_raises

    def where(self, *_args):
        return _Query(self)


def test_a_live_job_continues() -> None:
    tracker = JobTracker(job_model=_JobModel(record=_Record("processing")))
    assert tracker.should_job_continue("uid") is True


def test_a_cancelled_job_stops() -> None:
    tracker = JobTracker(job_model=_JobModel(record=_Record("cancelled")))
    assert tracker.should_job_continue("uid") is False


def test_a_failed_read_is_not_an_answer(caplog) -> None:
    """A DB blip must propagate, not resolve to "continue".

    Old behaviour: ``return True`` — the cancelled job carried on.
    """
    tracker = JobTracker(job_model=_JobModel(read_raises=RuntimeError("pool empty")))

    with caplog.at_level("ERROR"), pytest.raises(RuntimeError):
        tracker.should_job_continue("uid")

    assert any("refusing to guess" in record.getMessage() for record in caplog.records), (
        "an unreadable gate must say so"
    )


def test_an_unconfigured_tracker_still_permits() -> None:
    """Absence of the tracking model is absence, not an unknown state.

    Documented mode (``JobTracker()`` with no model logs-only), and
    ``QueueProvider`` registers it that way when a product ships no Job
    model. There is no cancellation record to disobey.
    """
    assert JobTracker().should_job_continue("uid") is True


class _CancelledForTenant(JobCancelledException):
    """A product-shaped subclass — the exact case the string check missed."""


class _Tracker:
    def __init__(self, error: Exception) -> None:
        self._error = error

    def validate_job_or_cancel(self, *_args, **_kwargs):
        raise self._error


class _TrackedJob(Trackable):
    def __init__(self, tracker) -> None:
        super().__init__()
        self._job_uid = "uid"
        self._job_tracker = tracker

    def _get_job_tracker(self):
        return self._job_tracker


def test_a_subclassed_cancellation_still_cancels() -> None:
    """``isinstance``, not ``__class__.__name__``.

    Old behaviour: the name did not match the literal, the cancellation was
    logged at WARNING and ``_validate_or_cancel`` returned normally, so the
    job ran to completion after being explicitly cancelled.
    """
    job = _TrackedJob(_Tracker(_CancelledForTenant("cancelled mid-push")))

    with pytest.raises(_CancelledForTenant):
        job._validate_or_cancel("push")


def test_the_base_cancellation_still_cancels() -> None:
    job = _TrackedJob(_Tracker(JobCancelledException("cancelled")))

    with pytest.raises(JobCancelledException):
        job._validate_or_cancel("push")


def test_an_unrelated_tracking_error_does_not_fail_the_job(caplog) -> None:
    """Tracking is observability: it must never fail the job it observes."""
    job = _TrackedJob(_Tracker(RuntimeError("tracker exploded")))

    with caplog.at_level("WARNING"):
        job._validate_or_cancel("push")

    assert any(
        "Failed to validate job continuation" in record.getMessage()
        for record in caplog.records
    )
