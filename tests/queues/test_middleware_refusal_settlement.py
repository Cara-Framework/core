"""A refused job is never settled as a completed one.

Job middleware used to short-circuit by RETURNING ``None``. ``None`` is also
the normal success return of an async handler (``Bus._run_sync`` documents
exactly that), so the worker could not tell the two apart: it ran
``complete_with_tracker`` and ACKed the delivery for a job whose body never
executed. Two jobs over one rate limit meant one was silently discarded and
reported as done.

Refusal is now a raise, and the exception opts into the STARVATION budget
rather than the FAILURE budget — a job that never got a slot must not spend
the retry attempts reserved for jobs that ran and blew up.
"""

from __future__ import annotations

import asyncio

import pytest

from cara.commands.core.QueueWorkCommand import JobProcessor
from cara.queues.contracts.CancellableJob import (
    JobCancelledException,
    JobThrottledException,
)
from cara.queues.middleware.RateLimited import RateLimited


class _Job:
    timeout = 60


async def _body(job):  # pragma: no cover - must never run in these tests
    raise AssertionError("the job body must not run when the gate is closed")


def test_rate_limited_raises_instead_of_returning() -> None:
    mw = RateLimited(max_attempts=1, decay_seconds=60, key="probe-raises")

    ran: list[str] = []

    async def _first(job):
        ran.append("first")
        return "done"

    assert asyncio.run(mw.handle(_Job(), _first)) == "done"

    with pytest.raises(JobThrottledException) as raised:
        asyncio.run(mw.handle(_Job(), _body))

    assert ran == ["first"], "only the admitted job may run"
    assert raised.value.key == "probe-raises"
    # The caller is told WHEN the gate reopens, not just that it is shut.
    assert 1 <= raised.value.retry_after <= 61


def test_throttle_and_cancellation_declare_their_settlement_lane() -> None:
    """The worker reads these two flags via ``getattr`` — they are the contract."""
    assert JobThrottledException.is_throttle is True
    assert JobCancelledException.do_not_retry is True
    # A throttle is not a failure and a cancellation is not a throttle.
    assert getattr(JobThrottledException, "do_not_retry", False) is False
    assert getattr(JobCancelledException, "is_throttle", False) is False


def test_a_refused_job_is_routed_by_the_starvation_budget() -> None:
    """The refusal reaches the worker as a throttle, not as a fault.

    The exact budget ARITHMETIC belongs to
    ``tests/commands/test_queue_work_throttle_retry.py``; what this file pins
    is only that a middleware refusal enters that lane at all — which is what
    ``is_throttle`` on the raised exception decides.
    """

    class _Instance:
        max_attempts = 3
        max_throttle_attempts = 5

    instance = _Instance()
    # Failure budget spent, starvation budget untouched: a refusal is still
    # live work, a fault is finished.
    msg = {"attempts": 3, "throttle_attempts": 0}

    assert JobProcessor._should_retry_job(msg, instance, JobThrottledException())
    assert not JobProcessor._should_retry_job(msg, instance, RuntimeError("boom"))
