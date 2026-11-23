"""A per-dispatch retry budget that cannot take effect is refused, not dropped.

``job.attempts(7)`` (and ``job.max_attempts = 7``) stored the value on the
instance, but a queued job crosses the broker as its signed constructor
arguments only: the worker rebuilds it and reads the CLASS's budget, so the
7 was silently ignored and the job ran with the class's 3. The budget is
declared on the class; an instance value that differs from it now raises
where it is set.
"""

from __future__ import annotations

import pytest

from cara.exceptions import InvalidArgumentException
from cara.queues import Bus
from cara.queues.contracts import BaseJob


class _PlainJob(BaseJob):
    default_queue = "default"

    async def handle(self):
        return None


class _FiveTries(_PlainJob):
    default_retry_attempts = 5


class _OneShot(_PlainJob):
    max_attempts = 1


def test_attempts_that_differs_from_the_class_is_refused_with_the_fix():
    with pytest.raises(InvalidArgumentException, match="max_attempts = 7"):
        _PlainJob().attempts(7)


def test_assigning_max_attempts_on_an_instance_is_refused():
    job = _PlainJob()

    with pytest.raises(InvalidArgumentException, match="on _PlainJob"):
        job.max_attempts = 7

    assert job.max_attempts == 3


def test_the_class_declared_budget_is_what_the_worker_reads():
    assert (_FiveTries().max_attempts, _OneShot().max_attempts) == (5, 1)


def test_the_queue_options_report_the_class_budget():
    assert _FiveTries().get_queue_options()["attempts"] == 5


def test_restating_the_class_budget_is_a_no_op():
    assert _FiveTries().attempts(5).max_attempts == 5


def test_a_boolean_is_not_mistaken_for_a_one_attempt_budget():
    with pytest.raises(InvalidArgumentException):
        _PlainJob().attempts(True).max_attempts  # noqa: B018 - must not get here


def test_the_job_a_worker_rebuilds_has_the_same_budget_as_the_dispatched_one():
    dispatched = _FiveTries()
    rebuilt = _FiveTries(**Bus.get_dispatch_params(dispatched))

    assert rebuilt.max_attempts == dispatched.max_attempts == 5
