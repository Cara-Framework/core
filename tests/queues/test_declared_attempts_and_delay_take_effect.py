"""A job's declared attempts and delay are the values that take effect.

``SendMailableJob.default_retry_attempts = 5`` was written to
``_retry_attempts``, which nothing read — the worker reads ``max_attempts`` —
so queued mail got the default 3 tries. Likewise
``default_delay`` / ``job.delay(n)`` wrote ``_delay``, which
``PendingDispatch`` never read: it started at None and pushed the job at once.
"""

from __future__ import annotations

import importlib

import pytest

from cara.mail.jobs import SendMailableJob
from cara.queues.contracts import BaseJob, PendingDispatch

pending_module = importlib.import_module("cara.queues.contracts.PendingDispatch")


class _PlainJob(BaseJob):
    default_queue = "default"

    async def handle(self):
        return None


class _SingleShotJob(_PlainJob):
    max_attempts = 1


class _DeferredJob(_PlainJob):
    default_delay = 30


class _Queue:
    """Records whether a dispatch went through ``later`` or ``push``."""

    def __init__(self) -> None:
        self.calls: list[tuple] = []

    def later(self, delay, job, **_options):
        self.calls.append(("later", delay))
        return "job-later"

    def push(self, job, **_options):
        self.calls.append(("push", None))
        return "job-now"


@pytest.fixture
def queue(monkeypatch):
    recorder = _Queue()
    monkeypatch.setattr(pending_module, "Queue", recorder)
    return recorder


def test_queued_mail_gets_its_declared_five_attempts():
    job = SendMailableJob("cara.mail.Mailable.Mailable", {})
    assert job.max_attempts == 5


def test_a_plain_job_keeps_the_worker_default():
    assert _PlainJob().max_attempts == 3


def test_a_class_level_max_attempts_still_wins():
    assert _SingleShotJob().max_attempts == 1


def test_attempts_confirming_the_class_budget_is_accepted():
    # A different value is refused: test_per_dispatch_attempts_is_refused.
    assert _PlainJob().attempts(3).max_attempts == 3


def test_a_declared_default_delay_reaches_queue_later(queue):
    PendingDispatch(_DeferredJob()).dispatch()
    assert queue.calls == [("later", 30)]


def test_job_delay_reaches_queue_later(queue):
    PendingDispatch(_PlainJob().delay(12)).dispatch()
    assert queue.calls == [("later", 12)]


def test_the_builders_delay_overrides_the_jobs(queue):
    PendingDispatch(_DeferredJob()).delay(5).dispatch()
    assert queue.calls == [("later", 5)]


def test_a_job_without_a_delay_is_pushed_now(queue):
    PendingDispatch(_PlainJob()).dispatch()
    assert queue.calls == [("push", None)]
