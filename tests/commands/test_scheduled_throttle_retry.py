"""A scheduled tick the concurrency gate refuses is retried, then counted.

Live 2026-11-01: the scheduler runs jobs inline, ``ConcurrencyLimited``
raised ``ConcurrencyExceeded`` "so the queue runner retries" — and no queue
runner existed, so 15 Amazon fires in one hour did no work, were counted as
plain failures, and ``ScheduledTaskMissed`` could not see them.
"""

from __future__ import annotations

import sys

import pytest

import cara.commands.core._ScheduledThrottle as throttle
from cara.observability import MetricsBase
from cara.queues.middleware.ConcurrencyExceeded import ConcurrencyExceeded
from cara.scheduling.drivers.APSchedulerDriver import APSchedulerDriver  # noqa: F401

# The barrel re-exports the class under the module's dotted name.
driver = sys.modules["cara.scheduling.drivers.APSchedulerDriver"]


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def _throttles(task: str, result: str) -> float:
    return MetricsBase.scheduled_task_throttles_total.labels(
        task=task, result=result
    )._value.get()


def test_a_refused_tick_runs_once_a_slot_frees():
    clock = _Clock()
    outcomes = iter([ConcurrencyExceeded("full"), ConcurrencyExceeded("full"), "ran"])
    before = _throttles("probe_recovered", "recovered")

    def run_once():
        outcome = next(outcomes)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    result = throttle._run_with_throttle_retry(
        "probe_recovered",
        run_once,
        budget_seconds=60,
        sleep=clock.sleep,
        clock=clock,
    )

    assert result == "ran"
    assert clock.sleeps == [2.0, 4.0]
    assert _throttles("probe_recovered", "recovered") == before + 1


def test_a_tick_still_refused_at_its_budget_is_dropped_and_counted():
    clock = _Clock()
    before = _throttles("probe_dropped", "dropped")

    def run_once():
        raise ConcurrencyExceeded("full")

    with pytest.raises(ConcurrencyExceeded):
        throttle._run_with_throttle_retry(
            "probe_dropped",
            run_once,
            budget_seconds=10,
            sleep=clock.sleep,
            clock=clock,
        )

    # 2 + 4, then the 4 seconds left of the budget — never past it.
    assert clock.sleeps == [2.0, 4.0, 4.0]
    assert _throttles("probe_dropped", "dropped") == before + 1


def test_an_interval_job_waits_at_most_half_its_period():
    budget = throttle._throttle_budget_seconds
    assert budget(60) == 30
    assert budget(15 * 60) == throttle._DEFAULT_BUDGET_SECONDS
    assert budget(None) == throttle._DEFAULT_BUDGET_SECONDS


def test_a_dropped_fire_is_its_own_outcome_not_a_failure():
    def callback():
        raise ConcurrencyExceeded("full")

    def outcome_count(outcome: str) -> float:
        return MetricsBase.scheduled_tasks_total.labels(
            task="probe_outcome", outcome=outcome
        )._value.get()

    throttled, failed = outcome_count("throttled"), outcome_count("failure")
    wrapped = driver._instrument_scheduled("probe_outcome", callback)

    with pytest.raises(ConcurrencyExceeded):
        wrapped()

    assert outcome_count("throttled") == throttled + 1
    assert outcome_count("failure") == failed
