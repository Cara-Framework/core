"""Throttled deliveries are BOUNDED and they ESCALATE.

``attempts`` was doing three jobs at once: the failure budget, the backoff
index, and — because a throttle deliberately leaves it frozen — nothing at
all on the throttle path. A job starved by a sustained concurrency limit
therefore came back after ``DEFAULT_RETRY_BACKOFF_SECONDS[0]`` (1 second)
forever: the budget check was permanently ``1 < 3`` → retry, the backoff
index was permanently 0, and every cycle appended a delivery row plus a job
UPDATE and a broker publish to the durable outbox. Nothing ever
dead-lettered, so operators got no terminal signal that the work was not
progressing.

``throttle_attempts`` is the throttle lane's own counter. These tests drive
consecutive throttles through ``_route_failed_message`` and pin: the failure
budget stays frozen, the delay escalates through the schedule, and the chain
terminates in the DLQ with a reason that separates capacity starvation from
job failure.

The harness below feeds each retry's options back into the next message by
hand. That is a SIMULATION of the redelivery hop, not the hop itself — and
saying otherwise is how a counter that the signed envelope silently dropped
still looked bounded here. The hop is pinned where it actually lives, in
``tests/queues/test_signed_json_job_serializer.py``
(``test_throttle_counter_survives_the_signed_round_trip``); if that test goes
red, everything in this file is measuring a value production never sees.
"""

from __future__ import annotations

import builtins
from types import SimpleNamespace

import pytest

from cara.commands.core.JobProcessor import JobProcessor
from cara.queues.retry.Policy import (
    DEFAULT_MAX_THROTTLE_ATTEMPTS,
    DEFAULT_RETRY_BACKOFF_SECONDS,
)


class _Throttled(Exception):
    """Mirrors ``ConcurrencyLimited.ConcurrencyExceeded``'s worker contract."""

    is_throttle = True


class _FacadeApplication:
    def __init__(self, queue_service):
        self.queue_service = queue_service
        self.logger = SimpleNamespace(
            debug=lambda *args, **kwargs: None,
            error=lambda *args, **kwargs: None,
            info=lambda *args, **kwargs: None,
            warning=lambda *args, **kwargs: None,
        )

    def make(self, key):
        if key == "queue":
            return self.queue_service
        if key == "logger":
            return self.logger
        raise KeyError(key)


class _Recorder:
    """Drives the retry chain the way the durable outbox round-trip does."""

    def __init__(self, monkeypatch, instance):
        self.instance = instance
        self.scheduled: list[tuple[int, dict]] = []
        self.dead_lettered: list[str] = []
        self.msg = {
            "attempts": 0,
            "queue": "sync",
            "job_id": "starved-1",
            "db_job_id": 9,
            "_tenant": None,
            "_tenant_mode": "central",
            "_otel": {},
        }
        queue_service = SimpleNamespace(
            driver=lambda: SimpleNamespace(
                _apply_retry_jitter=lambda delay, _instance: delay
            ),
            later=lambda delay, _instance, **options: self.scheduled.append(
                (delay, options)
            ),
        )
        monkeypatch.setattr(
            builtins,
            "app",
            lambda: _FacadeApplication(queue_service),
            raising=False,
        )
        self.delivery_store = SimpleNamespace(
            dead_letter_with_tracker=lambda job_id, token, *, db_job_id, reason: (
                self.dead_lettered.append(reason)
            )
        )

    def fail_once(self, exc: Exception) -> str:
        """Route one failure and feed the republished counters back in."""
        outcome = JobProcessor._route_failed_message(
            channel=SimpleNamespace(
                basic_ack=lambda **_kwargs: None,
                basic_nack=lambda **_kwargs: None,
            ),
            method_frame=SimpleNamespace(delivery_tag=1),
            msg=self.msg,
            instance=self.instance,
            exc=exc,
            queue_name="sync",
            delivery_store=self.delivery_store,
            delivery_lease_token="lease-1",
            tracker=SimpleNamespace(require_job_status_strict=lambda *_args: None),
            db_job_id=9,
        )
        if outcome == "retry_scheduled":
            options = self.scheduled[-1][1]
            self.msg = {
                **self.msg,
                "attempts": options["attempts"],
                "throttle_attempts": options["throttle_attempts"],
            }
        return outcome

    @property
    def delays(self) -> list[int]:
        return [delay for delay, _options in self.scheduled]


def test_throttle_chain_escalates_and_then_dead_letters(monkeypatch):
    """The three symptoms of the frozen counter, pinned together.

    Pre-fix: ``delays`` was ``[1, 1, 1, …]`` forever and this loop never
    reached a terminal outcome.
    """
    recorder = _Recorder(monkeypatch, SimpleNamespace(max_throttle_attempts=4))

    outcomes = [recorder.fail_once(_Throttled("no slot")) for _ in range(4)]

    # (a) The failure budget is untouched — starvation is not a job failure.
    assert [options["attempts"] for _delay, options in recorder.scheduled] == [0, 0, 0]
    assert [options["throttle_attempts"] for _d, options in recorder.scheduled] == [
        1,
        2,
        3,
    ]
    # (b) The delay escalates through the schedule instead of freezing at 1s.
    assert recorder.delays == list(DEFAULT_RETRY_BACKOFF_SECONDS)
    assert recorder.delays[-1] > recorder.delays[0]
    # (c) The chain terminates where operators can see it.
    assert outcomes == [
        "retry_scheduled",
        "retry_scheduled",
        "retry_scheduled",
        "dead_lettered",
    ]
    assert recorder.dead_lettered == ["throttle_exhausted: no slot"]


def test_throttle_holds_at_the_schedule_ceiling(monkeypatch):
    """Past the last schedule entry the delay holds — it does not wrap to 1s."""
    recorder = _Recorder(monkeypatch, SimpleNamespace(max_throttle_attempts=6))

    for _ in range(5):
        recorder.fail_once(_Throttled("no slot"))

    assert recorder.delays == [1, 5, 30, 30, 30]


def test_a_real_failure_still_spends_the_failure_budget(monkeypatch):
    """The fault lane is unchanged: ``attempts`` advances, throttle resets."""
    recorder = _Recorder(monkeypatch, SimpleNamespace())

    recorder.fail_once(_Throttled("no slot"))
    recorder.fail_once(RuntimeError("boom"))

    _delay, options = recorder.scheduled[-1]
    assert options["attempts"] == 1
    assert options["throttle_attempts"] == 0
    assert recorder.fail_once(RuntimeError("boom")) == "retry_scheduled"
    assert recorder.scheduled[-1][1]["attempts"] == 2
    assert recorder.fail_once(RuntimeError("boom")) == "dead_lettered"
    assert recorder.dead_lettered == ["boom"]


def test_dedup_key_carries_both_counters(monkeypatch):
    """A frozen ``attempts`` made every throttle retry mint the same key.

    Uniqueness held only by accident, because the source ``job_id``
    happened to rotate between cycles.
    """
    recorder = _Recorder(monkeypatch, SimpleNamespace(max_throttle_attempts=4))

    recorder.fail_once(_Throttled("no slot"))
    recorder.fail_once(_Throttled("no slot"))

    keys = [options["deduplication_key"] for _d, options in recorder.scheduled]
    assert keys == ["retry:starved-1:0:1", "retry:starved-1:0:2"]
    assert len(set(keys)) == len(keys)


@pytest.mark.parametrize("throttle_attempts", [0, DEFAULT_MAX_THROTTLE_ATTEMPTS - 2])
def test_framework_default_throttle_budget_is_finite(throttle_attempts):
    """A job that declares no cap still terminates."""
    msg = {"attempts": 0, "throttle_attempts": throttle_attempts}

    assert JobProcessor._should_retry_job(msg, SimpleNamespace(), _Throttled()) is True

    exhausted = {"attempts": 0, "throttle_attempts": DEFAULT_MAX_THROTTLE_ATTEMPTS - 1}
    assert (
        JobProcessor._should_retry_job(exhausted, SimpleNamespace(), _Throttled())
        is False
    )


def test_a_missing_throttle_counter_reads_as_zero():
    """Envelopes signed before the field existed must deserialize cleanly."""
    legacy = {"attempts": 0, "queue": "sync"}

    assert JobProcessor._envelope_counter(legacy, "throttle_attempts") == 0
    assert JobProcessor._should_retry_job(legacy, SimpleNamespace(), _Throttled()) is True
