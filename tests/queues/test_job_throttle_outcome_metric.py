"""A throttle bounce is consumed as ``throttled``, never as ``failed``.

``ConcurrencyExceeded`` (a fair-share or provider-slot refusal), a
marketplace cooldown and an idempotency overlap all declare ``is_throttle``:
the worker re-delivers them without spending an attempt, logs INFO and sends
no Sentry event. The consumed-outcome counter still said ``failed``, so a
healthy fleet under ordinary contention crossed ``JobFailureRateHigh``
(failed / consumed > 20 %). A throttle that exhausted its starvation budget
dead-letters and stays a failure.
"""

from __future__ import annotations

import importlib

import pytest

from cara.commands.core.JobProcessor import JobProcessor
from cara.configuration import Configuration
from cara.queues.LeaseHeartbeat import LeaseHeartbeat
from cara.queues.middleware.ConcurrencyExceeded import ConcurrencyExceeded
from tests.queues.test_delivery_execution_heartbeat import (
    _DeliveryStore,
    _run_worker,
    _Tracker,
)


@pytest.fixture(autouse=True)
def _isolated_configuration(monkeypatch):
    monkeypatch.setattr(Configuration, "_instance", None)
    Configuration.empty()
    yield
    for heartbeat in LeaseHeartbeat.live():
        heartbeat.stop()


class _Series:
    def __init__(self, sink: list, name: str, labels: dict):
        self._sink, self._name, self._labels = sink, name, labels

    def inc(self, *_args):
        self._sink.append((self._name, self._labels))

    def dec(self, *_args):
        return None

    def observe(self, *_args):
        return None

    def set(self, *_args):
        return None


class _Metric:
    def __init__(self, sink: list, name: str):
        self._sink, self._name = sink, name

    def labels(self, **labels):
        return _Series(self._sink, self._name, labels)


class _Metrics:
    def __init__(self):
        self.recorded: list[tuple[str, dict]] = []

    def __getattr__(self, name):
        return _Metric(self.recorded, name)

    def outcomes(self) -> list[str]:
        return [
            labels["outcome"]
            for name, labels in self.recorded
            if name == "queue_jobs_consumed_total"
        ]

    def exceptions(self) -> list[str]:
        return [
            labels["exception"]
            for name, labels in self.recorded
            if name == "exceptions_total"
        ]


class _Refused:
    queue = "sync"

    def __init__(self, exc: Exception):
        self._exc = exc

    async def handle(self):
        raise self._exc


def _consume(monkeypatch, exc: Exception, routed: str) -> _Metrics:
    execution = importlib.import_module("cara.commands.core._JobExecution")
    metrics = _Metrics()
    monkeypatch.setattr(execution, "MetricsBase", metrics)
    monkeypatch.setattr(
        JobProcessor, "_route_failed_message", staticmethod(lambda **_kw: routed)
    )
    outcome, _settled = _run_worker(
        monkeypatch, _Refused(exc), _DeliveryStore(), _Tracker()
    )
    assert outcome == "failure"
    return metrics


def test_a_redelivered_throttle_is_consumed_as_throttled(monkeypatch):
    metrics = _consume(monkeypatch, ConcurrencyExceeded("no slot"), "retry_scheduled")
    assert metrics.outcomes() == ["throttled"]
    assert metrics.exceptions() == []


def test_a_throttle_that_exhausted_its_budget_is_a_failure(monkeypatch):
    metrics = _consume(monkeypatch, ConcurrencyExceeded("no slot"), "dead_lettered")
    assert metrics.outcomes() == ["failed"]


def test_a_real_failure_is_still_failed_and_counted_as_an_exception(monkeypatch):
    metrics = _consume(monkeypatch, ValueError("boom"), "retry_scheduled")
    assert metrics.outcomes() == ["failed"]
    assert metrics.exceptions() == ["ValueError"]
