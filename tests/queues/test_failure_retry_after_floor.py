"""A failure that names its wait is retried no sooner than that wait.

Only throttle-class exceptions had their ``retry_after`` read; a job that
failed because an upstream answered ``Retry-After: 600`` (a webhook receiver,
a rate-limited API) came back on the generic 1s/5s/30s schedule and spent its
whole failure budget inside the window it was told to wait out.
"""

from __future__ import annotations

import builtins
from types import SimpleNamespace

import pytest

from cara.commands.core.JobProcessor import JobProcessor


class _App:
    def __init__(self, queue_service):
        self.queue_service = queue_service
        self.logger = SimpleNamespace(
            debug=lambda *a, **k: None,
            error=lambda *a, **k: None,
            info=lambda *a, **k: None,
            warning=lambda *a, **k: None,
        )

    def make(self, key):
        return {"queue": self.queue_service, "logger": self.logger}[key]


class _Failure(RuntimeError):
    def __init__(self, retry_after):
        super().__init__("upstream said wait")
        self.retry_after = retry_after


def _requeue(monkeypatch, exc) -> int:
    delays: list[int] = []

    class _Driver:
        @staticmethod
        def _apply_retry_jitter(delay, instance):
            return delay

    queue_service = SimpleNamespace(
        driver=lambda: _Driver(),
        later=lambda delay, instance, **options: delays.append(delay),
    )
    monkeypatch.setattr(builtins, "app", lambda: _App(queue_service), raising=False)
    JobProcessor._requeue_with_delay(
        SimpleNamespace(basic_ack=lambda *, delivery_tag: None),
        SimpleNamespace(delivery_tag=1),
        {
            "attempts": 0,
            "queue": "q",
            "job_id": "j",
            "db_job_id": 1,
            "_tenant": 5,
            "_tenant_mode": "tenant",
            "_otel": {},
        },
        SimpleNamespace(retry_backoff=[5]),
        exc,
        "q",
        "lease",
        SimpleNamespace(ensure_retry_progress_strict=lambda job_id: None),
        1,
    )
    return delays[0]


def test_failure_retry_after_is_a_floor(monkeypatch) -> None:
    assert _requeue(monkeypatch, _Failure(600)) == 600


def test_shorter_retry_after_never_cuts_the_schedule(monkeypatch) -> None:
    assert _requeue(monkeypatch, _Failure(2)) == 5


@pytest.mark.parametrize("value", [None, 0, -5, True, "600"])
def test_unreadable_retry_after_keeps_the_schedule(monkeypatch, value) -> None:
    assert _requeue(monkeypatch, _Failure(value)) == 5
