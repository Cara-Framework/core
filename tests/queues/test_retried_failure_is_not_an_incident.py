"""A failure the queue will retry is one warning line; only the last one pages.

Measured 2026-11-13 (scratch stack, 50,000 jobs, 25 % failing once with a
retryable error and 3 % poison): every failed ATTEMPT wrote an ERROR with a
full traceback (~6.6 KB, 84 lines) and sent one Sentry event, before the
router had even decided to retry it. 17,000 failed attempts wrote ~200 MB of
worker log in four minutes and would have sent 17,000 Sentry events for
12,500 jobs that succeeded on their retry. The decision is known one call
later: a retry scheduled is a WARNING line with no traceback and no capture;
a dead letter (attempts spent, ``do_not_retry``) is the ERROR with the
traceback and the one Sentry event.
"""

from __future__ import annotations

import sys

import pytest

execution = sys.modules.get("cara.commands.core._JobExecution")
if execution is None:  # pragma: no cover - import order
    import cara.commands.core._JobExecution as execution  # noqa: F811


class _Log:
    def __init__(self) -> None:
        self.warnings: list[tuple] = []
        self.errors: list[tuple] = []

    def warning(self, message, *args, **kwargs):
        self.warnings.append((message, args, kwargs))

    def error(self, message, *args, **kwargs):
        self.errors.append((message, args, kwargs))


@pytest.fixture
def seen(monkeypatch):
    log = _Log()
    captured: list[BaseException] = []
    monkeypatch.setattr(execution, "Log", log)
    monkeypatch.setattr(execution.observability, "capture_exception", captured.append)
    return log, captured


def _fail_inside_except(routed: str, exc: BaseException) -> None:
    try:
        raise exc
    except type(exc) as caught:
        execution._report_job_failure(caught, routed)


def test_a_failure_the_queue_retries_is_one_warning_and_no_capture(seen):
    log, captured = seen

    _fail_inside_except("retry_scheduled", ConnectionError("dropped"))

    assert log.errors == []
    assert captured == []
    assert len(log.warnings) == 1
    assert "exc_info" not in log.warnings[0][2]


def test_a_dead_letter_is_the_error_with_the_traceback_and_one_capture(seen):
    log, captured = seen
    poison = RuntimeError("poison")

    _fail_inside_except("dead_lettered", poison)

    assert len(log.errors) == 1
    assert log.errors[0][2].get("exc_info") is True
    assert captured == [poison]


def test_a_timeout_follows_the_same_rule(seen):
    log, captured = seen

    _fail_inside_except("retry_scheduled", TimeoutError("slow"))
    assert (log.errors, captured) == ([], [])

    late = TimeoutError("slow again")
    _fail_inside_except("dead_lettered", late)
    assert captured == [late]
