"""The debug header dump is built only when it would be written.

``LogHttpRequests.handle`` decoded, redacted and sanitized EVERY request
header and handed the result to ``Log.debug`` — after which the logger threw
it away, because the access-log category runs at INFO in every deployed
environment. ~30 us of a browser request spent on a line nobody keeps.

The dump is now built only when ``CategoryFilter`` (the logger's own rule)
says a DEBUG line in ``cara.http.requests`` is wanted.
"""

from __future__ import annotations

import asyncio
import importlib
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from cara.middleware.http import LogHttpRequests
from cara.routing import Router

log_module = importlib.import_module("cara.middleware.http.LogHttpRequests")
redaction_module = importlib.import_module("cara.middleware.http.PathRedaction")

_HEADERS = [
    (b"user-agent", b"Mozilla/5.0\nINFO forged-line"),
    (b"authorization", b"Bearer s3cr3t"),
]


def _run(monkeypatch, *, debug_enabled) -> tuple[SimpleNamespace, Mock]:
    log = SimpleNamespace(info=Mock(), warning=Mock(), debug=Mock())
    redact = Mock(side_effect=log_module._redact_headers)
    monkeypatch.setattr(log_module, "Log", log)
    monkeypatch.setattr(log_module, "_redact_headers", redact)
    monkeypatch.setattr(redaction_module, "config", lambda _key, default=None: default)

    def should_log(level, category):
        assert (level, category) == ("DEBUG", "cara.http.requests")
        if isinstance(debug_enabled, Exception):
            raise debug_enabled
        return debug_enabled

    monkeypatch.setattr(log_module.CategoryFilter, "should_log", staticmethod(should_log))

    router = Router(None)
    middleware = LogHttpRequests(SimpleNamespace(make=lambda _key: router))
    request = SimpleNamespace(
        path="/api/orders",
        method="GET",
        ip=lambda: "203.0.113.9",
        scope={"query_string": b"", "headers": _HEADERS},
    )
    response = SimpleNamespace(get_status_code=lambda: 200)

    async def next_fn(_request):
        return response

    assert asyncio.run(middleware.handle(request, next_fn)) is response
    return log, redact


def test_headers_are_not_processed_when_debug_is_off(monkeypatch):
    log, redact = _run(monkeypatch, debug_enabled=False)

    assert redact.call_count == 0  # was 1 on every request
    assert log.debug.call_count == 0
    assert log.info.call_count == 1  # the access line itself is untouched


def test_headers_are_dumped_redacted_and_sanitized_when_debug_is_on(monkeypatch):
    log, redact = _run(monkeypatch, debug_enabled=True)

    assert redact.call_count == 1
    (template, dumped), kwargs = log.debug.call_args
    assert template == "  Headers: %s"
    assert kwargs == {"category": "cara.http.requests"}
    assert dumped == {
        "user-agent": "Mozilla/5.0%0AINFO forged-line",
        "authorization": "[REDACTED]",
    }


def test_unreadable_log_level_falls_back_to_letting_the_logger_decide(monkeypatch):
    log, redact = _run(monkeypatch, debug_enabled=RuntimeError("no configuration"))

    assert redact.call_count == 1
    assert log.debug.call_count == 1


@pytest.mark.parametrize(
    ("raw", "sanitized"),
    [
        ("/plain/path", "/plain/path"),
        ("", ""),
        ("a\nb", "a%0Ab"),
        ("esc\x1b[31m", "esc%1B[31m"),
        ("del\x7f c1\x80\x9f", "del%7F c1%80%9F"),
        ("non-control \xa0 çay é 🙂", "non-control \xa0 çay é 🙂"),
        ("\x00\x1f", "%00%1F"),
    ],
)
def test_sanitizer_output_is_unchanged(raw, sanitized):
    assert log_module._sanitize_log_path(raw) == sanitized
