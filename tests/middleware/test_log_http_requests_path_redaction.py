"""The access-log LINE never carries a capability from the URL path.

``LogHttpRequests`` writes to stdout and the daily log files; before
``PathRedaction`` it redacted the query string but wrote ``request.path`` raw,
so ``/api/webhooks/shipstation/<secret>`` sat in the logs for their whole
retention. Both of its lines — success and exception — and the
``RecordRequestMetrics`` slow-request warning are pinned here.
"""

from __future__ import annotations

import importlib
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from cara.middleware.http import (
    LogHttpRequests,
    PathRedaction,
    RecordRequestMetrics,
)
from cara.routing import Route, Router

log_module = importlib.import_module("cara.middleware.http.LogHttpRequests")
metrics_module = importlib.import_module("cara.middleware.http.RecordRequestMetrics")
redaction_module = importlib.import_module("cara.middleware.http.PathRedaction")

SECRET = "ss_9Xq2-secret"
WEBHOOK = f"/api/webhooks/shipstation/{SECRET}"
REDACTED_WEBHOOK = f"/api/webhooks/shipstation/{PathRedaction.REDACTED_SEGMENT}"


def _controller(_request=None, _response=None):
    return {"ok": True}


def _application() -> SimpleNamespace:
    router = Router(
        None,
        Route.post("/api/webhooks/shipstation/@token", _controller),
        Route.get("/api/products/@id", _controller),
    )
    return SimpleNamespace(make=lambda _key: router)


def _request(path: str, query: bytes = b"") -> SimpleNamespace:
    return SimpleNamespace(
        path=path,
        method="POST",
        ip=lambda: "203.0.113.9",
        scope={"query_string": query, "headers": []},
    )


def _response(status: int = 200) -> SimpleNamespace:
    return SimpleNamespace(get_status_code=lambda: status)


@pytest.fixture
def log(monkeypatch) -> SimpleNamespace:
    """Capture every log call; framework-default secret parameters."""
    captured = SimpleNamespace(info=Mock(), warning=Mock(), debug=Mock())
    monkeypatch.setattr(log_module, "Log", captured)
    monkeypatch.setattr(metrics_module, "Log", captured)
    monkeypatch.setattr(redaction_module, "config", lambda _key, default=None: default)
    return captured


def _line(call) -> str:
    """One logged line, its %-args folded in."""
    message, *args = call.args
    return message % tuple(args) if args else message


@pytest.mark.asyncio
async def test_the_success_line_redacts_the_path_capability(log):
    middleware = LogHttpRequests(_application())

    async def _ok(_request):
        return _response(200)

    await middleware.handle(_request(WEBHOOK, b"token=abc&page=2"), _ok)

    line = _line(log.info.call_args)
    assert SECRET not in line
    assert REDACTED_WEBHOOK in line
    # The query-string redaction still rides alongside.
    assert "?token=[REDACTED]&page=2" in line


@pytest.mark.asyncio
async def test_the_exception_line_redacts_the_path_capability(log):
    middleware = LogHttpRequests(_application())

    async def _boom(_request):
        raise RuntimeError("handler failed")

    with pytest.raises(RuntimeError):
        await middleware.handle(_request(WEBHOOK), _boom)

    line = _line(log.warning.call_args)
    assert SECRET not in line
    assert REDACTED_WEBHOOK in line


@pytest.mark.asyncio
async def test_an_ordinary_path_is_logged_verbatim(log):
    middleware = LogHttpRequests(_application())

    async def _ok(_request):
        return _response(200)

    await middleware.handle(_request("/api/products/PRD01HZY3V8Q"), _ok)

    assert "/api/products/PRD01HZY3V8Q" in _line(log.info.call_args)


@pytest.mark.asyncio
async def test_the_slow_request_warning_redacts_the_path_capability(log):
    middleware = RecordRequestMetrics(_application())
    middleware.SLOW_REQUEST_THRESHOLD_MS = -1

    async def _ok(_request):
        return _response(200)

    await middleware.handle(_request(WEBHOOK), _ok)

    line = _line(log.warning.call_args)
    assert SECRET not in line
    assert REDACTED_WEBHOOK in line
