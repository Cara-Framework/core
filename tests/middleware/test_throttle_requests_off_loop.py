"""``ThrottleRequests`` spends its bucket on a worker thread, not the event loop.

The spend is a synchronous Redis EVAL. It ran inline in ``handle``, so one
slow Redis round trip stalled every request on the worker's event loop. It
now goes through ``ExecutionContext.run_in_thread`` (context vars copied in);
the decision, the headers and an outage's exception are unchanged.
"""

from __future__ import annotations

import asyncio
import contextvars
import sys
import threading
from types import SimpleNamespace

import pytest

from cara.exceptions import TooManyRequestsException
from cara.rates import Limit, RateLimitDecision

from ._fixtures import throttle_middleware

_throttle_module = sys.modules["cara.middleware.http.ThrottleRequests"]

_marker: contextvars.ContextVar[str] = contextvars.ContextVar("_marker", default="")


def _decision(allowed: bool) -> RateLimitDecision:
    return RateLimitDecision(
        allowed=allowed,
        limit=120,
        period_seconds=60,
        remaining=87 if allowed else 0,
        retry_after=0 if allowed else 3,
        reset_after=12,
    )


class _Limiter:
    """Records which thread spent the bucket, and what it saw of the context."""

    def __init__(self, decision=None, error: Exception | None = None) -> None:
        self._limit = Limit.per_minute(120).by("user:7")
        self._decision = decision
        self._error = error
        self.spent_on: list[int] = []
        self.saw_marker: list[str] = []

    def resolve_limiter(self, _name, _request) -> Limit:
        return self._limit

    def attempt(self, limit: Limit, *, cost: int = 1) -> RateLimitDecision:
        self.spent_on.append(threading.get_ident())
        self.saw_marker.append(_marker.get())
        if self._error is not None:
            raise self._error
        return self._decision


class _Response:
    def __init__(self) -> None:
        self.headers: dict[str, str] = {}

    def header(self, name: str, value: str) -> None:
        self.headers[name] = value


def _install(monkeypatch, limiter: _Limiter) -> None:
    monkeypatch.setattr(_throttle_module.facades, "RateLimiter", limiter)
    monkeypatch.setattr(
        _throttle_module.facades,
        "Config",
        SimpleNamespace(get=lambda _key, default=None: default),
    )
    counter = SimpleNamespace(labels=lambda **_k: counter, inc=lambda: None)
    monkeypatch.setattr(_throttle_module, "counter", lambda *_a, **_k: counter)


async def _handle(limiter_name: str = "api"):
    _marker.set("request-7")
    response = _Response()

    async def next_fn(_request):
        return response

    request = SimpleNamespace(ip=lambda: "203.0.113.9")
    loop_thread = threading.get_ident()
    result = await throttle_middleware(limiter=limiter_name).handle(request, next_fn)
    return result, loop_thread


def test_the_spend_runs_off_the_event_loop_with_the_request_context(monkeypatch):
    limiter = _Limiter(_decision(allowed=True))
    _install(monkeypatch, limiter)

    response, loop_thread = asyncio.run(_handle())

    assert len(limiter.spent_on) == 1
    assert limiter.spent_on[0] != loop_thread
    assert limiter.saw_marker == ["request-7"]
    assert response.headers == {
        "RateLimit-Policy": '"api";q=120;w=60',
        "RateLimit": '"api";r=87;t=12',
    }


def test_a_refusal_decided_on_the_thread_is_the_same_429(monkeypatch):
    limiter = _Limiter(_decision(allowed=False))
    _install(monkeypatch, limiter)

    with pytest.raises(TooManyRequestsException) as excinfo:
        asyncio.run(_handle())

    assert excinfo.value.retry_after == 3
    assert excinfo.value.response_headers == {
        "RateLimit-Policy": '"api";q=120;w=60',
        "RateLimit": '"api";r=0;t=12',
    }


def test_an_outage_on_the_thread_raises_what_it_raised_inline(monkeypatch):
    outage = ConnectionError("redis unreachable")
    _install(monkeypatch, _Limiter(error=outage))

    with pytest.raises(ConnectionError) as excinfo:
        asyncio.run(_handle())

    assert excinfo.value is outage
