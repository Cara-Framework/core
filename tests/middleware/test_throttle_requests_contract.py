"""What a throttled route answers: the IETF headers when allowed, a typed 429 when not."""

from __future__ import annotations

import asyncio
import sys
from types import SimpleNamespace

import pytest

from cara.exceptions import TooManyRequestsException
from cara.openapi import RATE_LIMIT_HEADERS
from cara.rates import Limit, RateLimitDecision

from ._fixtures import throttle_middleware

_throttle_module = sys.modules["cara.middleware.http.ThrottleRequests"]

_ALLOWED = RateLimitDecision(
    allowed=True,
    limit=120,
    period_seconds=60,
    remaining=87,
    retry_after=0,
    reset_after=12,
)
_REFUSED = RateLimitDecision(
    allowed=False,
    limit=120,
    period_seconds=60,
    remaining=0,
    retry_after=3,
    reset_after=40,
)


class _Response:
    def __init__(self) -> None:
        self.headers: dict[str, str] = {}

    def header(self, name: str, value: str) -> None:
        self.headers[name] = value


class _Limiter:
    def __init__(self, limit: Limit, decision: RateLimitDecision) -> None:
        self._limit = limit
        self._decision = decision
        self.spent: list[Limit] = []

    def resolve_limiter(self, name: str, request) -> Limit:
        return self._limit

    def attempt(self, limit: Limit, *, cost: int = 1) -> RateLimitDecision:
        self.spent.append(limit)
        return self._decision


class _Counter:
    def __init__(self, sink: list) -> None:
        self._sink = sink

    def labels(self, **labels):
        self._sink.append(labels)
        return self

    def inc(self) -> None:
        pass


def _install(monkeypatch, limiter, *, trusted=(), metrics: list | None = None) -> None:
    monkeypatch.setattr(_throttle_module.facades, "RateLimiter", limiter)
    monkeypatch.setattr(
        _throttle_module.facades,
        "Config",
        SimpleNamespace(
            get=lambda key, default=None: (
                list(trusted) if key == "rate.trusted_ips" else default
            )
        ),
    )
    sink = metrics if metrics is not None else []
    monkeypatch.setattr(_throttle_module, "counter", lambda *_a, **_k: _Counter(sink))


def _handle(middleware, *, ip: str = "203.0.113.9"):
    response = _Response()
    reached: list[bool] = []

    async def next_fn(_request):
        reached.append(True)
        return response

    request = SimpleNamespace(ip=lambda: ip)
    result = asyncio.run(middleware.handle(request, next_fn))
    return result, reached


def test_an_allowed_request_carries_the_policy_and_what_is_left_of_it(
    monkeypatch,
) -> None:
    limiter = _Limiter(Limit.per_minute(120).by("user:7"), _ALLOWED)
    _install(monkeypatch, limiter)

    response, reached = _handle(throttle_middleware(limiter="api"))

    assert reached == [True]
    assert limiter.spent == [limiter._limit]
    assert response.headers == {
        "RateLimit-Policy": '"api";q=120;w=60',
        "RateLimit": '"api";r=87;t=12',
    }


def test_a_refused_request_raises_the_typed_429_without_reaching_the_route(
    monkeypatch,
) -> None:
    _install(monkeypatch, _Limiter(Limit.per_minute(120).by("user:7"), _REFUSED))

    with pytest.raises(TooManyRequestsException) as excinfo:
        _handle(throttle_middleware(limiter="api"))

    refusal = excinfo.value
    assert refusal.status_code == 429
    assert refusal.retry_after == 3
    assert refusal.to_dict() == {
        "error": "Too Many Requests",
        "type": "rate_limit_exceeded",
        "retry_after": 3,
    }
    assert refusal.response_headers == {
        "RateLimit-Policy": '"api";q=120;w=60',
        "RateLimit": '"api";r=0;t=40',
    }


def test_the_published_header_contract_names_exactly_what_is_sent(monkeypatch) -> None:
    """``RATE_LIMIT_HEADERS`` is what OpenAPI documents; hold the middleware to it."""
    _install(monkeypatch, _Limiter(Limit.per_minute(120).by("user:7"), _ALLOWED))
    response, _ = _handle(throttle_middleware(limiter="api"))

    assert tuple(response.headers) == RATE_LIMIT_HEADERS.passed

    _install(monkeypatch, _Limiter(Limit.per_minute(120).by("user:7"), _REFUSED))
    with pytest.raises(TooManyRequestsException) as excinfo:
        _handle(throttle_middleware(limiter="api"))

    # ``Retry-After`` rides the refusal too: the exception handler lifts it from
    # ``retry_after`` on the one error path.
    sent = {"Retry-After", *excinfo.value.response_headers}
    assert sent == set(RATE_LIMIT_HEADERS.refused[429])


def test_an_unlimited_policy_spends_nothing_and_says_nothing(monkeypatch) -> None:
    limiter = _Limiter(Limit.none(), _REFUSED)
    _install(monkeypatch, limiter)

    response, reached = _handle(throttle_middleware(limiter="internal"))

    assert reached == [True]
    assert limiter.spent == []
    assert response.headers == {}


def test_a_trusted_address_bypasses_before_any_limiter_is_consulted(
    monkeypatch,
) -> None:
    class _Unreachable:
        def resolve_limiter(self, name, request):
            raise AssertionError("a trusted probe must not resolve a limiter")

    _install(monkeypatch, _Unreachable(), trusted=["127.0.0.1"])

    response, reached = _handle(throttle_middleware(limiter="health"), ip="127.0.0.1")

    assert reached == [True]
    assert response.headers == {}


def test_every_decision_lands_on_the_counter_alerting_reads(monkeypatch) -> None:
    metrics: list = []
    _install(
        monkeypatch,
        _Limiter(Limit.per_minute(120).by("user:7"), _ALLOWED),
        metrics=metrics,
    )
    _handle(throttle_middleware(limiter="api"))

    _install(
        monkeypatch,
        _Limiter(Limit.per_minute(120).by("user:7"), _REFUSED),
        metrics=metrics,
    )
    with pytest.raises(TooManyRequestsException):
        _handle(throttle_middleware(limiter="api"))

    assert metrics == [
        {"limiter": "api", "outcome": "allowed"},
        {"limiter": "api", "outcome": "denied"},
    ]


def test_a_metrics_fault_never_fails_the_request(monkeypatch) -> None:
    _install(monkeypatch, _Limiter(Limit.per_minute(120).by("user:7"), _ALLOWED))

    def broken(*_args, **_kwargs):
        raise RuntimeError("registry unavailable")

    monkeypatch.setattr(_throttle_module, "counter", broken)

    response, reached = _handle(throttle_middleware(limiter="api"))

    assert reached == [True]
    assert response.headers["RateLimit"] == '"api";r=87;t=12'
