"""An unregistered or unnamed throttle must refuse, not invent a limit.

``throttle:login`` used to fall through to a global 60/minute default when the
``login`` registration was mistyped, moved to a provider that is not booted,
or simply forgotten. Nothing surfaced the 12x widening: the router table, the
middleware list and the rate-limit headers all still read "throttled", so the
first evidence would have been a successful credential-stuffing run.

§9: an unconfigured gate denies, and an unknown SLA means NO deadline rather
than an invented one. The same holds for a throttle with no name, and for a
number where a name belongs — ``throttle:600`` once built an ad-hoc budget at
the call site, outside the one file every ceiling lives in.
"""

from __future__ import annotations

import sys

import pytest

from cara.exceptions import RateLimitConfigurationException
from cara.rates import Limit

from ._fixtures import throttle_middleware

_throttle_module = sys.modules["cara.middleware.http.ThrottleRequests"]


class _RateLimiterStub:
    """Stands in for the ``RateLimiter`` facade."""

    def __init__(self, limiters: dict | None = None) -> None:
        self._limiters = limiters or {}

    def resolve_limiter(self, name: str, request):
        callback = self._limiters.get(name)
        return callback(request) if callback else None


class TestAThrottleWithoutARegisteredNameRefuses:
    def test_refuses_instead_of_falling_back_to_a_global_limit(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(_throttle_module.facades, "RateLimiter", _RateLimiterStub())

        with pytest.raises(RateLimitConfigurationException, match="throttle:login"):
            throttle_middleware(limiter="login")._resolve_limit(request=object())

    def test_the_refusal_names_where_to_register_it(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(_throttle_module.facades, "RateLimiter", _RateLimiterStub())

        with pytest.raises(RateLimitConfigurationException) as excinfo:
            throttle_middleware(limiter="typoed")._resolve_limit(request=object())

        assert "config/rate.py" in str(excinfo.value)

    @pytest.mark.parametrize("limiter", [None, "", 600], ids=["none", "empty", "number"])
    def test_a_nameless_or_numeric_throttle_refuses_too(
        self, monkeypatch: pytest.MonkeyPatch, limiter
    ) -> None:
        monkeypatch.setattr(_throttle_module.facades, "RateLimiter", _RateLimiterStub())

        with pytest.raises(RateLimitConfigurationException) as excinfo:
            throttle_middleware(limiter=limiter)._resolve_limit(request=object())

        assert "config/rate.py" in str(excinfo.value)


def test_a_registered_named_limiter_resolves(monkeypatch: pytest.MonkeyPatch) -> None:
    expected = Limit.per_minute(5).by("ip:198.51.100.1")
    monkeypatch.setattr(
        _throttle_module.facades,
        "RateLimiter",
        _RateLimiterStub({"login": lambda _r: expected}),
    )

    resolved = throttle_middleware(limiter="login")._resolve_limit(request=object())

    assert resolved is expected
