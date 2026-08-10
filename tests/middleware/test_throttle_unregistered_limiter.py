"""An unregistered named throttle must refuse, not widen the limit.

``throttle:login`` used to fall through to the global
``Limit(RateLimiter.limit, RateLimiter.window / 60)`` — 60/minute — when the
``RateLimiter.for_("login", Limit.per_minute(5))`` registration was mistyped,
moved to a provider that is not booted, or simply forgotten. Nothing
surfaced the 12x widening: the router table, the middleware list and the
``X-RateLimit-Limit`` header all still read "throttled", so the first
evidence would have been a successful credential-stuffing run.

§9: an unconfigured gate denies, and an unknown SLA means NO deadline rather
than an invented one. The refusal is an exception rather than a zero-budget
``Limit`` because ``_attempt_limit`` reads ``max_attempts == 0`` as
UNLIMITED — that sentinel would have been maximally fail-OPEN.
"""

from __future__ import annotations

import sys

import pytest

from cara.exceptions import RateLimitConfigurationException
from cara.middleware.http.ThrottleRequests import ThrottleRequests


class _RateLimiterStub:
    """Stands in for the ``RateLimiter`` facade.

    ``limit``/``window`` carry the framework defaults on purpose: if the
    deleted global fallback ever returns, the middleware silently answers
    ``Limit(60, 1)`` here instead of raising.
    """

    limit = 60
    window = 60

    def __init__(self, limiters: dict | None = None) -> None:
        self._limiters = limiters or {}

    def resolve_limiter(self, name: str, request):
        callback = self._limiters.get(name)
        return callback(request) if callback else None


def _middleware(limit=None, window=None) -> ThrottleRequests:
    """Build the middleware without the provider boot the base
    ``Middleware.__init__`` triggers."""
    middleware = ThrottleRequests.__new__(ThrottleRequests)
    middleware.custom_limit = limit
    middleware.custom_window_minutes = window
    return middleware


class TestUnregisteredNamedLimiter:
    def test_refuses_instead_of_falling_back_to_the_global_limit(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        module = sys.modules["cara.middleware.http.ThrottleRequests"]

        monkeypatch.setattr(module, "RateLimiter", _RateLimiterStub())

        with pytest.raises(RateLimitConfigurationException, match="throttle:login"):
            _middleware(limit="login")._resolve_limit_config(request=object())

    def test_the_refusal_names_where_to_register_it(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        module = sys.modules["cara.middleware.http.ThrottleRequests"]

        monkeypatch.setattr(module, "RateLimiter", _RateLimiterStub())

        with pytest.raises(RateLimitConfigurationException) as excinfo:
            _middleware(limit="typoed")._resolve_limit_config(request=object())

        assert "config/rate.py" in str(excinfo.value)

    def test_a_shapeless_configuration_refuses_too(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``throttle:,5`` — a window with no limit — reached the same global
        fallback. There is no configuration that silently means 60/min."""
        module = sys.modules["cara.middleware.http.ThrottleRequests"]

        monkeypatch.setattr(module, "RateLimiter", _RateLimiterStub())

        with pytest.raises(RateLimitConfigurationException):
            _middleware(limit=None, window=5)._resolve_limit_config(request=object())


class TestRegisteredConfigurationsAreUnchanged:
    def test_a_registered_named_limiter_resolves(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from cara.rates import Limit

        module = sys.modules["cara.middleware.http.ThrottleRequests"]

        expected = Limit(max_attempts=5, decay_minutes=1)
        monkeypatch.setattr(
            module, "RateLimiter", _RateLimiterStub({"login": lambda _r: expected})
        )

        resolved = _middleware(limit="login")._resolve_limit_config(request=object())

        assert resolved is expected

    def test_the_numeric_form_still_builds_its_own_limit(self) -> None:
        resolved = _middleware(limit=600, window=1)._resolve_limit_config(
            request=object()
        )

        assert resolved.max_attempts == 600
        assert resolved.decay_minutes == 1
