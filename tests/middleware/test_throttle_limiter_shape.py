"""A limiter callback returns ONE keyed ``Limit``, and a Limit is a real policy.

``RateLimiter.for_`` once documented "a Limit object **or list of Limit
objects**". Nothing implemented the list: the middleware dereferenced the
budget on whatever came back, so an operator who took the documented option
got an ``AttributeError`` 500 on every request to every route carrying that
``throttle:<name>``, with nothing naming the misconfigured limiter. The promise
was deleted rather than implemented — a route that needs two budgets names two
limiters, each its own middleware.

Deleting a docstring line cannot be tested, so the enforcement is tested
instead: ``resolve_limiter`` refuses every shape the throttle cannot spend —
including a Limit with no key, which has no bucket — fails closed, and names
the limiter. These tests drive the REAL ``RateLimiter.resolve_limiter``; the
sibling ``test_throttle_unregistered_limiter`` module stubs that method out.
"""

from __future__ import annotations

import sys

import pytest

from cara.exceptions import RateLimitConfigurationException
from cara.rates import Limit
from cara.rates.RateLimiter import RateLimiter

from ._fixtures import throttle_middleware

_throttle_module = sys.modules["cara.middleware.http.ThrottleRequests"]


def _real_rate_limiter(**limiters) -> RateLimiter:
    """A genuine ``RateLimiter`` with genuine ``for_`` registrations."""
    limiter = RateLimiter(application=None)
    for name, callback in limiters.items():
        limiter.for_(name, callback)
    return limiter


@pytest.mark.parametrize(
    ("factory", "exception_type"),
    [
        (lambda: Limit.per_minute(0), ValueError),
        (lambda: Limit(-1, 60), ValueError),
        (lambda: Limit(True, 60), TypeError),
        (lambda: Limit(5, 60, burst=0), ValueError),
        (lambda: Limit(5, 60, burst=2.5), TypeError),
        (lambda: Limit(0, 0, burst=3), ValueError),
        (lambda: Limit.none().by(""), ValueError),
    ],
    ids=[
        "zero-limit-with-a-period",
        "negative",
        "bool",
        "zero-burst",
        "float-burst",
        "unlimited-with-burst",
        "empty-key",
    ],
)
def test_limit_builder_rejects_ambiguous_or_invalid_shapes(
    factory, exception_type
) -> None:
    with pytest.raises(exception_type):
        factory()


def test_burst_defaults_to_the_limit_and_the_pace_rounds_up() -> None:
    seven_a_minute = Limit.per_minute(7)

    assert seven_a_minute.burst == 7
    # 60000 / 7 = 8571.43 ms; rounding down would mint budget.
    assert seven_a_minute.emission_interval_ms == 8572
    assert Limit.per_second(4, burst=10).emission_interval_ms == 250


class TestOnlyOneKeyedLimitResolves:
    def test_resolving_a_list_returning_limiter_refuses(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            _throttle_module.facades,
            "RateLimiter",
            _real_rate_limiter(
                login=lambda _r: [
                    Limit.per_minute(5).by("ip:198.51.100.1"),
                    Limit.per_hour(100).by("ip:198.51.100.1"),
                ]
            ),
        )

        with pytest.raises(RateLimitConfigurationException) as excinfo:
            throttle_middleware(limiter="login")._resolve_limit(request=object())

        assert "throttle:login" in str(excinfo.value)
        assert "list" in str(excinfo.value)

    def test_the_refusal_names_where_the_limiter_is_configured(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            _throttle_module.facades,
            "RateLimiter",
            _real_rate_limiter(api=lambda _r: 60),
        )

        with pytest.raises(RateLimitConfigurationException) as excinfo:
            throttle_middleware(limiter="api")._resolve_limit(request=object())

        assert "config/rate.py" in str(excinfo.value)

    def test_a_none_returning_callback_is_refused_too(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A callback whose branch fell through returns ``None`` — a different
        fault from an unregistered name, and a silent bypass if it were ever
        read as "no limit"."""
        monkeypatch.setattr(
            _throttle_module.facades,
            "RateLimiter",
            _real_rate_limiter(admin=lambda _r: None),
        )

        with pytest.raises(RateLimitConfigurationException) as excinfo:
            throttle_middleware(limiter="admin")._resolve_limit(request=object())

        assert "NoneType" in str(excinfo.value)

    def test_a_limit_without_a_key_has_no_bucket_to_spend(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            _throttle_module.facades,
            "RateLimiter",
            _real_rate_limiter(api=lambda _r: Limit.per_minute(60)),
        )

        with pytest.raises(RateLimitConfigurationException) as excinfo:
            throttle_middleware(limiter="api")._resolve_limit(request=object())

        assert ".by(" in str(excinfo.value)

    @pytest.mark.parametrize(
        "returned",
        [
            [Limit.per_minute(5).by("ip:198.51.100.1")],
            (Limit.per_minute(5).by("ip:198.51.100.1"),),
            {"api": Limit.per_minute(5).by("ip:198.51.100.1")},
            60,
            "60/min",
            None,
        ],
        ids=["list", "tuple", "dict", "int", "str", "none"],
    )
    def test_every_shape_but_a_single_limit_is_refused(
        self, monkeypatch: pytest.MonkeyPatch, returned
    ) -> None:
        monkeypatch.setattr(
            _throttle_module.facades,
            "RateLimiter",
            _real_rate_limiter(api=lambda _r: returned),
        )

        with pytest.raises(RateLimitConfigurationException):
            throttle_middleware(limiter="api")._resolve_limit(request=object())


class TestTheSupportedShapeIsUntouched:
    def test_a_single_keyed_limit_resolves_through_the_real_limiter(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        expected = Limit.per_minute(5).by("ip:198.51.100.1")
        monkeypatch.setattr(
            _throttle_module.facades,
            "RateLimiter",
            _real_rate_limiter(login=lambda _r: expected),
        )

        resolved = throttle_middleware(limiter="login")._resolve_limit(request=object())

        assert resolved is expected

    def test_an_unlimited_limit_needs_no_key(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            _throttle_module.facades,
            "RateLimiter",
            _real_rate_limiter(internal=lambda _r: Limit.none()),
        )

        resolved = throttle_middleware(limiter="internal")._resolve_limit(
            request=object()
        )

        assert resolved.unlimited is True

    def test_an_unregistered_name_still_reports_itself_as_unregistered(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The shape refusal must not swallow the unregistered-limiter refusal —
        different fault, different message."""
        monkeypatch.setattr(_throttle_module.facades, "RateLimiter", _real_rate_limiter())

        with pytest.raises(RateLimitConfigurationException) as excinfo:
            throttle_middleware(limiter="never_registered")._resolve_limit(
                request=object()
            )

        assert "unregistered rate" in str(excinfo.value)
