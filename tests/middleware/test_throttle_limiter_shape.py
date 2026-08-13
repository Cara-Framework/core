"""A limiter callback returns ONE ``Limit`` — the docs promised otherwise.

``RateLimiter.for_`` and ``RateLimiter.resolve_limiter`` both documented
"a Limit object **or list of Limit objects**". Nothing implements the list.
``ThrottleRequests._get_limit_config`` hands the callback's value straight
back and ``_attempt_limit`` immediately reads ``limit_config.max_attempts``,
so an operator who took the documented second option got
``AttributeError: 'list' object has no attribute 'max_attempts'`` — a 500 on
every request to every route carrying that ``throttle:<name>``, with nothing
in the error naming the misconfigured limiter.

§10: the promise is deleted rather than implemented. Composing several
windows would also need a key per limit, a most-restrictive rule for the
``X-RateLimit-*`` headers, and a rule for which limit's ``response``
callback wins — none of which exist, and inventing them behind a docstring
line is how the lie got there.

Deleting a docstring line cannot be tested, so the enforcement is tested
instead: ``resolve_limiter`` refuses a shape the throttle cannot read, fails
closed, and names the limiter. These tests drive the REAL
``RateLimiter.resolve_limiter`` — the sibling
``test_throttle_unregistered_limiter`` module stubs that method out, which
would have hidden this entirely.
"""

from __future__ import annotations

import sys

import pytest

from cara.exceptions import RateLimitConfigurationException
from cara.middleware.http.ThrottleRequests import ThrottleRequests
from cara.rates import Limit
from cara.rates.RateLimiter import RateLimiter

_throttle_module = sys.modules["cara.middleware.http.ThrottleRequests"]


def _real_rate_limiter(**limiters) -> RateLimiter:
    """A genuine ``RateLimiter`` with genuine ``for_`` registrations.

    Explicit fixed-driver options keep the test on the same validated
    contract production uses; nothing here reaches ``attempt``.
    """
    limiter = RateLimiter(
        application=None,
        options={"limit": 60, "window_seconds": 60, "cache_prefix": "rate_"},
    )
    for name, callback in limiters.items():
        limiter.for_(name, callback)
    return limiter


def _middleware(limit=None, window=None) -> ThrottleRequests:
    """Build the middleware without the provider boot the base
    ``Middleware.__init__`` triggers."""
    middleware = ThrottleRequests.__new__(ThrottleRequests)
    middleware.custom_limit = limit
    middleware.custom_window_minutes = window
    return middleware


@pytest.mark.parametrize(
    "options",
    [
        {},
        {"limit": 0, "window_seconds": 60, "cache_prefix": "rate_"},
        {"limit": 60, "window_seconds": 0, "cache_prefix": "rate_"},
        {"limit": 60, "window_seconds": 60, "cache_prefix": ""},
    ],
)
def test_fixed_driver_rejects_incomplete_or_nonpositive_options(options) -> None:
    with pytest.raises(RateLimitConfigurationException):
        RateLimiter(application=None, options=options)


@pytest.mark.parametrize(
    ("factory", "exception_type"),
    [
        (lambda: Limit.per_minute(0), ValueError),
        (lambda: Limit(-1, 1), ValueError),
        (lambda: Limit(True, 1), TypeError),
        (lambda: Limit.none().by(""), ValueError),
        (lambda: Limit.none().response(None), TypeError),
    ],
)
def test_limit_builder_rejects_ambiguous_or_invalid_shapes(
    factory, exception_type
) -> None:
    with pytest.raises(exception_type):
        factory()


class TestTheListFormWasNeverImplemented:
    def test_a_list_of_limits_cannot_be_enforced(self) -> None:
        """The consequence the docstring promised away, stated as code:
        ``_attempt_limit`` dereferences ``max_attempts`` on whatever the
        limiter returned."""
        limits = [Limit.per_minute(5), Limit.per_hour(100)]

        with pytest.raises(AttributeError, match="max_attempts"):
            _middleware()._attempt_limit("k", limits)

    def test_resolving_a_list_returning_limiter_refuses(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Fails closed at the resolve step, before the middleware can turn
        it into an unattributable AttributeError deeper in the stack."""
        monkeypatch.setattr(
            _throttle_module.facades,
            "RateLimiter",
            _real_rate_limiter(
                login=lambda _r: [Limit.per_minute(5), Limit.per_hour(100)]
            ),
        )

        with pytest.raises(RateLimitConfigurationException) as excinfo:
            _middleware(limit="login")._resolve_limit_config(request=object())

        assert "throttle:login" in str(excinfo.value)
        assert "list" in str(excinfo.value)

    def test_the_refusal_names_where_the_limiter_is_configured(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An operator reading a 500 log line has to be able to find the
        registration — the AttributeError named nothing."""
        monkeypatch.setattr(
            _throttle_module.facades,
            "RateLimiter",
            _real_rate_limiter(api=lambda _r: 60),
        )

        with pytest.raises(RateLimitConfigurationException) as excinfo:
            _middleware(limit="api")._resolve_limit_config(request=object())

        assert "config/rate.py" in str(excinfo.value)

    def test_a_none_returning_callback_is_refused_too(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A callback whose branch fell through returns ``None``, which the
        pre-fix path reported as "unregistered limiter" — the wrong
        diagnosis, and it would be a silent bypass if that refusal were ever
        relaxed."""
        monkeypatch.setattr(
            _throttle_module.facades,
            "RateLimiter",
            _real_rate_limiter(admin=lambda _r: None),
        )

        with pytest.raises(RateLimitConfigurationException):
            _middleware(limit="admin")._resolve_limit_config(request=object())


class TestTheSupportedShapeIsUntouched:
    def test_a_single_limit_resolves_through_the_real_limiter(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        expected = Limit.per_minute(5).by("ip:127.0.0.1")
        monkeypatch.setattr(
            _throttle_module.facades,
            "RateLimiter",
            _real_rate_limiter(login=lambda _r: expected),
        )

        resolved = _middleware(limit="login")._resolve_limit_config(request=object())

        assert resolved is expected

    def test_an_unlimited_limit_is_a_valid_shape(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``Limit.none()`` is ``max_attempts=0``, which ``_attempt_limit``
        reads as UNLIMITED. The shape check must not mistake a falsy budget
        for a missing attribute."""
        monkeypatch.setattr(
            _throttle_module.facades,
            "RateLimiter",
            _real_rate_limiter(internal=lambda _r: Limit.none()),
        )

        resolved = _middleware(limit="internal")._resolve_limit_config(request=object())

        assert resolved.max_attempts == 0

    def test_an_unregistered_name_still_reports_itself_as_unregistered(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The new shape refusal must not swallow the pre-existing
        unregistered-limiter refusal — different fault, different message."""
        monkeypatch.setattr(_throttle_module.facades, "RateLimiter", _real_rate_limiter())

        with pytest.raises(RateLimitConfigurationException) as excinfo:
            _middleware(limit="never_registered")._resolve_limit_config(request=object())

        assert "unregistered rate" in str(excinfo.value)


class TestTheDocumentedContractIsTheEnforcedOne:
    """§10 the only way it can actually be held: not by grepping the
    docstring for a deleted sentence — the docstring quotes the old promise
    while explaining why it is gone — but by pinning that the one shape the
    docs now describe is the one shape the code accepts."""

    @pytest.mark.parametrize(
        "returned",
        [
            [Limit.per_minute(5)],
            (Limit.per_minute(5),),
            {"api": Limit.per_minute(5)},
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
            _middleware(limit="api")._resolve_limit_config(request=object())
