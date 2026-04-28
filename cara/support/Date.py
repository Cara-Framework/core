"""Date — testable now() facade.

Laravel's ``Illuminate\\Support\\Facades\\Date`` parity. Wraps
``pendulum.now`` so tests can freeze, advance, and rewind time
without each test having to know about pendulum's
``set_test_now`` API directly::

    # production code
    created_at = Date.now()

    # in a test
    Date.set_test_now("2026-01-01 00:00:00")
    assert Date.now().to_iso8601_string().startswith("2026-01-01")
    Date.travel(hours=2)
    assert Date.now().hour == 2
    Date.set_test_now(None)            # release the freeze

The point of routing through this facade — instead of letting
every call site reach for ``pendulum.now()`` — is auditability:
one chokepoint where time enters the system, easy to mock,
impossible to "forget" in tests.
"""

from __future__ import annotations

from typing import Any, Optional, Union

import pendulum

# Module-level frozen time. ``None`` means "use real wall clock".
_test_now: Optional[pendulum.DateTime] = None


class Date:
    """Static facade over ``pendulum`` with a testable ``now()``."""

    DEFAULT_TIMEZONE = "UTC"

    # ── now / today ─────────────────────────────────────────────────

    @classmethod
    def now(cls, tz: str = DEFAULT_TIMEZONE) -> pendulum.DateTime:
        """Return the current time, honouring :meth:`set_test_now`."""
        if _test_now is not None:
            # Convert the frozen instant into the requested timezone so
            # callers asking ``Date.now("Europe/Istanbul")`` still see a
            # localised value.
            return _test_now.in_timezone(tz) if tz else _test_now
        return pendulum.now(tz)

    @classmethod
    def today(cls, tz: str = DEFAULT_TIMEZONE) -> pendulum.DateTime:
        """Return today at midnight in ``tz`` — Laravel ``Date::today``."""
        return cls.now(tz).start_of("day")

    @classmethod
    def yesterday(cls, tz: str = DEFAULT_TIMEZONE) -> pendulum.DateTime:
        """Return yesterday at midnight in ``tz``."""
        return cls.today(tz).subtract(days=1)

    @classmethod
    def tomorrow(cls, tz: str = DEFAULT_TIMEZONE) -> pendulum.DateTime:
        """Return tomorrow at midnight in ``tz``."""
        return cls.today(tz).add(days=1)

    # ── parse / create ─────────────────────────────────────────────

    @classmethod
    def parse(
        cls, value: Union[str, int, float, pendulum.DateTime], tz: str = DEFAULT_TIMEZONE
    ) -> pendulum.DateTime:
        """Coerce ``value`` (string / unix timestamp / DateTime) → DateTime."""
        if isinstance(value, pendulum.DateTime):
            return value.in_timezone(tz) if tz else value
        if isinstance(value, (int, float)):
            return pendulum.from_timestamp(value, tz=tz)
        return pendulum.parse(str(value), tz=tz)  # type: ignore[arg-type]

    # ── test helpers ───────────────────────────────────────────────

    @classmethod
    def set_test_now(cls, value: Any) -> None:
        """Freeze :meth:`now` to ``value``. ``None`` releases the freeze.

        Accepts the same inputs as :meth:`parse` plus ``None`` for
        "back to wall clock". When pendulum's optional ``test`` extra
        is installed, also pins ``pendulum.now()`` directly so any
        bare ``pendulum.now()`` call elsewhere in the codebase sees
        the same frozen instant. Without the extra, only call sites
        routed through :class:`Date` / :func:`cara.helpers.now` see
        the freeze.
        """
        global _test_now
        if value is None:
            _test_now = None
            cls._pendulum_release()
            return
        _test_now = cls.parse(value)
        cls._pendulum_freeze(_test_now)

    @classmethod
    def has_test_now(cls) -> bool:
        """True if :meth:`now` is currently frozen."""
        return _test_now is not None

    @classmethod
    def travel(
        cls,
        *,
        days: int = 0,
        hours: int = 0,
        minutes: int = 0,
        seconds: int = 0,
        weeks: int = 0,
    ) -> pendulum.DateTime:
        """Advance the frozen clock — Laravel ``Date::travel``.

        Auto-freezes at the current real time if no freeze is active,
        matching Laravel parity (``travel`` implies the test wants
        deterministic time from this point onward).
        """
        global _test_now
        base = _test_now if _test_now is not None else pendulum.now(cls.DEFAULT_TIMEZONE)
        _test_now = base.add(
            weeks=weeks, days=days, hours=hours, minutes=minutes, seconds=seconds
        )
        # Keep pendulum's internal test-now in lock-step with ours so
        # bare ``pendulum.now()`` calls scattered through the codebase
        # advance with us — best-effort if the test extra is installed.
        cls._pendulum_freeze(_test_now)
        return _test_now

    @classmethod
    def freeze(cls, value: Any = None) -> pendulum.DateTime:
        """Freeze at ``value`` (default = right now). Returns the frozen instant."""
        cls.set_test_now(value if value is not None else pendulum.now(cls.DEFAULT_TIMEZONE))
        return _test_now  # type: ignore[return-value]

    # ── Internal: best-effort pendulum integration ────────────────

    @staticmethod
    def _pendulum_freeze(instant: "pendulum.DateTime") -> None:
        """Pin ``pendulum.now()`` to ``instant`` if the test extra exists.

        Pendulum 3.x ships ``travel_to`` only when installed with the
        ``test`` extra; on a stock install it raises NotImplementedError.
        We swallow that — :class:`Date`'s own freeze still works for
        any call site routed through :meth:`Date.now`.
        """
        try:
            pendulum.travel_to(instant, freeze=True)
        except (NotImplementedError, AttributeError, Exception):
            pass

    @staticmethod
    def _pendulum_release() -> None:
        """Release any pendulum-side freeze if the test extra exists."""
        try:
            pendulum.travel_back()
        except (NotImplementedError, AttributeError, Exception):
            pass


__all__ = ["Date"]
