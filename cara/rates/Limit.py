"""Limit — one rate policy: its pace, its burst and the bucket it spends."""

from __future__ import annotations


def _require_count(name: str, value: object, *, allow_zero: bool) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    if value < 0 or (value == 0 and not allow_zero):
        qualifier = "non-negative" if allow_zero else "positive"
        raise ValueError(f"{name} must be {qualifier}")


class Limit:
    """``limit`` requests per ``period_seconds``, spent from a bucket of ``burst``.

    Enforced as GCRA — the generic cell rate algorithm, a token bucket kept as
    one timestamp. A key may send ``burst`` requests back to back, then one
    every ``period_seconds / limit`` seconds as the bucket refills.

    It replaced a fixed window, which failed in both directions at once. A
    client could spend one window's budget in its last second and the next
    window's in the one after — twice the limit across a boundary — while a
    client that ran out was told to wait for whatever was left of the window,
    up to a whole minute, however lightly it behaved afterwards. The bucket
    refills continuously: an over-pace client is told to come back in the time
    one request takes to earn, and the boundary burst does not exist.

    ``burst`` defaults to ``limit``, so a full bucket absorbs one period's
    worth at once and the steady pace is the limit itself.
    """

    def __init__(self, limit: int, period_seconds: int, *, burst: int | None = None):
        _require_count("limit", limit, allow_zero=True)
        _require_count("period_seconds", period_seconds, allow_zero=True)
        if (limit == 0) != (period_seconds == 0):
            raise ValueError("an unlimited Limit needs both limit and period_seconds at 0")
        resolved_burst = limit if burst is None else burst
        _require_count("burst", resolved_burst, allow_zero=limit == 0)
        if limit == 0 and resolved_burst != 0:
            raise ValueError("an unlimited Limit has no burst")
        self.limit = limit
        self.period_seconds = period_seconds
        self.burst = resolved_burst
        self.key: str | None = None

    @classmethod
    def per_second(cls, limit: int, *, burst: int | None = None) -> Limit:
        return cls(limit, 1, burst=burst)

    @classmethod
    def per_minute(cls, limit: int, *, burst: int | None = None) -> Limit:
        return cls(limit, 60, burst=burst)

    @classmethod
    def per_hour(cls, limit: int, *, burst: int | None = None) -> Limit:
        return cls(limit, 3600, burst=burst)

    @classmethod
    def none(cls) -> Limit:
        """No limit at all — the throttle lets every request through."""
        return cls(0, 0)

    @property
    def unlimited(self) -> bool:
        return self.limit == 0

    @property
    def emission_interval_ms(self) -> int:
        """Milliseconds one request takes to earn, rounded UP.

        Rounding down would mint budget: 7/min is 8571.43ms a request, and a
        truncated 8571 lets every seventh request in a hair early.
        """
        if self.unlimited:
            raise ValueError("an unlimited Limit has no emission interval")
        return -(-self.period_seconds * 1000 // self.limit)

    def by(self, key: str) -> Limit:
        """Name the bucket this Limit spends (``user:42``, ``ip:203.0.113.9``)."""
        if not isinstance(key, str) or not key.strip():
            raise ValueError("rate-limit key must be a non-empty string")
        self.key = key.strip()
        return self
