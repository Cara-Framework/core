"""Fail-closed accounting against the shared rate-limit authority."""

from __future__ import annotations

import contextlib

import cara.facades as facades
from cara.exceptions import RateLimitConfigurationException, ServiceUnavailableException

from ._RateBackendHealth import _RateBackendHealth

_health = _RateBackendHealth()


def attempt_rate_limit(
    cache_key: str,
    window_seconds: int,
    max_attempts: int,
) -> tuple[bool, int, int]:
    """Increment the authoritative bucket or deny while it is unavailable."""
    if not isinstance(cache_key, str) or not cache_key:
        raise RateLimitConfigurationException(
            "Rate-limit cache key must be a non-empty string."
        )
    for name, value in (
        ("window_seconds", window_seconds),
        ("max_attempts", max_attempts),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise RateLimitConfigurationException(
                f"Rate-limit {name} must be a positive integer."
            )
    try:
        count = facades.Cache.increment(cache_key, 1, window_seconds)
        if isinstance(count, bool) or not isinstance(count, int) or count < 1:
            raise RuntimeError("rate-limit counter returned an invalid value")
        ttl = facades.Cache.ttl(cache_key)
        if isinstance(ttl, bool) or not isinstance(ttl, int) or ttl < 0:
            raise RuntimeError("rate-limit counter has no authoritative expiry")
    except Exception as exc:
        _record_failure(exc)
        raise ServiceUnavailableException(
            "Rate limiter temporarily unavailable",
            retry_after=1,
        ) from exc

    if _health.record_success():
        with contextlib.suppress(
            OSError,
            RuntimeError,
            AttributeError,
            ConnectionError,
        ):
            facades.Log.warning(
                "Rate-limit cache backend recovered",
                category="rate.backend",
            )

    allowed = count <= max_attempts
    remaining = max(max_attempts - count, 0)
    return allowed, remaining, ttl


def _record_failure(exc: Exception) -> None:
    if not _health.record_failure():
        return
    with contextlib.suppress(
        OSError,
        RuntimeError,
        AttributeError,
        ConnectionError,
    ):
        facades.Log.warning(
            "Rate-limit cache backend unhealthy (%s: %s); denying requests",
            exc.__class__.__name__,
            exc,
            category="rate.backend",
        )


def _reset_for_tests() -> None:
    _health.reset()


__all__ = ["attempt_rate_limit"]
