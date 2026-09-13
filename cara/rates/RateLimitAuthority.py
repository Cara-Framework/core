"""Fail-closed GCRA accounting against the shared rate-limit authority."""

from __future__ import annotations

import contextlib
import logging
import math

import cara.facades as facades
from cara.exceptions import RateLimitConfigurationException, ServiceUnavailableException
from cara.observability import counter, metric_name

from ._RateBackendHealth import _RateBackendHealth
from .Limit import Limit
from .RateLimitDecision import RateLimitDecision

_health = _RateBackendHealth()
_logger = logging.getLogger("cara.rates")

# One namespace for every bucket, so the state behind a 429 can be found or
# cleared without knowing which transport spent it.
_KEY_PREFIX = "rate:"


def rate_limit_cache_key(key: str) -> str:
    """The cache key the bucket ``Limit.by(key)`` spends lives under."""
    return f"{_KEY_PREFIX}{key}"


def attempt_rate_limit(limit: Limit, *, cost: int = 1) -> RateLimitDecision:
    """Spend ``cost`` from ``limit``'s bucket, or deny while the authority is down.

    The cache's GCRA cell decides atomically against one clock for every
    worker; a refused spend writes nothing, so a client hammering a closed
    bucket does not push its own reopening further away. When the cell cannot
    answer — backend down, or an answer that is not one — the request is
    refused with a retryable 503 rather than let through uncounted (§9).
    """
    _validate(limit, cost)
    try:
        state = facades.Cache.throttle(
            rate_limit_cache_key(limit.key),
            emission_interval_ms=limit.emission_interval_ms,
            burst=limit.burst,
            cost=cost,
        )
        allowed, remaining, retry_after_ms, reset_after_ms = _checked_state(state)
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

    return RateLimitDecision(
        allowed=allowed,
        limit=limit.limit,
        period_seconds=limit.period_seconds,
        remaining=remaining,
        retry_after=math.ceil(retry_after_ms / 1000),
        reset_after=math.ceil(reset_after_ms / 1000),
    )


def _validate(limit: object, cost: object) -> None:
    if not isinstance(limit, Limit):
        raise RateLimitConfigurationException("A rate-limit attempt spends a Limit.")
    if limit.unlimited:
        raise RateLimitConfigurationException(
            "An unlimited Limit has no bucket; let the request through without an attempt."
        )
    if not limit.key:
        raise RateLimitConfigurationException(
            "A Limit names its bucket with .by(...) before it can be spent."
        )
    if isinstance(cost, bool) or not isinstance(cost, int) or cost < 1:
        raise RateLimitConfigurationException("Rate-limit cost must be a positive integer.")
    if cost > limit.burst:
        raise RateLimitConfigurationException(
            f"A cost of {cost} can never fit a burst of {limit.burst}."
        )


def _checked_state(state: object) -> tuple[bool, int, int, int]:
    """The cell's answer, or ``RuntimeError`` when it is not one."""
    if not isinstance(state, tuple) or len(state) != 4:
        raise RuntimeError("rate-limit cell returned an invalid state")
    allowed, remaining, retry_after_ms, reset_after_ms = state
    if not isinstance(allowed, bool):
        raise RuntimeError("rate-limit cell returned an invalid decision")
    for value in (remaining, retry_after_ms, reset_after_ms):
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise RuntimeError("rate-limit cell returned an invalid measurement")
    return allowed, remaining, retry_after_ms, reset_after_ms


def _record_failure(exc: Exception) -> None:
    _count_backend_fault()
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


def _count_backend_fault() -> None:
    """Every attempt the authority could not decide, on the counter alerting reads.

    The warning above is logged once per outage; the counter is not, so an alert
    can see an outage that has been refusing every throttled request for an hour.
    """
    try:
        counter(
            metric_name("rate_limit_backend_faults_total"),
            "Rate-limit attempts refused because the cache authority could not answer.",
        ).inc()
    except Exception:
        _logger.debug("rate-limit backend fault metric failed", exc_info=True)


def _reset_for_tests() -> None:
    _health.reset()


__all__ = ["attempt_rate_limit", "rate_limit_cache_key"]
