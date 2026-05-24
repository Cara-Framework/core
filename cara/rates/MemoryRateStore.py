"""Process-local rate-limit fallback for Redis-down windows.

Default rate-limit posture in this framework is "fail-closed": when
``Cache.increment`` raises (Redis unreachable, timing out, refusing
connections) the limiter raises ``ServiceUnavailableException`` so
every caller hits a 503. That is correct when abuse and cache outage
correlate (the textbook attack window) — silently lifting the cap is
worse than refusing traffic. But for installations that prefer
degraded-but-still-bounded throughput over total outage, this module
provides a third option: an in-memory fallback that keeps enforcing
the limit *per worker process* until Redis is healthy again.

What it is — and what it isn't
------------------------------
* Per-process. Each ASGI worker keeps its own counters; a 10 req/min
  cap becomes effectively ``10 × N_workers`` while the fallback is
  engaged. Documented trade-off: we'd rather bound the budget at
  ``N × limit`` than have it be unbounded. When Redis recovers we
  switch straight back to globally-coordinated counting.
* Fixed-window. Matches the existing ``Cache.increment + Cache.ttl``
  semantics exactly so a switch into / out of fallback during a
  request stream produces no behaviour discontinuity beyond the
  per-worker boundary.
* Self-healing. There is no separate retry timer or circuit-breaker
  state machine — every request still calls ``Cache.increment``
  first; a single successful call flips the health flag back to
  healthy and clears the "we're in fallback" warning latch.

Logging policy
--------------
``Log.warning`` fires exactly twice per Redis outage cycle:
  * Once on the transition healthy → unhealthy (so ops sees the
    degraded mode start in the log stream).
  * Once on the transition unhealthy → healthy (so ops sees the
    recovery).
Without the latch every single request during the outage would emit
a duplicate warning — a 50 req/s service in fallback for an hour
would produce 180,000 identical lines.
"""

from __future__ import annotations

import threading
import time
from typing import Any


class MemoryRateLimitStore:
    """Fixed-window counter store backed by an in-process dict.

    The contract mirrors ``Cache.increment`` / ``Cache.ttl`` so the
    rate-limit code can call this store with the same arguments and
    interpret the result the same way. Each key carries an explicit
    ``expires_at`` timestamp; readers compute remaining TTL against
    wall-clock time so a switch into the store mid-window keeps a
    sensible reset deadline.

    Concurrency: the lock guards ``(count, expires_at)`` reads and
    writes so concurrent coroutines on the same worker can't observe a
    half-applied increment. Without the lock the dict ops themselves
    are GIL-atomic but the *increment* (read + modify + write) is not
    — the same race the Redis ``INCR`` migration fixed for the cache
    path.
    """

    def __init__(self) -> None:
        # key -> (count, expires_at_monotonic)
        self._buckets: dict[str, tuple[int, float]] = {}
        self._lock = threading.Lock()

    def increment(self, key: str, ttl_seconds: int) -> int:
        """Add one to the bucket; return the post-increment count.

        Window roll-over: if the recorded ``expires_at`` is already
        past, the bucket has aged out — treat the request as the
        first one in a fresh window. This mirrors Redis ``INCR`` on a
        key with an expired TTL: the key is deleted before the
        increment, so the next caller starts at 1.
        """
        now = time.monotonic()
        with self._lock:
            existing = self._buckets.get(key)
            if existing is None or existing[1] <= now:
                count = 1
                expires_at = now + max(1, int(ttl_seconds))
            else:
                count = existing[0] + 1
                expires_at = existing[1]
            self._buckets[key] = (count, expires_at)
            return count

    def ttl(self, key: str) -> int | None:
        """Remaining seconds until the current window expires.

        Returns ``None`` for unknown keys so the caller can fall back
        to the configured window length (matches ``Cache.ttl``'s
        contract — same "I have nothing to tell you, use your own
        default" sentinel).
        """
        now = time.monotonic()
        with self._lock:
            existing = self._buckets.get(key)
            if existing is None:
                return None
            count, expires_at = existing
            if expires_at <= now:
                # Bucket aged out but we haven't been incremented since;
                # nothing meaningful to report.
                self._buckets.pop(key, None)
                return None
            return max(0, int(expires_at - now))

    def reset(self, key: str) -> None:
        """Drop the bucket — equivalent to ``Cache.forget``."""
        with self._lock:
            self._buckets.pop(key, None)

    def clear(self) -> None:
        """Drop every bucket. Used by tests + by the recovery path
        so a long fallback window doesn't leave stale per-worker
        counts skewing the next outage's accounting."""
        with self._lock:
            self._buckets.clear()

    def prune_expired(self) -> int:
        """Drop expired buckets; return the count removed.

        Bounded memory housekeeping — without it a pathological key
        churn (every request keys on a unique IP, every IP is single-use)
        would let the dict grow unbounded across the fallback window.
        Called opportunistically on each ``increment`` is too aggressive;
        callers invoke it periodically (e.g. once every N requests).
        """
        now = time.monotonic()
        removed = 0
        with self._lock:
            stale = [k for k, (_, exp) in self._buckets.items() if exp <= now]
            for k in stale:
                self._buckets.pop(k, None)
                removed += 1
        return removed

    def __len__(self) -> int:
        # Lockless read — only used by tests / diagnostics, doesn't need
        # the atomicity guarantee.
        return len(self._buckets)


class RedisHealthState:
    """Tracks whether the upstream cache (Redis) is currently healthy
    so the fallback path can emit a transition log exactly once per
    cycle instead of once per request.

    Two booleans suffice: a current state and a "have we logged this
    state already" latch. The transition is computed implicitly by
    comparing the new event ("Redis call failed" / "Redis call
    succeeded") against the latch.
    """

    def __init__(self) -> None:
        self._healthy: bool = True
        self._announced_state: bool = True  # we start "healthy" so no announcement yet
        self._last_failure_at: float | None = None
        self._lock = threading.Lock()

    @property
    def is_healthy(self) -> bool:
        return self._healthy

    @property
    def last_failure_at(self) -> float | None:
        return self._last_failure_at

    def record_failure(self, exc: BaseException) -> bool:
        """Mark Redis unhealthy. Return True iff this is the first
        failure of a new outage cycle (so the caller knows to log)."""
        with self._lock:
            self._healthy = False
            self._last_failure_at = time.monotonic()
            should_announce = self._announced_state is True
            if should_announce:
                self._announced_state = False
            return should_announce

    def record_success(self) -> bool:
        """Mark Redis healthy. Return True iff this is the first
        success after a recorded outage (so the caller logs the
        recovery exactly once)."""
        with self._lock:
            was_unhealthy = not self._healthy
            self._healthy = True
            should_announce = was_unhealthy and (self._announced_state is False)
            if should_announce:
                self._announced_state = True
            return should_announce

    def reset(self) -> None:
        """Test helper — restore the initial healthy/announced state."""
        with self._lock:
            self._healthy = True
            self._announced_state = True
            self._last_failure_at = None


# Module-level singletons. Each ASGI worker has its own — the whole
# point of the in-memory store is that it lives in the worker's
# address space; sharing across workers would defeat the "we don't
# need Redis right now" property.
_memory_store = MemoryRateLimitStore()
_health_state = RedisHealthState()


def get_memory_store() -> MemoryRateLimitStore:
    """Return the worker-local store. Public so tests can clear it."""
    return _memory_store


def get_health_state() -> RedisHealthState:
    """Return the worker-local health tracker. Public for tests."""
    return _health_state


def resolve_fallback_mode() -> str:
    """Read the configured fallback mode.

    Values:
      * ``"memory"`` — engage the in-memory store on cache failure.
      * ``"open"``   — legacy availability-first fail-open posture
        (also accepts the older ``rate.fail_open=True`` knob).
      * ``"closed"`` (default) — raise 503 on cache failure.

    Config lookup is wrapped in a defensive try because this helper
    is called from middleware paths that may run before the Config
    facade is fully bound (early bootstrap, test harnesses).
    """
    try:
        from cara.facades import Config  # type: ignore

        mode = Config.get("rate.fallback_mode", None)
        if isinstance(mode, str):
            normalised = mode.strip().lower()
            if normalised in ("memory", "open", "closed"):
                return normalised
        # Backward-compat: respect ``rate.fail_open`` when the new
        # ``fallback_mode`` key isn't set. Operators who already
        # opted into fail-open don't have to migrate to the new flag.
        if bool(Config.get("rate.fail_open", False)):
            return "open"
    except Exception:
        # Config facade unavailable — assume the safest default.
        pass
    return "closed"


def attempt_with_fallback(
    cache_key: str,
    window_seconds: int,
    max_attempts: int,
) -> tuple[bool, int, int, str]:
    """Single entry point for "increment + read TTL + handle cache outage".

    Both ``RateLimiter.attempt`` and ``ThrottleRequests._attempt_limit``
    call this so the fallback policy lives in one place. The caller
    keeps its own response-shape decisions; this helper only owns the
    counting + the Redis vs. memory decision.

    Returns a four-tuple ``(allowed, remaining, reset_in, backend)``
    where ``backend`` is ``"cache"`` for the Redis path,
    ``"memory"`` for the in-memory fallback, or ``"open"`` for the
    legacy fail-open posture (no counting). The caller decides what
    to log / surface based on the backend.

    Raises ``cara.exceptions.ServiceUnavailableException`` when the
    fallback mode is ``"closed"`` and Redis is unavailable — same
    behaviour the limiter had before this module existed.
    """
    from cara.facades import Cache, Log

    try:
        count = Cache.increment(cache_key, 1, window_seconds)
    except Exception as exc:
        mode = resolve_fallback_mode()
        if _health_state.record_failure(exc):
            try:
                Log.warning(
                    "Rate-limit cache backend unhealthy "
                    f"({exc.__class__.__name__}: {exc}); fallback_mode={mode}",
                    category="rate.fallback",
                )
            except Exception:
                pass

        if mode == "memory":
            count = _memory_store.increment(cache_key, window_seconds)
            ttl = _memory_store.ttl(cache_key)
            allowed = count <= max_attempts
            remaining = max(max_attempts - count, 0)
            reset_in = ttl if ttl is not None else window_seconds
            return allowed, remaining, reset_in, "memory"

        if mode == "open":
            return True, max_attempts, 0, "open"

        from cara.exceptions import ServiceUnavailableException

        raise ServiceUnavailableException(
            "Rate limiter temporarily unavailable",
            retry_after=1,
        ) from exc

    # Cache call succeeded. If we were previously unhealthy, log the
    # recovery exactly once and clear the in-memory bucket so the
    # per-worker count doesn't shadow the now-authoritative Redis
    # count. Skipping the clear leaves a stale higher-of-the-two
    # bucket on the next outage cycle.
    if _health_state.record_success():
        try:
            Log.warning(
                "Rate-limit cache backend recovered; switching back to "
                "globally-coordinated counting",
                category="rate.fallback",
            )
        except Exception:
            pass
        _memory_store.clear()

    allowed = count <= max_attempts
    remaining = max(max_attempts - count, 0)
    try:
        ttl = Cache.ttl(cache_key)
    except Exception:
        ttl = None
    reset_in = ttl if ttl is not None else window_seconds
    return allowed, remaining, reset_in, "cache"


def _reset_for_tests() -> None:
    """Test-only helper — restore module-level state so cases don't
    leak the previous test's outage cycle into the next one."""
    _memory_store.clear()
    _health_state.reset()


__all__ = [
    "MemoryRateLimitStore",
    "RedisHealthState",
    "attempt_with_fallback",
    "get_health_state",
    "get_memory_store",
    "resolve_fallback_mode",
]
