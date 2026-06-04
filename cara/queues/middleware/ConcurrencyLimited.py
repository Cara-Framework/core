"""Redis-backed concurrency limiter for queue jobs.

Unlike ``RateLimited`` (which *skips* jobs exceeding a per-window count),
this middleware enforces a hard ceiling on *simultaneous* executions across
all worker processes. When the ceiling is reached, the job is released
back to the queue with an exponential backoff delay — no data is dropped.

Use case: scrape.do allows 10 concurrent HTTP requests. Setting
``max_concurrent=10`` guarantees we never exceed that across all workers
regardless of how many workers or threads are running.

Usage:
    class CollectProductJob(BaseJob):
        def middleware(self):
            return [
                ConcurrencyLimited(max_concurrent=10, key="scrapedo"),
            ]
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable


class ConcurrencyLimited:
    """Enforce max concurrent job executions via Redis semaphore.

    Acquire a slot before execution, release after. If no slot is
    available, the job is re-raised with a ``ConcurrencyExceeded``
    so the queue runner can requeue with delay.
    """

    REDIS_KEY_PREFIX = "cara:concurrency:"
    DEFAULT_SLOT_TTL = 120  # seconds — auto-expire dead slots

    def __init__(
        self,
        max_concurrent: int = 10,
        key: str | None = None,
        retry_delay: int = 5,
        slot_ttl: int = DEFAULT_SLOT_TTL,
    ):
        self.max_concurrent = max_concurrent
        self.key = key
        self.retry_delay = retry_delay
        self.slot_ttl = slot_ttl

    async def handle(self, job, next_fn: Callable):
        concurrency_key = self.key or job.__class__.__name__
        redis_key = f"{self.REDIS_KEY_PREFIX}{concurrency_key}"

        cache = self._resolve_cache()
        slot_id = f"{id(job)}:{time.time()}"

        if cache is not None:
            acquired = self._try_acquire(cache, redis_key, slot_id)
            if not acquired:
                self._log_throttled(concurrency_key)
                await asyncio.sleep(self.retry_delay)
                # Re-raise so the queue runner retries (does not count
                # against max_attempts — ThrottlesExceptions handles that).
                raise ConcurrencyExceeded(
                    f"Concurrency limit ({self.max_concurrent}) reached for {concurrency_key}"
                )
            try:
                result = next_fn(job)
                if asyncio.iscoroutine(result):
                    return await result
                return result
            finally:
                self._release(cache, redis_key, slot_id)
        else:
            # No Redis — fall through without limiting.
            result = next_fn(job)
            if asyncio.iscoroutine(result):
                return await result
            return result

    # Lua script that combines prune-expired + count + conditional-add
    # in a single server-side EVAL. Redis evaluates the whole script
    # under the keyspace lock so concurrent workers cannot slip past
    # the count check between commands. Returns 1 if a slot was
    # acquired, 0 if the cap is full.
    #
    # KEYS[1] = redis_key (sorted set holding slot_id → expiry score)
    # ARGV[1] = now (unix ts)
    # ARGV[2] = expiry (now + slot_ttl)
    # ARGV[3] = max_concurrent
    # ARGV[4] = slot_id
    # ARGV[5] = key_ttl (slot_ttl + headroom)
    _ACQUIRE_LUA = (
        "redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1]) "
        "local n = redis.call('ZCARD', KEYS[1]) "
        "if n >= tonumber(ARGV[3]) then return 0 end "
        "redis.call('ZADD', KEYS[1], ARGV[2], ARGV[4]) "
        "redis.call('EXPIRE', KEYS[1], ARGV[5]) "
        "return 1"
    )

    def _try_acquire(self, cache, redis_key: str, slot_id: str) -> bool:
        """Atomic slot acquisition using a single Redis EVAL.

        Pre-fix this method issued ``ZCARD`` (read) and ``ZADD`` (write)
        as separate round-trips. Two workers racing on the last free
        slot both read ``zcard < max_concurrent``, both passed the
        gate, both ``ZADD``-ed themselves in — and the cap was silently
        exceeded by one for every concurrent racer. Routing prune +
        count + conditional-add through a Lua script closes the
        TOCTOU window: Redis evaluates the script under its keyspace
        lock so the second concurrent caller observes the first's
        ZADD before its own ZCARD.

        Falls back to the pre-fix pipeline path when the client doesn't
        expose ``eval`` (some fake/in-memory drivers used in early-boot
        tests) — those drivers don't have concurrent callers anyway.
        """
        try:
            redis = self._get_redis(cache)
            if redis is None:
                return True  # degrade gracefully

            now = time.time()
            expiry = now + self.slot_ttl
            key_ttl = self.slot_ttl + 60

            evaluator = getattr(redis, "eval", None)
            if callable(evaluator):
                try:
                    result = evaluator(
                        self._ACQUIRE_LUA,
                        1,  # numkeys
                        redis_key,
                        str(now),
                        str(expiry),
                        str(int(self.max_concurrent)),
                        slot_id,
                        str(int(key_ttl)),
                    )
                    return bool(int(result if result is not None else 0))
                except Exception:
                    # If EVAL itself fails (script error, redis cluster
                    # quirk), fall through to the legacy path below
                    # rather than dropping every dispatch.
                    pass

            # Legacy fallback — kept for non-Redis backends. Still
            # TOCTOU-prone, but the only callers reaching this branch
            # are single-threaded test fakes.
            pipe = redis.pipeline(True)
            pipe.zremrangebyscore(redis_key, "-inf", now)
            pipe.zcard(redis_key)
            results = pipe.execute()
            active_count = results[1]

            if active_count >= self.max_concurrent:
                return False

            redis.zadd(redis_key, {slot_id: now + self.slot_ttl})
            redis.expire(redis_key, self.slot_ttl + 60)
            return True
        except Exception:
            return True  # degrade gracefully on Redis errors

    def _release(self, cache, redis_key: str, slot_id: str) -> None:
        try:
            redis = self._get_redis(cache)
            if redis:
                redis.zrem(redis_key, slot_id)
        except Exception:
            pass  # TTL ensures cleanup

    @staticmethod
    def _get_redis(cache):
        """Get the raw Redis connection from the Cache service."""
        try:
            if hasattr(cache, "_redis"):
                return cache._redis
            store = getattr(cache, "store", None)
            if store and hasattr(store, "_redis"):
                return store._redis
            if store and hasattr(store, "redis"):
                return store.redis
            conn = getattr(cache, "connection", None)
            if callable(conn):
                return conn()
            redis_attr = getattr(cache, "redis", None)
            if redis_attr:
                return redis_attr
            return None
        except Exception:
            return None

    @staticmethod
    def _resolve_cache():
        try:
            from bootstrap import application

            cache_service = application.make("cache")
            if cache_service is None:
                return None
            return cache_service
        except Exception:
            return None

    @staticmethod
    def _log_throttled(key: str) -> None:
        try:
            from cara.facades import Log

            Log.debug(
                f"Concurrency limit reached for {key}, requeueing with delay",
                category="cara.queue.middleware",
            )
        except ImportError:
            pass


class ConcurrencyExceeded(Exception):
    """Raised when concurrency limit is exceeded.

    The queue runner should requeue the job with a delay. This exception
    does NOT count against max_attempts since it's a transient throttle,
    not a job failure.

    The ``is_throttle`` class-attribute is the load-bearing signal —
    ``JobProcessor._requeue_with_delay`` reads it via ``getattr`` to
    suppress the normal ``attempts += 1`` write when republishing. Pre-
    fix the docstring promised this behaviour but no code wired it up,
    so a scrape-pipeline burst that triggered three throttles in a row
    drained healthy jobs into the DLQ purely from losing the slot
    lottery — not from any real failure.
    """

    is_throttle: bool = True
