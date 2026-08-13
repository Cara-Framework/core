"""Redis-backed concurrency limiter for queue jobs.

Unlike ``RateLimited`` (which *skips* jobs exceeding a per-window count),
this middleware enforces a hard ceiling on *simultaneous* executions across
all worker processes. When the ceiling is reached, the job is released
back to the queue with an exponential backoff delay — no data is dropped.

Use case: an upstream API allows 10 concurrent HTTP requests. Setting
``max_concurrent=10`` guarantees we never exceed that across all workers
regardless of how many workers or threads are running.

Usage:
    class MyJob(BaseJob):
        def middleware(self):
            return [
                ConcurrencyLimited(max_concurrent=10, key="upstream_api"),
            ]
"""

from __future__ import annotations

import asyncio
import builtins
import logging
import time
from collections.abc import Callable

from cara.exceptions import MissingContainerBindingException
from cara.observability import counter, metric_name

from .ConcurrencyBackendUnavailable import ConcurrencyBackendUnavailable
from .ConcurrencyExceeded import ConcurrencyExceeded

_logger = logging.getLogger("cara.queue.middleware")


def _redis_fault_types() -> tuple[type[BaseException], ...]:
    """Exception classes that mean "Redis itself failed to answer".

    redis-py raises its OWN ``ConnectionError`` / ``TimeoutError`` /
    ``ResponseError``, all rooted at ``redis.exceptions.RedisError`` — they
    are NOT the builtins of the same name. This middleware used to catch the
    builtins, so every handler it declared was unreachable: a real Redis
    fault escaped the middleware as an unhandled job failure while the
    carefully written degradation paths never ran once.

    Resolved at call time because redis-py is an optional dependency of the
    framework (``RedisCacheDriver`` is the only module that requires it), and
    an install without it must still be able to import this module.
    """
    try:
        from redis.exceptions import RedisError  # local: heavy optional dep

        return (RedisError, OSError)
    except ImportError:
        return (OSError,)


class ConcurrencyLimited:
    """Enforce max concurrent job executions via Redis semaphore.

    Acquire a slot before execution, release after. If no slot is
    available, the job is re-raised with a ``ConcurrencyExceeded``
    so the queue runner can requeue with delay.

    THROUGHPUT WARNING — this ceiling was inert for its whole life until
    the connection lookup below was fixed. ``_get_redis`` duck-typed five
    private attribute names (``cache._redis``, ``cache.store._redis``,
    ``cache.store.redis``, ``cache.connection()``, ``cache.redis``) and the
    ``Cache`` manager has none of them, so the probe returned ``None`` on
    every call and every job sailed through uncapped. Any
    ``max_concurrent`` value tuned while that was true was tuned against
    a limiter that never said no: once this middleware actually holds,
    provider-bound jobs start requeueing as ``ConcurrencyExceeded`` and
    queue depth rises where it previously did not. Re-review the
    configured ceilings before rolling this out, and watch backlog.

    Failure posture (DOCTRINE §9 — an unconfigured gate must not allow):

    * MISCONFIGURATION (the bound cache driver cannot serve a semaphore)
      raises :class:`ConcurrencyBackendUnavailable` on first use. A job
      declaring a hard ceiling that silently gets none is the bug this
      class was written to prevent.
    * FAULT (Redis reachable in principle, this call failed) fails CLOSED:
      one WARNING naming the concurrency key, one counter increment, and
      the job requeues as a throttle. No attempt is consumed, so nothing is
      dropped — but on-call should read a connector backlog during a Redis
      incident as this middleware working, not as a new bug.
    * ABSENCE (no application container/cache binding) is misconfiguration
      and raises :class:`ConcurrencyBackendUnavailable`. Declaring a hard
      ceiling in a process that cannot enforce it must never run uncapped.
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
        if isinstance(max_concurrent, bool) or not isinstance(max_concurrent, int):
            raise TypeError("max_concurrent must be a positive integer")
        if max_concurrent <= 0:
            raise ValueError("max_concurrent must be a positive integer")
        if isinstance(retry_delay, bool) or not isinstance(retry_delay, int):
            raise TypeError("retry_delay must be a non-negative integer")
        if retry_delay < 0:
            raise ValueError("retry_delay must be a non-negative integer")
        if isinstance(slot_ttl, bool) or not isinstance(slot_ttl, int):
            raise TypeError("slot_ttl must be a positive integer")
        if slot_ttl <= 0:
            raise ValueError("slot_ttl must be a positive integer")
        if key is not None and (not isinstance(key, str) or not key.strip()):
            raise ValueError("key must be a non-empty string or None")
        self.max_concurrent = max_concurrent
        self.key = key.strip() if key is not None else None
        self.retry_delay = retry_delay
        self.slot_ttl = slot_ttl

    async def handle(self, job, next_fn: Callable):
        concurrency_key = self.key or job.__class__.__name__
        redis_key = f"{self.REDIS_KEY_PREFIX}{concurrency_key}"

        cache = self._resolve_cache()
        if cache is None:
            raise ConcurrencyBackendUnavailable(
                "ConcurrencyLimited requires an application cache binding; "
                f"cannot enforce the {concurrency_key!r} ceiling."
            )

        slot_id = f"{id(job)}:{time.time()}"
        if not self._try_acquire(cache, concurrency_key, redis_key, slot_id):
            self._log_throttled(concurrency_key)
            await asyncio.sleep(self.retry_delay)
            # Re-raise so the queue runner retries (does not count
            # against max_attempts — ThrottlesExceptions handles that).
            raise ConcurrencyExceeded(
                f"Concurrency limit ({self.max_concurrent}) reached for {concurrency_key}"
            )
        try:
            return await self._call_next(job, next_fn)
        finally:
            self._release(cache, concurrency_key, redis_key, slot_id)

    @staticmethod
    async def _call_next(job, next_fn: Callable):
        """Invoke the next link, awaiting it when it returns a coroutine.

        Middleware must observe the REAL execution outcome, not the
        coroutine object, or the ``finally`` that releases the slot runs
        before the job has done any work.
        """
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

    def _try_acquire(
        self,
        cache,
        concurrency_key: str,
        redis_key: str,
        slot_id: str,
    ) -> bool:
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

        The pipeline path that race lived in is GONE, not kept as a
        fallback. It survived a full release behind a comment claiming
        "the only callers reaching this branch are single-threaded test
        fakes" while the ``except`` six lines above routed every failed
        EVAL — exactly what a Redis Cluster multi-key script, a script
        cache eviction or a transient disconnect produces — into it under
        production load. A driver that cannot run EVAL cannot enforce this
        ceiling at all and must say so (:class:`ConcurrencyBackendUnavailable`),
        not fall back to a known-broken read-then-write.
        """
        redis = self._connection(cache)
        now = time.time()
        expiry = now + self.slot_ttl
        key_ttl = self.slot_ttl + 60

        try:
            result = redis.eval(
                self._ACQUIRE_LUA,
                1,  # numkeys
                redis_key,
                str(now),
                str(expiry),
                str(int(self.max_concurrent)),
                slot_id,
                str(int(key_ttl)),
            )
        except _redis_fault_types() as exc:
            self._log_backend_fault(concurrency_key, exc)
            self._count_backend_fault(concurrency_key)
            return False
        return bool(int(result if result is not None else 0))

    def _release(self, cache, concurrency_key: str, redis_key: str, slot_id: str) -> None:
        try:
            self._connection(cache).zrem(redis_key, slot_id)
        except _redis_fault_types() as exc:
            # The slot carries an expiry score and is reclaimed by TTL, but
            # an outage during release must remain observable.
            self._log_backend_fault(concurrency_key, exc)
            self._count_backend_fault(concurrency_key)

    @staticmethod
    def _connection(cache):
        """The raw Redis client behind the configured cache driver.

        Goes through ``Cache.driver()`` — the manager's one supported
        accessor — and then the driver's own ``connection()``. The previous
        implementation duck-typed five private attribute names on the
        manager and its hypothetical ``store``; ``RedisCacheDriver`` has
        never carried any of them (its client is ``_client``), so this
        returned ``None`` on every single call and the ceiling silently
        never engaged.

        Raises instead of degrading when the bound driver cannot serve a
        semaphore. A job that declares a hard ceiling and is quietly given
        none is precisely the unconfigured-gate-that-allows §9 forbids, and
        it is invisible: every dashboard shows the cap as enforced.
        """
        resolve_driver = getattr(cache, "driver", None)
        if not callable(resolve_driver):
            raise ConcurrencyBackendUnavailable(
                "ConcurrencyLimited needs the Cache manager; the container's "
                f"'cache' binding is a {type(cache).__name__} with no driver()."
            )
        driver = resolve_driver()
        connection = getattr(driver, "connection", None)
        if not callable(connection):
            driver_name = getattr(driver, "driver_name", type(driver).__name__)
            raise ConcurrencyBackendUnavailable(
                f"ConcurrencyLimited requires a Redis-backed cache driver; "
                f"{driver_name!r} exposes no connection(). Set the cache driver "
                "to redis on every deployable that runs a job declaring "
                "ConcurrencyLimited, or drop the middleware from that job."
            )
        return connection()

    @staticmethod
    def _resolve_cache():
        try:
            application = builtins.app()
            cache_service = application.make("cache")
            if cache_service is None:
                return None
            return cache_service
        except (
            ImportError,
            RuntimeError,
            AttributeError,
            MissingContainerBindingException,
        ):
            return None

    @staticmethod
    def _log_throttled(key: str) -> None:
        _logger.debug("Concurrency limit reached for %s, requeueing with delay", key)

    @staticmethod
    def _log_backend_fault(key: str, exc: BaseException) -> None:
        """WARNING for a fault-path fail-closed. Not optional garnish.

        Failing closed turns a Redis outage into an unbounded throttle
        requeue loop that never dead-letters. Without a line naming the
        concurrency key that loop is exactly as invisible as the silent
        ceiling loss it replaced — the queue just gets deeper and nothing
        says why.
        """
        _logger.warning(
            "concurrency backend unavailable for %s — failing closed: %s",
            key,
            exc,
            exc_info=exc,
        )

    @staticmethod
    def _count_backend_fault(key: str) -> None:
        """Counter for the same event, so alerting can see it without logs.

        Distinguishes "the ceiling is holding" (jobs requeue, backlog grows)
        from "the ceiling is unreachable" (jobs requeue, backlog grows) —
        the two look identical in every queue-depth gauge.
        """
        try:
            counter(
                metric_name("queue_concurrency_backend_faults_total"),
                "Concurrency-slot acquisitions that failed closed because the "
                "Redis backend could not answer.",
                ("concurrency_key",),
            ).labels(concurrency_key=key).inc()
        except Exception:
            _logger.debug("concurrency backend fault metric failed", exc_info=True)
