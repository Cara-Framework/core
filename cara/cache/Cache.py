"""
Central Cache Manager for the Cara framework.

This module provides the Cache class, which manages multiple cache drivers and delegates cache
operations to the appropriate driver instance.

Supports Laravel-style cache tags and cache locks for distributed systems.
"""

from typing import Any, List, Optional

from cara.cache.contracts import Cache
from cara.exceptions import DriverNotRegisteredException


class CacheLock:
    """
    Distributed lock using cache.

    Prevents race conditions in distributed systems using cache as storage.
    """

    def __init__(self, cache, key: str, timeout: int = 86400, owner: Optional[str] = None):
        """
        Initialize a cache lock.

        Owner default: a unique per-instance string. The previous default of
        ``"default"`` collided across processes — process B could ``release``
        process A's lock because both stored the literal string ``"default"``
        as the owner field, and ``forget_if(key, "default")`` matched. Now
        every CacheLock instance gets ``f"{pid}:{uuid}"`` so cross-process
        ownership is unambiguous.

        Args:
            cache: Cache driver instance
            key: Lock key
            timeout: Lock timeout in seconds (default: 24 hours)
            owner: Lock owner identifier (for distinguishing lock holders).
                Pass an explicit value when multiple coroutines / threads
                share the same lock and you want the same owner to be able
                to re-acquire / release. Otherwise leave None for a unique
                per-instance owner.
        """
        import os
        import uuid as _uuid

        self.cache = cache
        self.key = f"lock:{key}"
        self.timeout = timeout
        self.owner = owner or f"{os.getpid()}:{_uuid.uuid4().hex}"

    # Spin interval between failed acquires. 100ms balances:
    # responsiveness when the lock holder finishes quickly vs. wasted
    # cache hits while waiting. Could be made configurable per-call,
    # but the same number is correct for every site so far.
    _SPIN_INTERVAL_S = 0.1

    def acquire(self, timeout: int = 0) -> bool:
        """
        Attempt to acquire the lock (sync API).

        Args:
            timeout: Max seconds to wait for lock (0 = non-blocking)

        Returns:
            True if lock acquired, False otherwise

        Async callers
        -------------
        ``acquire`` blocks the calling thread on its retry sleep — fine
        from sync code (CLI commands, sync workers), but in an async
        context it would stall the event loop. Async callers should use
        ``await acquire_async(timeout)`` instead, which yields control
        with ``asyncio.sleep`` so other coroutines keep making progress
        while we wait.
        """
        import time
        start = time.time()

        while True:
            # Try to add the lock key (only succeeds if key doesn't exist)
            if self.cache.add(self.key, self.owner, self.timeout):
                return True

            if timeout == 0 or (time.time() - start) >= timeout:
                return False

            time.sleep(self._SPIN_INTERVAL_S)

    async def acquire_async(self, timeout: int = 0) -> bool:
        """Async-safe variant of :meth:`acquire`.

        Yields the event loop on each retry interval instead of
        blocking the worker thread. The cache primitive itself
        (``cache.add``) is sync — that's a single fast op so it's
        acceptable to call inline.
        """
        import asyncio
        import time
        start = time.time()

        while True:
            if self.cache.add(self.key, self.owner, self.timeout):
                return True

            if timeout == 0 or (time.time() - start) >= timeout:
                return False

            await asyncio.sleep(self._SPIN_INTERVAL_S)

    def release(self) -> bool:
        """Release the lock if (and only if) it is still held by this owner.

        Uses ``forget_if`` so the ownership check and the delete happen as
        a single atomic step. The previous "check then delete" pattern had
        a TOCTOU race: between ``get`` and ``forget`` the lock could
        expire and be reacquired by another owner whose key would then be
        wrongly deleted.
        """
        forget_if = getattr(self.cache, "forget_if", None)
        if callable(forget_if):
            return bool(forget_if(self.key, self.owner))
        # Fallback for drivers that haven't implemented the CAS primitive
        # yet — preserves prior behaviour but has a TOCTOU race window.
        # Drivers should implement ``forget_if`` for safe distributed locks.
        import logging
        logging.getLogger("cara.cache").warning(
            "CacheLock: driver %s lacks forget_if(); using non-atomic fallback. "
            "Implement forget_if() for safe distributed lock release.",
            type(self.cache).__name__,
        )
        if self.cache.get(self.key) == self.owner:
            return self.cache.forget(self.key)
        return False

    def __enter__(self):
        """Sync context manager entry — raises if lock cannot be
        acquired within ``self.timeout``.

        NOTE: a 24-hour default block is a footgun when used from an
        async handler (the sync ``acquire`` ``time.sleep``s, freezing
        the event loop). Async callers should use ``async with`` /
        ``acquire_async``.
        """
        if not self.acquire(timeout=self.timeout):
            raise TimeoutError(
                f"Could not acquire lock '{self.key}' within {self.timeout}s"
            )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Sync context manager exit."""
        self.release()
        return False

    async def __aenter__(self):
        """Async context manager entry — uses ``acquire_async`` so the
        event loop keeps running while we wait."""
        if not await self.acquire_async(timeout=self.timeout):
            raise TimeoutError(
                f"Could not acquire lock '{self.key}' within {self.timeout}s"
            )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        self.release()
        return False


class CacheTaggedStore:
    """
    Tagged cache operations.

    Allows grouping cache entries by tags for bulk invalidation.
    """

    def __init__(self, cache, tags: List[str]):
        """
        Initialize tagged cache store.

        Args:
            cache: Cache driver instance
            tags: List of tags to apply to operations
        """
        self.cache = cache
        self.tags = tags

    def _build_tagged_key(self, key: str) -> str:
        """Build a key with tag prefix."""
        tag_prefix = ":".join(self.tags)
        return f"{tag_prefix}:{key}"

    def get(self, key: str, default: Any = None) -> Any:
        """Get value from tagged cache."""
        return self.cache.get(self._build_tagged_key(key), default)

    def put(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Store value in tagged cache."""
        self.cache.put(self._build_tagged_key(key), value, ttl)

    def forever(self, key: str, value: Any) -> None:
        """Store value permanently in tagged cache."""
        self.cache.forever(self._build_tagged_key(key), value)

    def forget(self, key: str) -> bool:
        """Remove value from tagged cache."""
        return self.cache.forget(self._build_tagged_key(key))

    def flush(self) -> int:
        """Flush all entries with these tags."""
        # Flush all keys matching tag pattern
        pattern = ":".join(self.tags) + ":*"
        return self.cache.forget_pattern(pattern)


class Cache:
    """
    Central cache manager. Delegates get/put/forget/flush to registered driver instances.

    The default driver name is injected via constructor (from CacheProvider).
    Supports Laravel-style cache tags and distributed cache locks.
    """

    def __init__(self, application, default_driver: str):
        self.application = application
        self._stores: dict[str, Cache] = {}
        self._default_driver: str = default_driver
        self._tags: List[str] = []

    def add_driver(self, driver_name: str, driver: Cache) -> None:
        """Register a driver instance under `driver_name`."""
        self._stores[driver_name] = driver

    def driver(self, name: Optional[str] = None) -> Cache:
        """
        Get a cache driver instance by name.

        Raises DriverNotRegisteredException if missing.
        """
        chosen = name if name is not None else self._default_driver

        if chosen not in self._stores:
            raise DriverNotRegisteredException(
                f"Cache driver '{chosen}' is not registered."
            )

        return self._stores[chosen]

    def get(
        self,
        key: str,
        default: Any = None,
        driver_name: Optional[str] = None,
    ) -> Any:
        """Retrieve a value from cache via the given driver (or default)."""
        return self.driver(driver_name).get(key, default)

    def put(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
        driver_name: Optional[str] = None,
    ) -> None:
        """Store a value under `key` with optional TTL (seconds) via the given driver."""
        self.driver(driver_name).put(key, value, ttl)

    def forever(
        self,
        key: str,
        value: Any,
        driver_name: Optional[str] = None,
    ) -> None:
        """Store a value permanently (no expiration) via the given driver."""
        self.driver(driver_name).forever(key, value)

    def forget(self, key: str, driver_name: Optional[str] = None) -> bool:
        """
        Remove a key from cache via the given driver.

        Returns True if deleted.
        """
        return self.driver(driver_name).forget(key)

    def flush(self, driver_name: Optional[str] = None) -> None:
        """Flush (clear) all entries from the given driver."""
        self.driver(driver_name).flush()

    def has(self, key: str, driver_name: Optional[str] = None) -> bool:
        """Check if a key exists in cache via the given driver."""
        return self.driver(driver_name).has(key)

    def add(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
        driver_name: Optional[str] = None,
    ) -> bool:
        """Add a value only if key doesn't exist via the given driver."""
        return self.driver(driver_name).add(key, value, ttl)

    def remember(
        self,
        key: str,
        ttl: int,
        callback,
        driver_name: Optional[str] = None,
        *,
        stampede_lock_seconds: int = 30,
    ) -> Any:
        """Get value from cache or execute callback and cache the result.

        Stampede protection
        -------------------
        ``Cache.remember`` is the canonical "compute-once, share-many"
        primitive. Without locking, a popular key expiring under load
        means every concurrent caller misses, every caller runs the
        callback (often a heavy SQL aggregate or external API call),
        and the worst spike happens at exactly the moment cache was
        supposed to absorb load.

        This wrapper acquires a short-lived ``stampede:remember:<key>``
        lock around the miss path. Losers of the lock wait briefly
        for the winner's result; on timeout they fall back to running
        the callback themselves rather than serving wrong-or-empty.

        Apps that don't want this behaviour (rare — only when the
        callback is cheap and uncacheable) can pass
        ``stampede_lock_seconds=0`` to disable the lock entirely.
        """
        driver = self.driver(driver_name)

        # Fast path — hit. No lock needed when we have a value already.
        _MISSING = object()
        cached = driver.get(key, _MISSING)
        if cached is not _MISSING:
            return cached

        # Drivers without ``add`` can't gate the regen with a lock.
        # Fall back to the un-protected behaviour the original
        # ``remember`` had.
        if stampede_lock_seconds <= 0 or not hasattr(driver, "add"):
            return driver.remember(key, ttl, callback)

        # Try to claim the regen slot. ``add`` is atomic on every
        # driver in this codebase (Redis SET NX, file driver O_EXCL).
        lock_key = f"stampede:remember:{key}"
        try:
            won = driver.add(lock_key, "1", stampede_lock_seconds)
        except Exception:
            won = False

        if won:
            try:
                value = callback()
                driver.put(key, value, ttl)
                return value
            finally:
                # Release the regen slot; another future expiry
                # round will grab it again.
                try:
                    driver.forget(lock_key)
                except Exception:
                    pass

        # Lost the race — wait briefly for the winner to populate the
        # key, then re-read. The poll interval is short (50ms) but
        # capped at the lock's lifetime so we don't deadlock if the
        # winner crashes.
        import time as _time

        deadline = _time.time() + stampede_lock_seconds
        while _time.time() < deadline:
            cached = driver.get(key, _MISSING)
            if cached is not _MISSING:
                return cached
            _time.sleep(0.05)

        # Winner crashed or callback runs longer than the lock TTL.
        # Run the callback ourselves rather than return None — the
        # caller's contract is "you'll get the value or this raises".
        value = callback()
        driver.put(key, value, ttl)
        return value

    def forget_pattern(self, pattern: str, driver_name: Optional[str] = None) -> int:
        """
        Delete multiple keys matching a pattern.

        Args:
            pattern: Glob-style pattern (e.g., "home:*", "products:featured:*")
            driver_name: Optional driver name (uses default if not specified)

        Returns:
            Number of keys deleted
        """
        return self.driver(driver_name).forget_pattern(pattern)

    def forget_by_prefix(
        self, prefix: str, driver_name: Optional[str] = None,
    ) -> int:
        """
        Delete every key starting with ``prefix``.

        Convenience wrapper over ``forget_pattern`` — most callers only
        ever want a prefix sweep ("category:facets:") and don't need to
        compose glob patterns themselves. Appends ``*`` so the driver
        sees a valid glob.
        """
        return self.driver(driver_name).forget_pattern(f"{prefix}*")

    def ttl(self, key: str, driver_name: Optional[str] = None) -> Optional[int]:
        """
        Remaining seconds-to-live for ``key``.

        Returns ``None`` when the key is missing or has no expiry, and
        a non-negative int otherwise. Useful for accurate ``Retry-After``
        headers on rate-limit responses, which previously reported the
        full window length regardless of when in the window the bucket
        filled.

        Drivers that don't expose TTL (e.g. ``NullCacheDriver``) should
        return ``None`` from ``ttl(...)``. The wrapper falls back to
        ``None`` if the driver lacks the method, so callers don't need
        to feature-detect.
        """
        driver = self.driver(driver_name)
        ttl_fn = getattr(driver, "ttl", None)
        if not callable(ttl_fn):
            return None
        try:
            return ttl_fn(key)
        except Exception:
            return None

    def increment(
        self,
        key: str,
        amount: int = 1,
        ttl: Optional[int] = None,
        driver_name: Optional[str] = None,
    ) -> int:
        """
        Atomically increment a counter at ``key`` by ``amount``.

        Initialises to ``amount`` if the key doesn't exist. ``ttl`` is
        applied on the first set so the counter expires after a deploy
        without manual sweeps. Backed by Redis ``INCRBY`` on the redis
        driver; the file driver emulates with a read-modify-write under
        a lock.
        """
        return self.driver(driver_name).increment(key, amount, ttl)

    def decrement(
        self,
        key: str,
        amount: int = 1,
        ttl: Optional[int] = None,
        driver_name: Optional[str] = None,
    ) -> int:
        """
        Atomically decrement a counter at ``key`` by ``amount``.

        Implemented as ``increment(key, -amount)`` — drivers don't need
        a separate decrement primitive. Returning value can go negative
        if callers decrement past zero; bound-checking is the caller's
        responsibility.
        """
        return self.driver(driver_name).increment(key, -int(amount), ttl)

    def tags(self, *tags: str, driver_name: Optional[str] = None) -> "CacheTaggedStore":
        """
        Tag cache entries for bulk invalidation (Laravel-style).

        Example:
            cache.tags("posts", "featured").put("post_1", post_data, ttl=3600)
            cache.tags("posts").flush()  # Flush all posts

        Args:
            tags: One or more tag strings
            driver_name: Optional driver name (uses default if not specified)

        Returns:
            CacheTaggedStore instance for tagged operations
        """
        driver = self.driver(driver_name)
        return CacheTaggedStore(driver, list(tags))

    def lock(
        self,
        key: str,
        timeout: int = 86400,
        owner: Optional[str] = None,
        driver_name: Optional[str] = None,
    ) -> CacheLock:
        """
        Get a distributed cache lock (Laravel-style).

        Useful for preventing race conditions in distributed systems.

        Example:
            lock = cache.lock("user_export")
            if lock.acquire():
                try:
                    # Do exclusive work
                    export_data()
                finally:
                    lock.release()

            # Or use as context manager:
            with cache.lock("user_export") as lock:
                export_data()

        Args:
            key: Lock key name
            timeout: Lock timeout in seconds (default: 24 hours)
            owner: Lock owner identifier
            driver_name: Optional driver name (uses default if not specified)

        Returns:
            CacheLock instance
        """
        driver = self.driver(driver_name)
        return CacheLock(driver, key, timeout, owner)
