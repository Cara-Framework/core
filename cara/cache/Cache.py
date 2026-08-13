"""
Central Cache Manager for the Cara framework.

This module provides the Cache class, which manages multiple cache drivers and delegates cache
operations to the appropriate driver instance.

Supports Laravel-style cache tags and cache locks for distributed systems.
"""

from __future__ import annotations

import logging
import time as _time
from collections.abc import Callable
from typing import Any

from cara.exceptions import DriverNotRegisteredException

from .CacheLock import CacheLock
from .CacheTaggedStore import CacheTaggedStore

_logger = logging.getLogger("cara.cache")


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
        self._tags: list[str] = []

    def add_driver(self, driver_name: str, driver: Cache) -> None:
        """Register a driver instance under `driver_name`."""
        self._stores[driver_name] = driver

    def driver(self, name: str | None = None) -> Cache:
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
        driver_name: str | None = None,
        *,
        strict: bool = True,
    ) -> Any:
        """Retrieve a value from cache via the given driver (or default)."""
        return self.driver(driver_name).get(key, default, strict=strict)

    def put(
        self,
        key: str,
        value: Any,
        ttl: int | None = None,
        driver_name: str | None = None,
        *,
        strict: bool = True,
    ) -> None:
        """Store a value under `key` with optional TTL (seconds) via the given driver."""
        self.driver(driver_name).put(key, value, ttl, strict=strict)

    def forever(
        self,
        key: str,
        value: Any,
        driver_name: str | None = None,
    ) -> None:
        """Store a value permanently (no expiration) via the given driver."""
        self.driver(driver_name).forever(key, value)

    def forget(self, key: str, driver_name: str | None = None) -> bool:
        """
        Remove a key from cache via the given driver.

        Returns True if deleted.
        """
        return self.driver(driver_name).forget(key)

    def pull(
        self,
        key: str,
        default: Any = None,
        driver_name: str | None = None,
    ) -> Any:
        """Atomically return and remove a value from the selected driver."""
        return self.driver(driver_name).pull(key, default)

    def forget_if(
        self, key: str, expected_value: Any, driver_name: str | None = None
    ) -> bool:
        """Atomically delete ``key`` only if its stored value equals
        ``expected_value`` (compare-and-delete).

        This is the owner-fenced release primitive for distributed locks: the
        ownership check and the delete happen as ONE step (Redis Lua / an
        equivalent CAS in every driver), so a lock whose TTL lapsed and was
        re-acquired by another owner can't be deleted by the previous holder.
        Exposed on the facade so callers that hand-roll an ``add``-based lock
        (scheduler overlap guard, WithoutOverlapping middleware) can release it
        safely without constructing a full :class:`CacheLock`. Returns True when
        this owner still held the key and it was deleted.
        """
        return self.driver(driver_name).forget_if(key, expected_value)

    def flush(self, driver_name: str | None = None) -> None:
        """Flush (clear) all entries from the given driver."""
        self.driver(driver_name).flush()

    def has(self, key: str, driver_name: str | None = None) -> bool:
        """Check if a key exists in cache via the given driver."""
        return self.driver(driver_name).has(key)

    def add(
        self,
        key: str,
        value: Any,
        ttl: int | None = None,
        driver_name: str | None = None,
    ) -> bool:
        """Add a value only if key doesn't exist via the given driver."""
        return self.driver(driver_name).add(key, value, ttl)

    def remember(
        self,
        key: str,
        ttl: int,
        callback,
        driver_name: str | None = None,
        *,
        stampede_lock_seconds: int = 30,
        strict: bool = True,
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

        The lock is part of the cache contract; callers cannot disable it.
        Invalid timeouts and drivers without the atomic ``add`` primitive are
        rejected instead of quietly reintroducing a thundering herd.

        ``strict=True`` is the authority default: backend/integrity failures
        propagate. A caller whose source of truth is the callback may choose
        ``strict=False``; an unavailable cache then executes the callback and
        returns its result without pretending anything was cached.
        """
        if (
            not isinstance(stampede_lock_seconds, int)
            or isinstance(stampede_lock_seconds, bool)
            or stampede_lock_seconds <= 0
        ):
            raise ValueError("stampede_lock_seconds must be a positive integer")
        driver = self.driver(driver_name)

        def compute_without_cache() -> Any:
            value = callback()
            try:
                driver.put(key, value, ttl, strict=False)
            except Exception:
                _logger.warning(
                    "disposable cache write failed for %s", key, exc_info=True
                )
            return value

        # Fast path — hit. No lock needed when we have a value already.
        _missing = object()
        cached = driver.get(key, _missing, strict=strict)
        if cached is not _missing:
            return cached

        if not callable(getattr(driver, "add", None)):
            raise RuntimeError("Cache driver does not support atomic add")

        # Try to claim the regen slot. ``add`` is atomic on every
        # driver in this codebase (Redis SET NX, file driver O_EXCL).
        lock_key = f"stampede:remember:{key}"
        try:
            won = driver.add(lock_key, "1", stampede_lock_seconds)
        except Exception:
            if strict:
                raise
            return compute_without_cache()

        if won:
            try:
                value = callback()
                driver.put(key, value, ttl, strict=strict)
                return value
            finally:
                # Release the regen slot; another future expiry
                # round will grab it again.
                try:
                    driver.forget(lock_key)
                except Exception:
                    _logger.debug("stampede lock cleanup failed", exc_info=True)

        # Lost the race — wait briefly for the winner to populate the
        # key, then re-read. The poll interval is short (50ms) but
        # capped at the lock's lifetime so we don't deadlock if the
        # winner crashes.
        #
        # The loop ALSO watches the lock state (not just the cached
        # value). Pre-fix it only checked the value; when the winner's
        # callback raised, ``finally`` released the lock but nothing
        # got cached, so every loser waited out the full deadline
        # then fell through to running the callback themselves — N
        # losers → N uncoordinated callback runs against the same
        # already-stressed downstream the winner just timed out
        # against (the very thundering herd this lock exists to
        # prevent). Detecting the empty-lock + empty-cache state lets
        # exactly ONE loser re-claim the slot via ``add`` and become
        # the secondary winner; the other losers see the lock taken
        # again and keep polling for the secondary winner's result.

        deadline = _time.time() + stampede_lock_seconds
        while _time.time() < deadline:
            cached = driver.get(key, _missing, strict=strict)
            if cached is not _missing:
                return cached
            # Lock state probe. ``add`` is the canonical atomic
            # primitive — using ``get`` then ``add`` here would race
            # in exactly the same window the initial claim above
            # races. Instead, optimistically attempt ``add`` whenever
            # the cache is still empty: ``add`` is a no-op + False
            # if the lock is already held, and only one caller wins
            # if multiple losers race the secondary claim
            # simultaneously.
            try:
                re_won = driver.add(lock_key, "1", stampede_lock_seconds)
            except Exception:
                if strict:
                    raise
                return compute_without_cache()
            if re_won:
                try:
                    value = callback()
                    driver.put(key, value, ttl, strict=strict)
                    return value
                finally:
                    try:
                        driver.forget(lock_key)
                    except Exception:
                        _logger.debug("stampede lock cleanup failed", exc_info=True)
            _time.sleep(0.05)

        # Winner crashed AND no loser could claim the secondary slot
        # before the deadline (rare: implies repeated crashes
        # outpacing every poll cycle). Run the callback ourselves
        # rather than return None — the caller's contract is "you'll
        # get the value or this raises".
        value = callback()
        driver.put(key, value, ttl, strict=strict)
        return value

    def remember_with_negative(
        self,
        key: str,
        hit_ttl: int,
        miss_ttl: int,
        callback: Callable[[], Any],
        driver_name: str | None = None,
        *,
        sentinel: Any = "",
        strict: bool = True,
    ) -> Any | None:
        """Like remember(), but stores a sentinel on cache-miss to prevent repeated computation.

        Returns None when the sentinel is found (indicating a previous miss).
        """
        driver = self.driver(driver_name)

        _missing = object()
        cached = driver.get(key, _missing, strict=strict)
        if cached is not _missing:
            return None if cached == sentinel else cached

        result = callback()
        if result is not None:
            driver.put(key, result, hit_ttl, strict=strict)
        else:
            driver.put(key, sentinel, miss_ttl, strict=strict)
        return result

    def forget_pattern(self, pattern: str, driver_name: str | None = None) -> int:
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
        self,
        prefix: str,
        driver_name: str | None = None,
    ) -> int:
        """
        Delete every key starting with ``prefix``.

        Convenience wrapper over ``forget_pattern`` — most callers only
        ever want a prefix sweep ("reports:daily:") and don't need to
        compose glob patterns themselves. Appends ``*`` so the driver
        sees a valid glob.
        """
        return self.driver(driver_name).forget_pattern(f"{prefix}*")

    def ttl(self, key: str, driver_name: str | None = None) -> int | None:
        """
        Remaining seconds-to-live for ``key``.

        Returns ``None`` when the key is missing or has no expiry, and
        a non-negative int otherwise. Useful for accurate ``Retry-After``
        headers on rate-limit responses, which previously reported the
        full window length regardless of when in the window the bucket
        filled.

        Every registered driver implements this contract. Backend failures
        propagate so security authorities cannot confuse an outage with a
        legitimately absent key.
        """
        return self.driver(driver_name).ttl(key)

    def increment(
        self,
        key: str,
        amount: int = 1,
        ttl: int | None = None,
        driver_name: str | None = None,
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
        ttl: int | None = None,
        driver_name: str | None = None,
    ) -> int:
        """
        Atomically decrement a counter at ``key`` by ``amount``.

        Implemented as ``increment(key, -amount)`` — drivers don't need
        a separate decrement primitive. Returning value can go negative
        if callers decrement past zero; bound-checking is the caller's
        responsibility.
        """
        return self.driver(driver_name).increment(key, -int(amount), ttl)

    def tags(self, *tags: str, driver_name: str | None = None) -> CacheTaggedStore:
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
        owner: str | None = None,
        driver_name: str | None = None,
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

    def exact_lock(
        self,
        key: str,
        timeout: int = 86400,
        owner: str | None = None,
        driver_name: str | None = None,
    ) -> CacheLock:
        """Get an owner-fenced lock on an already-canonical cache key.

        Use this only when a pre-existing coordination protocol defines the
        complete key and changing it would split old and new workers during a
        rolling deploy. Unlike :meth:`lock`, this method does not add the
        ``lock:`` namespace. Acquisition still stores a unique owner token and
        release still uses the driver's atomic ``forget_if`` fence.

        This is a separate API on purpose: callers cannot accidentally disable
        the prefix invariant of ordinary named locks with a boolean option.
        """
        if not isinstance(key, str) or not key:
            raise ValueError("Exact cache lock key must be a non-empty string.")
        driver = self.driver(driver_name)
        return CacheLock(driver, key, timeout, owner, exact_key=True)
