"""
Defines the core contract for caching drivers in the Cara framework.

Any cache driver (file, redis, etc.) must implement these methods. This ensures consistent behavior
(get, put, forever, forget, flush) across drivers.
"""

from __future__ import annotations

from typing import Any

from cara.exceptions import CacheConfigurationException


class CacheContract:
    """
    A simple contract for caching operations.

    Methods:
    - get(key, default=None)
    - put(key, value, ttl=None)
    - forever(key, value)
    - forget(key)
    - flush()
    - has(key)
    - add(key, value, ttl=None)
    """

    def get(self, key: str, default: Any = None, *, strict: bool = True) -> Any:
        """Read ``key``.

        Backend and integrity failures raise by default. Callers using cache
        solely as a disposable acceleration layer must opt into
        ``strict=False`` explicitly; authority state must never confuse an
        outage with a cache miss.
        """
        raise NotImplementedError

    def put(
        self,
        key: str,
        value: Any,
        ttl: int | None = None,
        *,
        strict: bool = True,
    ) -> None:
        raise NotImplementedError

    @staticmethod
    def _resolve_ttl(ttl: int | None, default_ttl: int) -> int:
        """Return an exact cache TTL, rejecting coercive input.

        Zero deliberately means no expiry. Negative, boolean, float and text
        values are configuration errors rather than alternate spellings.
        """
        value = default_ttl if ttl is None else ttl
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise CacheConfigurationException(
                "Cache TTL must be a non-negative integer number of seconds"
            )
        return value

    def forever(self, key: str, value: Any) -> None:
        raise NotImplementedError

    def forget(self, key: str) -> bool:
        raise NotImplementedError

    def pull(self, key: str, default: Any = None) -> Any:
        """Atomically return and delete ``key``.

        Security-sensitive one-time handles (OAuth state, login challenges,
        passwordless links) must not use a racy ``get`` followed by ``forget``.
        """
        raise NotImplementedError

    def flush(self) -> None:
        raise NotImplementedError

    def has(self, key: str) -> bool:
        """Check if a key exists in cache."""
        raise NotImplementedError

    def add(
        self,
        key: str,
        value: Any,
        ttl: int | None = None,
    ) -> bool:
        """Add a value only if key doesn't exist. Returns True if added."""
        raise NotImplementedError

    def forget_pattern(self, pattern: str) -> int:
        """
        Delete multiple keys matching a pattern.

        For Redis: Uses SCAN + DEL with glob pattern matching.
        For File: Uses glob matching on cache files.

        Args:
            pattern: Glob-style pattern (e.g., "home:*", "products:featured:*")

        Returns:
            Number of keys deleted
        """
        raise NotImplementedError

    def increment(self, key: str, amount: int = 1, ttl: int | None = None) -> int:
        """Atomically increment a key by ``amount``. Returns the new value.

        If the key does not exist it is initialised to 0 before incrementing.
        Callers must provide a positive ``ttl``; permanent authority counters
        are forbidden. Subsequent increments preserve the original window.
        Every driver guarantees atomicity across its supported concurrency
        boundary (Redis server-side, file cache through a process file lock).
        """
        raise NotImplementedError

    def ttl(self, key: str) -> int | None:
        """Return the key's remaining expiry, or ``None`` when absent."""
        raise NotImplementedError

    def forget_if(self, key: str, expected_value: Any) -> bool:
        """
        Atomically delete ``key`` only if its current value equals
        ``expected_value``. Returns True iff the delete actually happened.

        This is the primitive used by ``CacheLock.release`` to avoid the
        classic "lock A's TTL expires, lock B acquires, lock A's release
        deletes B's key" race. A non-atomic ``get -> forget`` cannot
        distinguish those two cases.

        Every driver MUST guarantee atomicity; a non-atomic implementation is
        not a cache-lock implementation.
        """
        raise NotImplementedError
