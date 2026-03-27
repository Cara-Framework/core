"""
Defines the core contract for caching drivers in the Cara framework.

Any cache driver (file, redis, etc.) must implement these methods. This ensures consistent behavior
(get, put, forever, forget, flush) across drivers.
"""

from typing import Any, Optional


class Cache:
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

    def get(self, key: str, default: Any = None) -> Any:
        raise NotImplementedError

    def put(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
    ) -> None:
        raise NotImplementedError

    def forever(self, key: str, value: Any) -> None:
        raise NotImplementedError

    def forget(self, key: str) -> bool:
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
        ttl: Optional[int] = None,
    ) -> bool:
        """Add a value only if key doesn't exist. Returns True if added."""
        raise NotImplementedError

    def remember(
        self,
        key: str,
        ttl: int,
        callback,
    ) -> Any:
        """
        Get value from cache or execute callback and cache the result.

        Args:
            key: Cache key
            ttl: Time to live in seconds
            callback: Callable that returns the value if not cached

        Returns:
            Cached value or result of callback
        """
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
