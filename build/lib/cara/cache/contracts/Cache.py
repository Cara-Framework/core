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
