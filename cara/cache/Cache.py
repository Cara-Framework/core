"""
Central Cache Manager for the Cara framework.

This module provides the Cache class, which manages multiple cache drivers and delegates cache
operations to the appropriate driver instance.
"""

from typing import Any, Optional

from cara.cache.contracts import Cache
from cara.exceptions import DriverNotRegisteredException


class Cache:
    """
    Central cache manager. Delegates get/put/forget/flush to registered driver instances.

    The default driver name is injected via constructor (from CacheProvider).
    """

    def __init__(self, application, default_driver: str):
        self.application = application
        self._stores: dict[str, Cache] = {}
        self._default_driver: str = default_driver

    def add_driver(self, driver_name: str, driver: Cache) -> None:
        """Register a driver instance under `driver_name`."""
        self._stores[driver_name] = driver

    def driver(self, name: str = None) -> Cache:
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
