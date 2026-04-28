"""
Cache Provider for the Cara framework.

This module provides the deferred service provider that configures and registers the cache
subsystem, including file and Redis cache drivers.
"""

from cara.cache import Cache
from cara.cache.drivers import FileCacheDriver, RedisCacheDriver
from cara.configuration import config
from cara.exceptions import CacheConfigurationException
from cara.foundation import DeferredProvider
from cara.support import paths


class CacheProvider(DeferredProvider):
    """
    Deferred provider for the cache subsystem.

    Reads configuration and registers the Cache manager and its drivers.
    """

    @classmethod
    def provides(cls) -> list[str]:
        return ["cache"]

    def register(self) -> None:
        """Register cache drivers based on configuration."""
        default_driver = config("cache.default")
        if not default_driver:
            raise CacheConfigurationException(
                "Cache default driver must be specified in configuration."
            )

        cache_manager = Cache(self.application, default_driver)

        self._add_file_driver(cache_manager)
        self._add_redis_driver(cache_manager)

        self.application.bind("cache", cache_manager)

    def _add_file_driver(self, cache_manager: Cache) -> None:
        """Register file cache driver with configuration."""
        raw_path = config("cache.drivers.file.path")
        if not raw_path or not isinstance(raw_path, str):
            raise CacheConfigurationException(
                "'cache.drivers.file.path' must be a non-empty string."
            )

        driver = FileCacheDriver(
            cache_directory=paths("base", raw_path),
            prefix=config("cache.drivers.file.prefix", ""),
            default_ttl=config("cache.drivers.file.ttl", 60),
        )
        cache_manager.add_driver(FileCacheDriver.driver_name, driver)

    def _add_redis_driver(self, cache_manager: Cache) -> None:
        """Register Redis cache driver with configuration."""
        if not config("cache.drivers.redis"):
            return

        driver = RedisCacheDriver(
            host=config("cache.drivers.redis.host", "127.0.0.1"),
            port=config("cache.drivers.redis.port", 6379),
            db=config("cache.drivers.redis.db", 0),
            password=config("cache.drivers.redis.password"),
            prefix=config("cache.drivers.redis.prefix", ""),
            default_ttl=config("cache.drivers.redis.ttl", 60),
        )
        cache_manager.add_driver(RedisCacheDriver.driver_name, driver)
