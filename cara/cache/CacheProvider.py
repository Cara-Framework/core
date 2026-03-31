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
        cache_config = config("cache", {})

        # Check if cache_config is dict-like (supports .get() method)
        # Cara uses Dotty wrapper which doesn't pass isinstance(obj, dict) but has dict methods
        if not hasattr(cache_config, "get") or cache_config is None:
            raise CacheConfigurationException(
                "Cache configuration must be a dictionary-like object."
            )

        default_driver = cache_config.get("default")
        if not default_driver:
            raise CacheConfigurationException(
                "Cache default driver must be specified in configuration."
            )

        cache_manager = Cache(self.application, default_driver)

        # Register cache drivers
        self._add_file_driver(cache_manager, cache_config)
        self._add_redis_driver(cache_manager, cache_config)

        self.application.bind("cache", cache_manager)

    def _add_file_driver(self, cache_manager: Cache, settings) -> None:
        """Register file cache driver with configuration."""
        file_settings = settings.get("drivers", {}).get("file", None)
        if not file_settings:
            raise CacheConfigurationException(
                "Missing or invalid 'cache.drivers.file' config."
            )

        raw_path = file_settings.get("path")
        if not raw_path or not isinstance(raw_path, str):
            raise CacheConfigurationException(
                "'cache.drivers.file.path' must be a non-empty string."
            )

        full_path = paths("base", raw_path)
        prefix = file_settings.get("prefix", "")
        ttl = file_settings.get("ttl", 60)

        driver = FileCacheDriver(
            cache_directory=full_path,
            prefix=prefix,
            default_ttl=ttl,
        )
        cache_manager.add_driver(FileCacheDriver.driver_name, driver)

    def _add_redis_driver(self, cache_manager: Cache, settings) -> None:
        """Register Redis cache driver with configuration."""
        redis_settings = settings.get("drivers", {}).get("redis", None)
        if not redis_settings:
            return  # Redis is optional

        host = redis_settings.get("host", "127.0.0.1")
        port = redis_settings.get("port", 6379)
        db = redis_settings.get("db", 0)
        password = redis_settings.get("password")
        prefix = redis_settings.get("prefix", "")
        ttl = redis_settings.get("ttl", 60)

        driver = RedisCacheDriver(
            host=host,
            port=port,
            db=db,
            password=password,
            prefix=prefix,
            default_ttl=ttl,
        )
        cache_manager.add_driver(RedisCacheDriver.driver_name, driver)
