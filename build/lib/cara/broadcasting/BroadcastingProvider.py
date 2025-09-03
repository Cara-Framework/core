"""
Broadcasting Provider for the Cara framework.

This module provides the deferred service provider that configures and registers the broadcasting
subsystem, including Redis, memory, log, and null broadcasting drivers.
"""

from cara.broadcasting import Broadcasting
from cara.broadcasting.drivers import (
    LogBroadcaster,
    MemoryBroadcaster,
    NullBroadcaster,
    RedisBroadcaster,
)
from cara.configuration import config
from cara.exceptions import BroadcastingConfigurationException
from cara.foundation import DeferredProvider


class BroadcastingProvider(DeferredProvider):
    """
    Deferred provider for the broadcasting subsystem.

    Reads configuration and registers the Broadcasting manager and its drivers.
    """

    @classmethod
    def provides(cls) -> list[str]:
        return ["broadcasting"]

    def register(self) -> None:
        """Register broadcasting drivers based on configuration."""
        broadcasting_config = config("broadcasting", {})

        # Check if broadcasting_config is dict-like
        if not hasattr(broadcasting_config, "get") or broadcasting_config is None:
            raise BroadcastingConfigurationException(
                "Broadcasting configuration must be a dictionary-like object."
            )

        default_driver = broadcasting_config.get("default")
        if not default_driver:
            raise BroadcastingConfigurationException(
                "Broadcasting default driver must be specified in configuration."
            )

        # Diagnostics: log selected default driver
        try:
            from cara.facades import Log

            Log.debug(
                f"BroadcastingProvider: default driver = {default_driver}",
                category="cara.broadcasting",
            )
        except Exception:
            pass

        broadcasting_manager = Broadcasting(self.application, default_driver)

        # Register broadcasting drivers
        self._add_redis_driver(broadcasting_manager, broadcasting_config)
        self._add_memory_driver(broadcasting_manager, broadcasting_config)
        self._add_log_driver(broadcasting_manager, broadcasting_config)
        self._add_null_driver(broadcasting_manager, broadcasting_config)

        self.application.bind("broadcasting", broadcasting_manager)

    def _add_redis_driver(self, manager: Broadcasting, settings) -> None:
        """Register Redis broadcasting driver with configuration."""
        redis_settings = settings.get("drivers", {}).get("redis", None)
        if not redis_settings:
            return  # Redis is optional

        # Get Redis URL from queue config to avoid duplication
        queue_config = config("queue", {})
        queue_redis_config = queue_config.get("drivers", {}).get("redis", {})

        host = queue_redis_config.get("host", "localhost")
        port = queue_redis_config.get("port", 6379)
        password = queue_redis_config.get("password", "")
        db = queue_redis_config.get("db", 0)

        if password:
            redis_url = f"redis://:{password}@{host}:{port}/{db}"
        else:
            redis_url = f"redis://{host}:{port}/{db}"

        # Include websocket configuration in the driver config
        driver_config = redis_settings.copy()
        if "WEBSOCKET" in settings:
            driver_config["websocket"] = settings["WEBSOCKET"]

        # Diagnostics: log Redis broadcaster settings
        try:
            from cara.facades import Log

            ws_cfg = (
                driver_config.get("websocket", {})
                if isinstance(driver_config, dict)
                else {}
            )
            Log.debug(
                f"BroadcastingProvider: registering RedisBroadcaster redis_url={redis_url} ws_path={ws_cfg.get('path')} ws_port={ws_cfg.get('port')}",
                category="cara.broadcasting",
            )
        except Exception:
            pass

        # Create and register driver instance
        driver = RedisBroadcaster(driver_config, redis_url=redis_url)
        manager.add_driver(RedisBroadcaster.driver_name, driver)

    def _add_memory_driver(self, manager: Broadcasting, settings) -> None:
        """Register memory broadcasting driver with configuration."""
        memory_settings = settings.get("drivers", {}).get("memory", None)
        if not memory_settings:
            return  # Memory is optional

        # Create and register driver instance
        driver = MemoryBroadcaster(memory_settings)
        manager.add_driver(MemoryBroadcaster.driver_name, driver)

    def _add_log_driver(self, manager: Broadcasting, settings) -> None:
        """Register log broadcasting driver with configuration."""
        log_settings = settings.get("drivers", {}).get("log", None)
        if not log_settings:
            return  # Log is optional

        # Create and register driver instance
        driver = LogBroadcaster(log_settings)
        manager.add_driver(LogBroadcaster.driver_name, driver)

    def _add_null_driver(self, manager: Broadcasting, settings) -> None:
        """Register null broadcasting driver with configuration."""
        null_settings = settings.get("drivers", {}).get("null", None)
        if not null_settings:
            return  # Null is optional

        # Create and register driver instance
        driver = NullBroadcaster(null_settings)
        manager.add_driver(NullBroadcaster.driver_name, driver)
