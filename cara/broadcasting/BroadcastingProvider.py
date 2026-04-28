"""
Broadcasting service provider — registers the manager + drivers and
loads the application's channel auth callbacks.

Channel auth callbacks live in ``routes/broadcasting.py`` of the
host application::

    # routes/broadcasting.py
    from cara.facades import Broadcast

    @Broadcast.channel("user.{user_id}.alerts")
    async def authorize_user_alerts(user, user_id: str) -> bool:
        return user is not None and str(user.id) == str(user_id)

The provider imports that module on registration, mirroring how
Laravel's ``BroadcastServiceProvider`` invokes
``Broadcast::routes()`` and ``require base_path('routes/channels.php')``.
Loading is best-effort — apps with no broadcasting routes module
boot fine.
"""

from __future__ import annotations

import importlib

from cara.broadcasting import Broadcasting
from cara.broadcasting.drivers import (
    LogBroadcaster,
    MemoryBroadcaster,
    NullBroadcaster,
    RedisBroadcaster,
)
from cara.configuration import config
from cara.exceptions import BroadcastingConfigurationException
from cara.facades import Log
from cara.foundation import DeferredProvider


class BroadcastingProvider(DeferredProvider):
    """Bind the broadcasting manager and its drivers."""

    @classmethod
    def provides(cls) -> list[str]:
        return ["broadcasting"]

    def register(self) -> None:
        default_driver = config("broadcasting.default")
        if not default_driver:
            raise BroadcastingConfigurationException(
                "Broadcasting default driver must be specified in configuration."
            )

        manager = Broadcasting(self.application, default_driver)

        self._add_redis_driver(manager)
        self._add_memory_driver(manager)
        self._add_log_driver(manager)
        self._add_null_driver(manager)

        self.application.bind("broadcasting", manager)

        # Load app-level channel auth callbacks. Best-effort; a missing
        # module is normal for apps that haven't migrated yet.
        try:
            importlib.import_module("routes.broadcasting")
            Log.debug(
                "Loaded routes.broadcasting (channel auth callbacks)",
                category="cara.broadcasting",
            )
        except ModuleNotFoundError:
            pass
        except Exception as e:
            # Real syntax/import error — surface it so apps don't ship
            # broken auth silently.
            Log.error(
                f"Failed to import routes.broadcasting: {e}",
                category="cara.broadcasting",
                exc_info=True,
            )

    # ------------------------------------------------------------------
    # Driver registration helpers
    # ------------------------------------------------------------------
    def _add_redis_driver(self, manager: Broadcasting) -> None:
        if not config("broadcasting.drivers.redis"):
            return

        host = config("broadcasting.drivers.redis.connection.host", "localhost")
        port = config("broadcasting.drivers.redis.connection.port", 6379)
        password = config("broadcasting.drivers.redis.connection.password", "")
        db = config("broadcasting.drivers.redis.connection.db", 0)

        if password:
            redis_url = f"redis://:{password}@{host}:{port}/{db}"
        else:
            redis_url = f"redis://{host}:{port}/{db}"

        driver_config = config("broadcasting.drivers.redis", {})
        if isinstance(driver_config, dict):
            driver_config = dict(driver_config)
        else:
            driver_config = {}

        ws_cfg = config("broadcasting.WEBSOCKET")
        if ws_cfg:
            driver_config["websocket"] = ws_cfg

        manager.add_driver(
            RedisBroadcaster.driver_name,
            RedisBroadcaster(driver_config, redis_url=redis_url),
        )

    def _add_memory_driver(self, manager: Broadcasting) -> None:
        settings = config("broadcasting.drivers.memory")
        if not settings:
            return
        driver_config = dict(settings) if isinstance(settings, dict) else {}
        ws_cfg = config("broadcasting.WEBSOCKET")
        if ws_cfg:
            driver_config["websocket"] = ws_cfg
        manager.add_driver(MemoryBroadcaster.driver_name, MemoryBroadcaster(driver_config))

    def _add_log_driver(self, manager: Broadcasting) -> None:
        settings = config("broadcasting.drivers.log")
        if not settings:
            return
        manager.add_driver(LogBroadcaster.driver_name, LogBroadcaster(settings))

    def _add_null_driver(self, manager: Broadcasting) -> None:
        settings = config("broadcasting.drivers.null")
        if not settings:
            return
        manager.add_driver(NullBroadcaster.driver_name, NullBroadcaster(settings))
