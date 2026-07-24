"""
Log broadcasting driver — emits log lines instead of WebSocket frames.

Useful in dev / CI when you want to verify broadcast intent without
actually wiring up redis or browser clients.
"""

from __future__ import annotations

from typing import Any

from cara.broadcasting.contracts.Broadcaster import Broadcaster
from cara.facades import Log


class LogBroadcaster(Broadcaster):
    """No-op broadcaster that logs every operation."""

    driver_name = "log"

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config

    async def broadcast(
        self,
        channels: str | list[str],
        event: str,
        data: dict[str, Any],
        *,
        except_socket_id: str | None = None,
    ) -> None:
        if isinstance(channels, str):
            channels = [channels]
        Log.info(
            "📡 [log] Broadcasting '%s' to %s (except_socket_id=%s)",
            event,
            channels,
            except_socket_id or "-",
            category="cara.broadcasting",
        )
        Log.debug("📡 [log] Payload: %s", data, category="cara.broadcasting")

    async def add_connection(
        self,
        connection_id: str,
        websocket: Any,
        user_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        Log.info(
            "🔗 [log] Connection added: %s (user=%s)",
            connection_id,
            user_id or "-",
            category="cara.broadcasting",
        )

    async def remove_connection(self, connection_id: str) -> None:
        Log.info(
            "🔗 [log] Connection removed: %s", connection_id, category="cara.broadcasting"
        )

    async def subscribe(self, connection_id: str, channel: str) -> bool:
        Log.info(
            "📺 [log] %s subscribed to %s",
            connection_id,
            channel,
            category="cara.broadcasting",
        )
        return True

    async def unsubscribe(self, connection_id: str, channel: str) -> bool:
        Log.info(
            "📺 [log] %s unsubscribed from %s",
            connection_id,
            channel,
            category="cara.broadcasting",
        )
        return True

    async def broadcast_to_user(
        self,
        user_id: str,
        event: str,
        data: dict[str, Any],
        *,
        except_socket_id: str | None = None,
    ) -> None:
        Log.info(
            "👤 [log] Broadcasting '%s' to user %s",
            event,
            user_id,
            category="cara.broadcasting",
        )
        Log.debug("👤 [log] Payload: %s", data, category="cara.broadcasting")

    def get_connection_count(self) -> int:
        return 0

    def get_channel_subscribers(self, channel: str) -> list[str]:
        return []

    def get_stats(self) -> dict[str, Any]:
        return {
            "driver": "log",
            "connections": 0,
            "channels": 0,
            "description": "Log driver — events are logged instead of broadcast",
        }
