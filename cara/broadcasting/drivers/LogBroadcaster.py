"""
Log broadcasting driver — emits log lines instead of WebSocket frames.

Useful in dev / CI when you want to verify broadcast intent without
actually wiring up redis or browser clients.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from cara.broadcasting.contracts.Broadcaster import Broadcaster
from cara.facades import Log


class LogBroadcaster(Broadcaster):
    """No-op broadcaster that logs every operation."""

    driver_name = "log"

    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config

    async def broadcast(
        self,
        channels: Union[str, List[str]],
        event: str,
        data: Dict[str, Any],
        *,
        except_socket_id: Optional[str] = None,
    ) -> None:
        if isinstance(channels, str):
            channels = [channels]
        Log.info(
            f"📡 [log] Broadcasting '{event}' to {channels} "
            f"(except_socket_id={except_socket_id or '-'})",
            category="cara.broadcasting",
        )
        Log.debug(f"📡 [log] Payload: {data}", category="cara.broadcasting")

    async def add_connection(
        self,
        connection_id: str,
        websocket: Any,
        user_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        Log.info(
            f"🔗 [log] Connection added: {connection_id} (user={user_id or '-'})",
            category="cara.broadcasting",
        )

    async def remove_connection(self, connection_id: str) -> None:
        Log.info(
            f"🔗 [log] Connection removed: {connection_id}",
            category="cara.broadcasting",
        )

    async def subscribe(self, connection_id: str, channel: str) -> bool:
        Log.info(
            f"📺 [log] {connection_id} subscribed to {channel}",
            category="cara.broadcasting",
        )
        return True

    async def unsubscribe(self, connection_id: str, channel: str) -> bool:
        Log.info(
            f"📺 [log] {connection_id} unsubscribed from {channel}",
            category="cara.broadcasting",
        )
        return True

    async def broadcast_to_user(
        self,
        user_id: str,
        event: str,
        data: Dict[str, Any],
        *,
        except_socket_id: Optional[str] = None,
    ) -> None:
        Log.info(
            f"👤 [log] Broadcasting '{event}' to user {user_id}",
            category="cara.broadcasting",
        )
        Log.debug(f"👤 [log] Payload: {data}", category="cara.broadcasting")

    def get_connection_count(self) -> int:
        return 0

    def get_channel_subscribers(self, channel: str) -> List[str]:
        return []

    def get_stats(self) -> Dict[str, Any]:
        return {
            "driver": "log",
            "connections": 0,
            "channels": 0,
            "description": "Log driver — events are logged instead of broadcast",
        }
