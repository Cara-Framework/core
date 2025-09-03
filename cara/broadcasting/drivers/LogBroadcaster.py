"""
Log Broadcasting Driver.

Logs broadcasting events instead of actually broadcasting them.
Useful for development and testing. Implements Laravel-style Broadcaster interface.
"""

from typing import Any, Dict, List, Optional, Union

from cara.broadcasting.contracts.Broadcaster import Broadcaster
from cara.facades import Log


class LogBroadcaster(Broadcaster):
    """Log broadcasting driver - logs events instead of broadcasting."""

    driver_name = "log"

    def __init__(self, config: Dict[str, Any]):
        self.config = config

    # Broadcaster interface implementation
    async def broadcast(
        self, channels: Union[str, List[str]], event: str, data: Dict[str, Any]
    ):
        """Log broadcast event instead of actually broadcasting."""
        if isinstance(channels, str):
            channels = [channels]

        Log.info(
            f"📡 Broadcasting '{event}' to channels: {channels}",
            category="cara.broadcasting",
        )
        Log.debug(f"📡 Event data: {data}", category="cara.broadcasting")

    async def add_connection(
        self,
        connection_id: str,
        websocket,
        user_id: Optional[str] = None,
        metadata: Optional[Dict] = None,
    ):
        """Log connection addition."""
        Log.info(
            f"🔗 Log Broadcasting: Connection {connection_id} added (user: {user_id})",
            category="cara.broadcasting",
        )

    async def remove_connection(self, connection_id: str):
        """Log connection removal."""
        Log.info(
            f"🔗 Log Broadcasting: Connection {connection_id} removed",
            category="cara.broadcasting",
        )

    async def subscribe(self, connection_id: str, channel: str) -> bool:
        """Log subscription."""
        Log.info(
            f"📺 Log Broadcasting: {connection_id} subscribed to {channel}",
            category="cara.broadcasting",
        )
        return True

    async def unsubscribe(self, connection_id: str, channel: str) -> bool:
        """Log unsubscription."""
        Log.info(
            f"📺 Log Broadcasting: {connection_id} unsubscribed from {channel}",
            category="cara.broadcasting",
        )
        return True

    async def broadcast_to_user(self, user_id: str, event: str, data: Dict[str, Any]):
        """Log user-specific broadcast."""
        Log.info(
            f"👤 Broadcasting '{event}' to user: {user_id}", category="cara.broadcasting"
        )
        Log.debug(f"👤 User event data: {data}", category="cara.broadcasting")

    def get_connection_count(self) -> int:
        """Log driver has no real connections."""
        return 0

    def get_channel_subscribers(self, channel: str) -> List[str]:
        """Log driver has no real subscribers."""
        return []

    def get_stats(self) -> Dict[str, Any]:
        """Get log broadcasting statistics."""
        return {
            "driver": "log",
            "connections": 0,
            "channels": 0,
            "description": "Log driver - events are logged instead of broadcast",
        }
