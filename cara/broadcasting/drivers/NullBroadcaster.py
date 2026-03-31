"""
Null Broadcasting Driver.

Does nothing - useful for disabling broadcasting.
Implements Laravel-style Broadcaster interface.
"""

from typing import Any, Dict, List, Optional, Union

from cara.broadcasting.contracts.Broadcaster import Broadcaster


class NullBroadcaster(Broadcaster):
    """Null broadcasting driver - does nothing."""

    driver_name = "null"

    def __init__(self, config: Dict[str, Any]):
        self.config = config

    # Broadcaster interface implementation - all no-ops
    async def broadcast(
        self, channels: Union[str, List[str]], event: str, data: Dict[str, Any]
    ):
        """Do nothing."""
        pass

    async def add_connection(
        self,
        connection_id: str,
        websocket,
        user_id: Optional[str] = None,
        metadata: Optional[Dict] = None,
    ):
        """Do nothing."""
        pass

    async def remove_connection(self, connection_id: str):
        """Do nothing."""
        pass

    async def subscribe(self, connection_id: str, channel: str) -> bool:
        """Do nothing."""
        return True

    async def unsubscribe(self, connection_id: str, channel: str) -> bool:
        """Do nothing."""
        return True

    async def broadcast_to_user(self, user_id: str, event: str, data: Dict[str, Any]):
        """Do nothing."""
        pass

    def get_connection_count(self) -> int:
        """Null driver has no connections."""
        return 0

    def get_channel_subscribers(self, channel: str) -> List[str]:
        """Null driver has no subscribers."""
        return []

    def get_stats(self) -> Dict[str, Any]:
        """Get null broadcasting statistics."""
        return {
            "driver": "null",
            "connections": 0,
            "channels": 0,
            "description": "Null driver - all operations are no-ops",
        }
