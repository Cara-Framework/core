"""
Broadcaster Interface.

Interface for broadcasting drivers - Laravel Broadcaster contract style.
All broadcasting drivers must implement this interface.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union


class Broadcaster(ABC):
    """Interface for broadcasting drivers."""

    @abstractmethod
    async def broadcast(
        self, channels: Union[str, List[str]], event: str, data: Dict[str, Any]
    ):
        """Broadcast an event to one or more channels."""
        pass

    @abstractmethod
    async def add_connection(
        self,
        connection_id: str,
        websocket,
        user_id: Optional[str] = None,
        metadata: Optional[Dict] = None,
    ):
        """Add a WebSocket connection."""
        pass

    @abstractmethod
    async def remove_connection(self, connection_id: str):
        """Remove a WebSocket connection."""
        pass

    @abstractmethod
    async def subscribe(self, connection_id: str, channel: str) -> bool:
        """Subscribe a connection to a channel."""
        pass

    @abstractmethod
    async def unsubscribe(self, connection_id: str, channel: str) -> bool:
        """Unsubscribe a connection from a channel."""
        pass

    @abstractmethod
    async def broadcast_to_user(self, user_id: str, event: str, data: Dict[str, Any]):
        """Broadcast to a specific user."""
        pass

    @abstractmethod
    def get_connection_count(self) -> int:
        """Get total number of active connections."""
        pass

    @abstractmethod
    def get_channel_subscribers(self, channel: str) -> List[str]:
        """Get list of connection IDs subscribed to a channel."""
        pass

    @abstractmethod
    def get_stats(self) -> Dict[str, Any]:
        """Get broadcasting statistics."""
        pass

    # Optional methods with default implementations
    async def authorize_channel(
        self, connection_id: str, channel: str, user_id: Optional[str] = None
    ) -> bool:
        """Authorize a connection to subscribe to a channel. Override for custom authorization."""
        return True

    async def cleanup(self):
        """Clean up resources. Override if cleanup is needed."""
        pass
