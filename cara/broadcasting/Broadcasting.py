"""
Broadcasting Manager for real-time messaging.

Laravel-style broadcasting manager that handles multiple drivers and provides
a clean API for WebSocket connections, channel subscriptions, and message broadcasting.
Similar to QueueManager and CacheManager patterns.
"""

from typing import Any, Dict, List, Optional, Union

from cara.exceptions import BroadcastingConfigurationException
from cara.facades import Log


class Broadcasting:
    """
    Main broadcasting manager - Laravel BroadcastManager style.

    Manages multiple broadcasting drivers and provides unified API.
    """

    def __init__(self, application, default_driver: str):
        self.application = application
        self.default_driver = default_driver
        self._drivers: Dict[str, Any] = {}

    def driver(self, name: Optional[str] = None):
        """Get a broadcasting driver instance."""
        name = name or self.default_driver

        # Return cached driver if exists
        if name in self._drivers:
            return self._drivers[name]

        # Driver not found
        raise BroadcastingConfigurationException(
            f"Broadcasting driver '{name}' is not registered."
        )

    def add_driver(self, name: str, driver_instance: Any):
        """Add a broadcasting driver instance."""
        self._drivers[name] = driver_instance

        # Register cleanup callback for graceful shutdown
        if hasattr(driver_instance, "cleanup"):
            if not hasattr(self.application, "_shutdown_callbacks"):
                self.application._shutdown_callbacks = []
            if driver_instance.cleanup not in self.application._shutdown_callbacks:
                self.application._shutdown_callbacks.append(driver_instance.cleanup)

    # Delegate common methods to default driver
    async def broadcast(
        self, channels: Union[str, List[str]], event: str, data: Dict[str, Any] = None
    ):
        """Broadcast an event to channels."""
        return await self.driver().broadcast(channels, event, data or {})

    async def add_connection(
        self,
        connection_id: str,
        websocket,
        user_id: Optional[str] = None,
        metadata: Optional[Dict] = None,
    ):
        """Add a WebSocket connection."""
        return await self.driver().add_connection(
            connection_id, websocket, user_id, metadata
        )

    async def remove_connection(self, connection_id: str):
        """Remove a WebSocket connection."""
        return await self.driver().remove_connection(connection_id)

    async def subscribe(self, connection_id: str, channel: str):
        """Subscribe a connection to a channel."""
        return await self.driver().subscribe(connection_id, channel)

    async def unsubscribe(self, connection_id: str, channel: str):
        """Unsubscribe a connection from a channel."""
        return await self.driver().unsubscribe(connection_id, channel)

    def get_connection_count(self) -> int:
        """Get total number of active connections."""
        return self.driver().get_connection_count()

    def get_channel_subscribers(self, channel: str) -> List[str]:
        """Get list of connection IDs subscribed to a channel."""
        return self.driver().get_channel_subscribers(channel)

    def get_stats(self) -> Dict[str, Any]:
        """Get broadcasting statistics."""
        return self.driver().get_stats()

    # Event broadcasting methods - Laravel style
    async def broadcast_event(self, event):
        """Broadcast an event that implements ShouldBroadcast."""
        from cara.broadcasting.contracts import ShouldBroadcast

        if not isinstance(event, ShouldBroadcast):
            raise BroadcastingConfigurationException(
                "Event must implement ShouldBroadcast interface"
            )

        if not event.broadcast_when():
            Log.debug(
                f"Event {type(event).__name__} broadcast skipped by broadcast_when()"
            )
            return

        channels = event.broadcast_on()
        if isinstance(channels, str):
            channels = [channels]

        event_name = event.broadcast_as()
        data = event.broadcast_with()

        Log.debug(
            f"📡 Broadcasting event '{event_name}' to channels: {channels}",
            category="cara.broadcasting",
        )
        Log.debug(f"📡 Event data: {data}", category="cara.broadcasting")
        Log.debug(
            f"📡 Current driver: {self.driver().__class__.__name__}",
            category="cara.broadcasting",
        )

        try:
            await self.broadcast(channels, event_name, data)
        except Exception as e:
            Log.error(
                f"Broadcasting failed for event '{event_name}': {e}",
                category="cara.broadcasting",
            )
            # Don't re-raise the exception to prevent it from affecting the calling code
            # The broadcast failure should not break the application flow

    async def broadcast_to_user(self, user_id: str, event: str, data: Dict[str, Any]):
        """Broadcast to a specific user."""
        return await self.driver().broadcast_to_user(user_id, event, data)
