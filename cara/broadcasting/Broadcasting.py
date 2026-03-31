"""Broadcasting Manager for real-time messaging.

Laravel-style broadcasting manager that handles multiple drivers and provides
a clean API for WebSocket connections, channel subscriptions, and message broadcasting.
Similar to QueueManager and CacheManager patterns.
"""

from typing import Any, Callable, Dict, List, Optional, Union

from cara.exceptions import BroadcastingConfigurationException


class Broadcasting:
    """Main broadcasting manager - Laravel BroadcastManager style.

    Manages multiple broadcasting drivers and provides unified API for:
    - WebSocket connection management
    - Channel subscriptions
    - Event broadcasting
    - User-specific broadcasts
    """

    def __init__(self, application: Any, default_driver: str) -> None:
        """Initialize the broadcasting manager.

        Args:
            application: The application container
            default_driver: The default broadcast driver name
        """
        self.application = application
        self.default_driver = default_driver
        self._drivers: Dict[str, Any] = {}

    def driver(self, name: Optional[str] = None) -> Any:
        """Get a broadcasting driver instance.

        Args:
            name: The driver name (uses default if not provided)

        Returns:
            The broadcasting driver instance

        Raises:
            BroadcastingConfigurationException: If driver is not registered
        """
        name = name or self.default_driver

        # Return cached driver if exists
        if name in self._drivers:
            return self._drivers[name]

        # Driver not found
        raise BroadcastingConfigurationException(
            f"Broadcasting driver '{name}' is not registered."
        )

    def add_driver(self, name: str, driver_instance: Any) -> None:
        """Add a broadcasting driver instance.

        Args:
            name: The driver name
            driver_instance: The driver instance to register
        """
        self._drivers[name] = driver_instance

        # Register cleanup callback for graceful shutdown
        if hasattr(driver_instance, "cleanup"):
            if not hasattr(self.application, "_shutdown_callbacks"):
                self.application._shutdown_callbacks: List[Callable] = []
            if driver_instance.cleanup not in self.application._shutdown_callbacks:
                self.application._shutdown_callbacks.append(driver_instance.cleanup)

    async def broadcast(
        self,
        channels: Union[str, List[str]],
        event: str,
        data: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """Broadcast an event to channels.

        Args:
            channels: Channel name(s) to broadcast to
            event: The event name
            data: The event data to broadcast

        Returns:
            The driver's broadcast result
        """
        return await self.driver().broadcast(channels, event, data or {})

    async def add_connection(
        self,
        connection_id: str,
        websocket: Any,
        user_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """Add a WebSocket connection.

        Args:
            connection_id: Unique connection identifier
            websocket: The WebSocket instance
            user_id: Optional user ID for authentication
            metadata: Optional connection metadata

        Returns:
            The driver's result
        """
        return await self.driver().add_connection(
            connection_id, websocket, user_id, metadata
        )

    async def remove_connection(self, connection_id: str) -> Any:
        """Remove a WebSocket connection.

        Args:
            connection_id: The connection ID to remove

        Returns:
            The driver's result
        """
        return await self.driver().remove_connection(connection_id)

    async def subscribe(self, connection_id: str, channel: str) -> Any:
        """Subscribe a connection to a channel.

        Args:
            connection_id: The connection ID
            channel: The channel name

        Returns:
            The driver's result
        """
        return await self.driver().subscribe(connection_id, channel)

    async def unsubscribe(self, connection_id: str, channel: str) -> Any:
        """Unsubscribe a connection from a channel.

        Args:
            connection_id: The connection ID
            channel: The channel name

        Returns:
            The driver's result
        """
        return await self.driver().unsubscribe(connection_id, channel)

    def get_connection_count(self) -> int:
        """Get total number of active connections.

        Returns:
            The number of active connections
        """
        return self.driver().get_connection_count()

    def get_channel_subscribers(self, channel: str) -> List[str]:
        """Get list of connection IDs subscribed to a channel.

        Args:
            channel: The channel name

        Returns:
            List of connection IDs subscribed to the channel
        """
        return self.driver().get_channel_subscribers(channel)

    def get_stats(self) -> Dict[str, Any]:
        """Get broadcasting statistics.

        Returns:
            Dictionary of broadcasting statistics
        """
        return self.driver().get_stats()

    async def broadcast_event(self, event: Any) -> None:
        """Broadcast an event that implements ShouldBroadcast.

        Args:
            event: An event instance implementing ShouldBroadcast

        Raises:
            BroadcastingConfigurationException: If event doesn't implement ShouldBroadcast
        """
        from cara.broadcasting.contracts import ShouldBroadcast
        from cara.facades import Log

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
            f"Broadcasting event '{event_name}' to channels: {channels}",
            category="cara.broadcasting",
        )
        Log.debug(f"Event data: {data}", category="cara.broadcasting")
        Log.debug(
            f"Current driver: {self.driver().__class__.__name__}",
            category="cara.broadcasting",
        )

        try:
            await self.broadcast(channels, event_name, data)
        except Exception as e:
            Log.error(
                f"Broadcasting failed for event '{event_name}': {e}",
                category="cara.broadcasting",
            )

    async def broadcast_to_user(
        self, user_id: str, event: str, data: Dict[str, Any]
    ) -> Any:
        """Broadcast to a specific user.

        Args:
            user_id: The user ID to broadcast to
            event: The event name
            data: The event data

        Returns:
            The driver's result
        """
        return await self.driver().broadcast_to_user(user_id, event, data)
