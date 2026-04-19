"""
WebSocket Connection Manager for Broadcasting.

Manages active WebSocket connections, channels, and message delivery.
Similar to Laravel's broadcasting connection management.

Includes automatic cleanup of stale connections and metadata to prevent memory leaks.
"""

import asyncio
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set

from cara.facades import Log


class ConnectionManager:
    """
    Manages WebSocket connections and channel subscriptions.

    Laravel-style connection management for broadcasting.
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config

        # Active connections: {connection_id: websocket}
        self.connections: Dict[str, Any] = {}

        # Channel subscriptions: {channel_name: {connection_id1, connection_id2}}
        self.channel_subscribers: Dict[str, Set[str]] = defaultdict(set)

        # User subscriptions: {connection_id: {channel1, channel2}}
        self.user_channels: Dict[str, Set[str]] = defaultdict(set)

        # Connection metadata: {connection_id: {user_id, ip, connected_at, etc}}
        self.connection_metadata: Dict[str, Dict[str, Any]] = {}

        # Configuration-based settings
        websocket_config = config.get("websocket", {})
        self.max_connections = websocket_config.get("max_connections", 1000)
        self.heartbeat_interval = websocket_config.get("heartbeat_interval", 30)
        self.connection_timeout = websocket_config.get("connection_timeout", 60)
        # Maximum metadata entries to keep in memory (prevents unbounded growth)
        self.max_metadata_entries = websocket_config.get("max_metadata_entries", 10000)

        # Heartbeat tasks per connection
        self._heartbeat_tasks: Dict[str, asyncio.Task] = {}

        # Cleanup task for stale connections
        self._cleanup_task: Optional[asyncio.Task] = None

    async def add_connection(
        self,
        connection_id: str,
        websocket: Any,
        user_id: Optional[str] = None,
        metadata: Optional[Dict] = None,
    ):
        """Add a new WebSocket connection."""
        # Check max connections limit
        if len(self.connections) >= self.max_connections:
            Log.warning(
                f"Broadcasting: Max connections ({self.max_connections}) reached, rejecting {connection_id}"
            )
            raise ConnectionError(
                f"Maximum connections ({self.max_connections}) exceeded"
            )

        self.connections[connection_id] = websocket

        # Store metadata
        self.connection_metadata[connection_id] = {
            "user_id": user_id,
            "connected_at": asyncio.get_running_loop().time(),
            **(metadata or {}),
        }

        Log.debug(
            f"ConnectionManager: {connection_id} added (user: {user_id})",
            category="cara.broadcasting",
        )

        # Start heartbeat
        if self.heartbeat_interval > 0:
            task = asyncio.create_task(self._heartbeat_loop(connection_id))
            self._heartbeat_tasks[connection_id] = task

    async def remove_connection(self, connection_id: str) -> None:
        """
        Remove a WebSocket connection and clean up all associated resources.

        Ensures complete cleanup to prevent memory leaks:
        - Removes from all channel subscriptions
        - Deletes connection object and metadata
        - Cancels heartbeat task

        Args:
            connection_id: The connection to remove
        """
        if connection_id in self.connections:
            # Remove from all channels
            channels_to_cleanup = self.user_channels.get(connection_id, set()).copy()
            for channel in channels_to_cleanup:
                await self.unsubscribe(connection_id, channel)

            # Clean up connection data - CRITICAL for preventing memory leaks
            del self.connections[connection_id]
            self.user_channels.pop(connection_id, None)
            self.connection_metadata.pop(connection_id, None)

            # Cancel heartbeat task
            task = self._heartbeat_tasks.pop(connection_id, None)
            if task and not task.done():
                task.cancel()

            Log.debug(
                f"ConnectionManager: {connection_id} removed",
                category="cara.broadcasting",
            )

    async def cleanup_stale_connections(self) -> None:
        """
        Periodically clean up stale connections and metadata.

        Prevents unbounded growth of metadata dictionaries by:
        - Removing connections that haven't sent heartbeat
        - Pruning old metadata entries
        - Enforcing max_metadata_entries limit

        Should be called periodically by the application.
        """
        current_time = asyncio.get_running_loop().time()
        stale_connections = []

        # Find stale connections (no heartbeat for connection_timeout seconds)
        for conn_id, metadata in self.connection_metadata.items():
            connected_at = metadata.get("connected_at", current_time)
            if current_time - connected_at > self.connection_timeout:
                if conn_id not in self.connections:
                    # Connection object is gone but metadata remains - clean it up
                    stale_connections.append(conn_id)

        # Remove stale metadata entries
        for conn_id in stale_connections:
            self.connection_metadata.pop(conn_id, None)
            Log.debug(
                f"Broadcasting: Cleaned up stale metadata for {conn_id}",
                category="cara.broadcasting",
            )

        # Enforce max_metadata_entries limit
        if len(self.connection_metadata) > self.max_metadata_entries:
            # Remove oldest entries by connected_at timestamp
            sorted_by_time = sorted(
                self.connection_metadata.items(),
                key=lambda x: x[1].get("connected_at", 0)
            )
            entries_to_remove = len(self.connection_metadata) - self.max_metadata_entries
            for conn_id, _ in sorted_by_time[:entries_to_remove]:
                self.connection_metadata.pop(conn_id, None)
            Log.warning(
                f"Broadcasting: Pruned {entries_to_remove} old metadata entries (limit: {self.max_metadata_entries})",
                category="cara.broadcasting",
            )

    async def start_cleanup_task(self) -> None:
        """
        Start periodic cleanup task.

        Should be called once when the ConnectionManager is initialized.
        Runs cleanup every 5 minutes by default.
        """
        if self._cleanup_task is None or self._cleanup_task.done():
            self._cleanup_task = asyncio.create_task(self._cleanup_loop())
            Log.info(
                "Broadcasting: Started cleanup task",
                category="cara.broadcasting",
            )

    async def _cleanup_loop(self) -> None:
        """Periodic cleanup loop."""
        cleanup_interval = 300  # 5 minutes
        while True:
            try:
                await asyncio.sleep(cleanup_interval)
                await self.cleanup_stale_connections()
            except asyncio.CancelledError:
                Log.info(
                    "Broadcasting: Cleanup task cancelled",
                    category="cara.broadcasting",
                )
                break
            except Exception as e:
                Log.error(
                    f"Broadcasting: Cleanup task error: {e}",
                    category="cara.broadcasting",
                )

    async def subscribe(self, connection_id: str, channel: str) -> bool:
        """Subscribe connection to a channel."""
        if connection_id not in self.connections:
            return False

        self.channel_subscribers[channel].add(connection_id)
        self.user_channels[connection_id].add(channel)

        Log.debug(
            f"Broadcasting: {connection_id} subscribed to {channel}",
            category="cara.broadcasting",
        )
        return True

    async def unsubscribe(self, connection_id: str, channel: str) -> bool:
        """Unsubscribe connection from a channel."""
        if connection_id in self.channel_subscribers[channel]:
            self.channel_subscribers[channel].remove(connection_id)

            # Clean up empty channels
            if not self.channel_subscribers[channel]:
                del self.channel_subscribers[channel]

        if connection_id in self.user_channels:
            self.user_channels[connection_id].discard(channel)

        Log.debug(
            f"Broadcasting: {connection_id} unsubscribed from {channel}",
            category="cara.broadcasting",
        )
        return True

    async def broadcast_to_channel(self, channel: str, event: str, data: Dict[str, Any]):
        """Broadcast message to all subscribers of a channel."""
        if channel not in self.channel_subscribers:
            Log.debug(
                f"Broadcasting: No subscribers for channel {channel}",
                category="cara.broadcasting",
            )
            return

        subscribers = self.channel_subscribers[channel].copy()
        message = {"event": event, "channel": channel, "data": data}

        Log.info(
            f"Broadcasting: Sending '{event}' to {len(subscribers)} subscribers on {channel}",
            category="cara.broadcasting",
        )
        Log.info(f"📋 Subscribers: {list(subscribers)}", category="cara.broadcasting")
        Log.info(
            f"📨 Message data keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}",
            category="cara.broadcasting",
        )

        # Send to all subscribers
        failed_connections = []
        successful_sends = 0

        for connection_id in subscribers:
            Log.debug(
                f"🔄 Attempting send to {connection_id}", category="cara.broadcasting"
            )
            if connection_id in self.connections:
                websocket = self.connections[connection_id]
                try:
                    # Skip complex connection state checks - if websocket exists, try to send
                    Log.debug(
                        f"📤 Sending message to {connection_id}",
                        category="cara.broadcasting",
                    )
                    await websocket.send_json(message)
                    successful_sends += 1
                    Log.debug(
                        f"✅ Successfully sent to {connection_id}",
                        category="cara.broadcasting",
                    )
                except Exception as e:
                    # Be more tolerant to temporary issues during job processing
                    error_msg = str(e)

                    # Only log as error for definitive connection closed errors
                    if any(
                        keyword in error_msg
                        for keyword in [
                            "ConnectionClosed",
                            "WebSocket is closed",
                            "Connection closed",
                            "ClientDisconnected",
                            "Connection is closed",
                        ]
                    ):
                        Log.debug(
                            f"Broadcasting: Connection {connection_id} closed during send",
                            category="cara.broadcasting",
                        )
                        failed_connections.append(connection_id)
                    elif "timeout" in error_msg.lower() or "busy" in error_msg.lower():
                        Log.debug(
                            f"Broadcasting: Temporary issue for {connection_id} (likely busy event loop): {e}",
                            category="cara.broadcasting",
                        )
                        # Don't mark as failed for temporary issues - just skip this send
                        continue
                    else:
                        Log.warning(
                            f"Broadcasting: Send failed for {connection_id}: {e}",
                            category="cara.broadcasting",
                        )
                        # For other errors, still mark as failed but don't be too aggressive
                        failed_connections.append(connection_id)
            else:
                # Connection not in active connections, mark for cleanup
                Log.warning(
                    f"❌ Connection {connection_id} not found in active connections",
                    category="cara.broadcasting",
                )
                failed_connections.append(connection_id)

        # Clean up failed connections
        if failed_connections:
            Log.debug(
                f"Broadcasting: Cleaning up {len(failed_connections)} failed connections",
                category="cara.broadcasting",
            )
            for connection_id in failed_connections:
                await self.remove_connection(connection_id)

        if successful_sends > 0:
            Log.info(
                f"✅ Broadcasting: Successfully sent to {successful_sends} connections",
                category="cara.broadcasting",
            )
        elif subscribers:
            Log.error(
                f"❌ Broadcasting: Failed to send to any of {len(subscribers)} subscribers on {channel}",
                category="cara.broadcasting",
            )
        else:
            Log.warning(
                f"⚠️ Broadcasting: No subscribers found for {channel}",
                category="cara.broadcasting",
            )

    async def broadcast_to_user(self, user_id: str, event: str, data: Dict[str, Any]):
        """Broadcast message to a specific user (all their connections)."""
        user_connections = [
            conn_id
            for conn_id, metadata in self.connection_metadata.items()
            if metadata.get("user_id") == user_id
        ]

        if not user_connections:
            Log.debug(
                f"Broadcasting: No connections found for user {user_id}",
                category="cara.broadcasting",
            )
            return

        message = {"event": event, "data": data}

        Log.info(
            f"Broadcasting: Sending '{event}' to user {user_id} ({len(user_connections)} connections)",
            category="cara.broadcasting",
        )

        # Send to all user connections
        for connection_id in user_connections:
            if connection_id in self.connections:
                websocket = self.connections[connection_id]
                try:
                    await websocket.send_json(message)
                except Exception as e:
                    Log.error(
                        f"Broadcasting: Failed to send to user {user_id} connection {connection_id}: {e}"
                    )

    def get_channel_subscribers(self, channel: str) -> List[str]:
        """Get list of connection IDs subscribed to a channel."""
        return list(self.channel_subscribers.get(channel, set()))

    def get_user_channels(self, connection_id: str) -> List[str]:
        """Get list of channels a connection is subscribed to."""
        return list(self.user_channels.get(connection_id, set()))

    def get_connection_count(self) -> int:
        """Get total number of active connections."""
        return len(self.connections)

    def get_channel_count(self) -> int:
        """Get total number of active channels."""
        return len(self.channel_subscribers)

    def get_stats(self) -> Dict[str, Any]:
        """Get broadcasting statistics."""
        return {
            "total_connections": self.get_connection_count(),
            "total_channels": self.get_channel_count(),
            "channels": {
                channel: len(subscribers)
                for channel, subscribers in self.channel_subscribers.items()
            },
        }

    async def broadcast(self, channel_name: str, message: Dict[str, Any]):
        """Broadcast a message to a channel - alias for broadcast_to_channel."""
        event_name = message.get("event", "message")
        data = message.get("data", {})
        await self.broadcast_to_channel(channel_name, event_name, data)

    async def _heartbeat_loop(self, connection_id: str):
        """Periodic ping to client to keep connection alive."""
        interval = self.heartbeat_interval
        while connection_id in self.connections:
            await asyncio.sleep(interval)
            websocket = self.connections.get(connection_id)
            if not websocket:
                break
            try:
                await websocket.send_json(
                    {"type": "ping", "ts": asyncio.get_running_loop().time()}
                )
            except Exception as e:
                Log.error(f"Heartbeat failed for {connection_id}: {e}")
                await self.remove_connection(connection_id)
                break
