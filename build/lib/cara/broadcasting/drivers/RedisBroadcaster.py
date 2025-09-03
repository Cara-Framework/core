"""
Redis Broadcasting Driver.

Redis-backed broadcaster for cross-process WebSocket broadcasting.
Implements Laravel-style Broadcaster interface with robust connection handling.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Dict, List, Optional, Set, Union

try:
    import redis.asyncio as redis_async
except ImportError:
    redis_async = None

from cara.broadcasting.ConnectionManager import ConnectionManager
from cara.broadcasting.contracts.Broadcaster import Broadcaster
from cara.exceptions import BroadcastingConfigurationException
from cara.facades import Log


class RedisBroadcaster(ConnectionManager, Broadcaster):
    """
    Redis broadcasting driver - Laravel Broadcaster style.

    Features:
    - Cross-process broadcasting via Redis pub/sub
    - Automatic reconnection handling
    - Clean connection lifecycle
    - Proper error handling
    - Channel prefix support
    - Connection pooling per event loop
    """

    driver_name = "redis"

    def __init__(self, config: Dict[str, Any], redis_url: str | None = None):
        """Initialize Redis broadcaster with configuration."""
        super().__init__(config)

        # Check for Redis dependency
        if redis_async is None:
            raise BroadcastingConfigurationException(
                "redis is required for RedisBroadcaster. "
                "Please install it with: pip install redis"
            )

        self._redis_async = redis_async

        # Build Redis URL from config or use provided URL
        if redis_url:
            self._redis_url = redis_url
        else:
            conn = config.get("connection", {})
            host = conn.get("host", "localhost")
            port = conn.get("port", 6379)
            db = conn.get("db", 0)
            self._redis_url = f"redis://{host}:{port}/{db}"

        self._prefix = config.get("connection", {}).get("prefix", "cara_broadcast:")
        self._sub_task: Optional[asyncio.Task] = None
        self._listener_pubsub = None
        self._redis_subscribed: Set[str] = set()

        # Store connections per event loop to avoid cross-loop issues
        self._redis_pools: Dict[int, Any] = {}
        self._redis_connections: Dict[int, Any] = {}
        self._listener_ready = asyncio.Event()

        # Add lock to prevent race conditions in _ensure_listener
        self._listener_lock = asyncio.Lock()

        # Add per-user connection limit (from config or default)
        websocket_config = config.get("websocket", {})
        self.max_connections_per_user = websocket_config.get(
            "max_connections_per_user", 10
        )

    def _get_loop_id(self) -> int:
        """Get current event loop ID for connection isolation."""
        try:
            loop = asyncio.get_running_loop()
            return id(loop)
        except RuntimeError:
            return 0  # No running loop

    async def _get_redis_pool(self):
        """Get or create Redis connection pool for current event loop."""
        loop_id = self._get_loop_id()

        if loop_id not in self._redis_pools:
            Log.debug(
                f"Creating new Redis connection pool for loop {loop_id}",
                category="cara.broadcasting",
            )
            self._redis_pools[loop_id] = self._redis_async.ConnectionPool.from_url(
                self._redis_url, decode_responses=True, max_connections=10
            )
        return self._redis_pools[loop_id]

    async def _get_redis(self):
        """Get Redis connection from pool for current event loop."""
        loop_id = self._get_loop_id()

        if loop_id not in self._redis_connections:
            try:
                pool = await self._get_redis_pool()
                self._redis_connections[loop_id] = self._redis_async.Redis(
                    connection_pool=pool
                )
                # Test the connection
                await self._redis_connections[loop_id].ping()
                Log.debug(
                    f"Created Redis connection for loop {loop_id}",
                    category="cara.broadcasting",
                )
            except Exception as e:
                Log.error(
                    f"Error getting Redis connection for loop {loop_id}: {e}",
                    category="cara.broadcasting",
                )
                # Clean up failed connection
                if loop_id in self._redis_connections:
                    del self._redis_connections[loop_id]
                raise
        return self._redis_connections[loop_id]

    def _prefix_channel(self, channel: str) -> str:
        """Add prefix to channel name if not already prefixed."""
        if channel.startswith(self._prefix):
            return channel
        return f"{self._prefix}{channel}"

    def _unprefix_channel(self, channel: str) -> str:
        """Remove prefix from channel name if present."""
        if channel.startswith(self._prefix):
            return channel[len(self._prefix) :]
        return channel

    async def broadcast(
        self, channels: Union[str, List[str]], event: str, data: Dict[str, Any]
    ):
        """Broadcast an event to one or more channels."""
        if isinstance(channels, str):
            channels = [channels]

        tasks = [self._broadcast_to_channel(channel, event, data) for channel in channels]
        await asyncio.gather(*tasks)

    async def _broadcast_to_channel(self, channel: str, event: str, data: Dict[str, Any]):
        """Simple, reliable broadcast - local first, Redis optional."""
        unprefixed_channel = self._unprefix_channel(channel)

        # ALWAYS broadcast locally first - this MUST work
        local_subscribers = self.channel_subscribers.get(unprefixed_channel, set())
        Log.debug(
            f"RB._broadcast_to_channel: channel={unprefixed_channel}, local_subscribers={len(local_subscribers)}",
            category="cara.broadcasting",
        )
        if local_subscribers:
            Log.info(
                f"📡 Broadcasting '{event}' to {len(local_subscribers)} connections on {unprefixed_channel}",
                category="cara.broadcasting",
            )

            # Send directly to each connection - simple and reliable
            successful_sends = 0
            failed_connections = []

            for connection_id in local_subscribers.copy():
                websocket = self.connections.get(connection_id)
                if websocket:
                    try:
                        message = {
                            "event": event,
                            "channel": unprefixed_channel,
                            "data": data,
                        }
                        await websocket.send_json(message)
                        successful_sends += 1
                        Log.debug(
                            f"✅ Sent to {connection_id}", category="cara.broadcasting"
                        )
                    except Exception as e:
                        Log.warning(
                            f"❌ Failed to send to {connection_id}: {e}",
                            category="cara.broadcasting",
                        )
                        failed_connections.append(connection_id)
                else:
                    Log.warning(
                        f"❌ Connection {connection_id} not found",
                        category="cara.broadcasting",
                    )
                    failed_connections.append(connection_id)

            # Clean up failed connections
            for connection_id in failed_connections:
                await self.remove_connection(connection_id)

            Log.info(
                f"✅ Broadcast completed: {successful_sends} successful, {len(failed_connections)} failed",
                category="cara.broadcasting",
            )
        else:
            Log.debug(
                f"❌ No local subscribers for {unprefixed_channel}",
                category="cara.broadcasting",
            )

        # Redis is optional - if it fails, local broadcast still worked
        try:
            prefixed_channel = self._prefix_channel(channel)
            payload = json.dumps(
                {"event": event, "channel": unprefixed_channel, "data": data}
            )
            redis = await self._get_redis()
            await redis.publish(prefixed_channel, payload)
            Log.debug(
                f"RB.publish: published to Redis channel={prefixed_channel} (unprefixed={unprefixed_channel})",
                category="cara.broadcasting",
            )
        except Exception as e:
            Log.debug(
                f"⚠️ Redis publish failed (local broadcast succeeded): {e}",
                category="cara.broadcasting",
            )
            # Don't fail the whole broadcast for Redis issues

    async def _ensure_listener(self):
        """Start Redis pubsub listener task with reconnection handling."""
        # Use lock to prevent race conditions with concurrent calls
        async with self._listener_lock:
            # Double-check pattern: check again after acquiring lock
            if self._sub_task and not self._sub_task.done():
                Log.debug(
                    "Redis listener task already running", category="cara.broadcasting"
                )
                return

            # Cancel any existing task that might be done/failed
            if self._sub_task:
                try:
                    self._sub_task.cancel()
                    await asyncio.sleep(0.1)  # Give it time to cancel
                except Exception:
                    pass
                self._sub_task = None

            # Check if Redis is available before starting
            try:
                redis = await self._get_redis()
                await redis.ping()
            except Exception as e:
                Log.warning(
                    f"Redis not available, skipping listener: {e}",
                    category="cara.broadcasting",
                )
                return

            # Only start if we have actual subscriptions to avoid unnecessary listeners
            if not self._redis_subscribed:
                Log.debug("No Redis subscriptions, skipping listener creation")
                return

            Log.info("Starting Redis listener task", category="cara.broadcasting")

            async def _listener():
                reconnect_count = 0
                while True:
                    try:
                        reconnect_count += 1
                        if reconnect_count > 1:
                            Log.warning(
                                f"Redis listener reconnecting (attempt {reconnect_count})",
                                category="cara.broadcasting",
                            )

                        redis = await self._get_redis()
                        self._listener_pubsub = redis.pubsub()
                        Log.info(
                            f"Redis pubsub initialized (attempt {reconnect_count})",
                            category="cara.broadcasting",
                        )

                        # Resubscribe to existing channels BEFORE starting the message loop
                        if self._redis_subscribed:
                            Log.info(
                                f"Resubscribing to {len(self._redis_subscribed)} channels",
                                category="cara.broadcasting",
                            )
                            await self._listener_pubsub.subscribe(*self._redis_subscribed)
                            Log.info(
                                "Resubscribed to channels", category="cara.broadcasting"
                            )

                        # Signal that listener is ready AFTER subscription
                        self._listener_ready.set()

                        # Reset reconnect count on successful initialization
                        reconnect_count = 0

                        # Start message loop using async iterator (blocks until message)
                        async for message in self._listener_pubsub.listen():
                            if message["type"] != "message":
                                continue

                            channel = message["channel"]
                            if isinstance(channel, bytes):
                                channel = channel.decode("utf-8")

                            unprefixed_channel = self._unprefix_channel(channel)
                            local_subs = self.channel_subscribers.get(
                                unprefixed_channel, set()
                            )
                            if local_subs:
                                Log.debug(
                                    f"Redis listener received message on {channel} (unprefixed={unprefixed_channel})",
                                    category="cara.broadcasting",
                                )
                            try:
                                data = json.loads(message["data"])
                            except Exception:
                                Log.error(
                                    "Invalid JSON payload from Redis", exc_info=True
                                )
                                continue

                            local_subs = self.channel_subscribers.get(
                                unprefixed_channel, set()
                            )
                            if local_subs:
                                failed_connections = []
                                for connection_id in local_subs.copy():
                                    websocket = self.connections.get(connection_id)
                                    if websocket:
                                        try:
                                            await websocket.send_json(data)
                                        except Exception as e:
                                            Log.warning(
                                                f"Failed to send Redis message to {connection_id}: {e}"
                                            )
                                            failed_connections.append(connection_id)

                                # Clean up failed connections after the loop
                                for connection_id in failed_connections:
                                    await self.remove_connection(connection_id)

                    except asyncio.CancelledError:
                        Log.info("Redis listener cancelled", category="cara.broadcasting")
                        break
                    except Exception as e:
                        Log.error(
                            f"Redis listener error (attempt {reconnect_count}): {e}",
                            category="cara.broadcasting",
                            exc_info=True,
                        )

                        # Add exponential backoff to prevent rapid reconnections
                        backoff_time = min(
                            60, 2 ** min(reconnect_count - 1, 6)
                        )  # Max 60 seconds, more aggressive
                        Log.warning(
                            f"Redis listener will retry in {backoff_time} seconds",
                            category="cara.broadcasting",
                        )
                        await asyncio.sleep(backoff_time)
                    finally:
                        if self._listener_pubsub:
                            try:
                                await self._listener_pubsub.unsubscribe()
                                await self._listener_pubsub.aclose()
                            except Exception as cleanup_error:
                                Log.error(
                                    f"Error closing pubsub: {cleanup_error}",
                                    category="cara.broadcasting",
                                )
                            self._listener_pubsub = None
                        self._listener_ready.clear()

            self._sub_task = asyncio.create_task(_listener())

            # Wait for listener to be ready, but don't block forever
            try:
                await asyncio.wait_for(self._listener_ready.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                Log.warning(
                    "Redis listener initialization timed out",
                    category="cara.broadcasting",
                )
                # Cancel the task if it didn't start properly
                if self._sub_task:
                    self._sub_task.cancel()
                    self._sub_task = None

    async def add_connection(
        self,
        connection_id: str,
        websocket: Any,
        user_id: Optional[str] = None,
        metadata: Optional[Dict] = None,
    ):
        """Add a new WebSocket connection with user limits."""
        # Check if connection already exists
        if connection_id in self.connections:
            Log.warning(f"Connection {connection_id} already exists, removing old one")
            await self.remove_connection(connection_id)

        # Check user connection limit (max 10 connections per user)
        if user_id:
            user_connections = [
                conn_id
                for conn_id, meta in self.connection_metadata.items()
                if meta.get("user_id") == user_id
            ]
            if len(user_connections) >= self.max_connections_per_user:
                # Remove oldest connection for this user
                oldest_connection = user_connections[0]
                Log.info(
                    f"Removing oldest connection {oldest_connection} for user {user_id} (limit: {self.max_connections_per_user})"
                )
                await self.remove_connection(oldest_connection)

        # Check global connection limit
        if len(self.connections) >= self.max_connections:
            raise Exception(f"Maximum connections ({self.max_connections}) exceeded")

        # Log connection details
        Log.info(
            f"Broadcasting: Adding connection {connection_id} for user {user_id or 'anonymous'}",
            category="cara.broadcasting",
        )
        Log.info(
            f"RB.add_connection: now total_connections(before_add)={len(self.connections)}",
            category="cara.broadcasting",
        )

        # Call parent add_connection method
        await super().add_connection(connection_id, websocket, user_id, metadata)

        # Log current connection stats
        Log.debug(
            f"Broadcasting stats: {len(self.connections)} total connections, "
            f"{len(self._redis_subscribed)} Redis channels, "
            f"Listener active: {self._sub_task and not self._sub_task.done() if self._sub_task else False}",
            category="cara.broadcasting",
        )

    async def subscribe(self, connection_id: str, channel: str) -> bool:
        """Subscribe connection to channel with duplicate prevention."""
        Log.debug(
            f"Subscribing {connection_id} to channel '{channel}'",
            category="cara.broadcasting",
        )

        # Check if connection exists
        if connection_id not in self.connections:
            Log.warning(
                f"Connection {connection_id} not found for subscription to {channel}"
            )
            return False

        # Check if already subscribed locally
        unprefixed_channel = self._unprefix_channel(channel)
        if connection_id in self.channel_subscribers.get(unprefixed_channel, set()):
            Log.debug(
                f"Connection {connection_id} already subscribed to {unprefixed_channel}"
            )
            return True

        # First do local subscription
        ok = await ConnectionManager.subscribe(self, connection_id, unprefixed_channel)
        if not ok:
            Log.warning(
                f"Local subscription failed for {connection_id} to channel '{unprefixed_channel}'"
            )
            return False

        prefixed_channel = self._prefix_channel(channel)

        try:
            # Check if this is the first subscription to this channel
            first_time = prefixed_channel not in self._redis_subscribed

            if first_time:
                # Add channel to set BEFORE starting listener to prevent race condition
                self._redis_subscribed.add(prefixed_channel)
                Log.debug(
                    f"First subscription to Redis channel '{prefixed_channel}'",
                    category="cara.broadcasting",
                )

                # Ensure Redis listener is running (only when we have subscriptions)
                await self._ensure_listener()

                # Subscribe to the channel if listener is ready
                if self._listener_pubsub:
                    await self._listener_pubsub.subscribe(prefixed_channel)
                    Log.info(
                        f"Subscribed to Redis channel '{prefixed_channel}'",
                        category="cara.broadcasting",
                    )
            else:
                Log.debug(
                    f"Redis channel '{prefixed_channel}' already subscribed",
                    category="cara.broadcasting",
                )

            # Debug info
            Log.debug(
                f"Current Redis subscriptions: {len(self._redis_subscribed)} channels",
                category="cara.broadcasting",
            )
            Log.debug(
                f"Local channel subscribers for {unprefixed_channel}: {len(self.channel_subscribers.get(unprefixed_channel, set()))}",
                category="cara.broadcasting",
            )
            return True

        except Exception as e:
            # Only log error if it's not a connection issue
            if "Connection refused" not in str(e) and "redis" not in str(e).lower():
                Log.error(
                    f"Redis subscribe error for {prefixed_channel}: {e}",
                    category="cara.broadcasting",
                )
            self._redis_subscribed.discard(prefixed_channel)
            # Also remove from local subscription
            await ConnectionManager.unsubscribe(self, connection_id, unprefixed_channel)
            return False

    async def remove_connection(self, connection_id: str):
        """Remove a WebSocket connection with proper cleanup."""
        if connection_id not in self.connections:
            return

        Log.info(
            f"Broadcasting: Removing connection {connection_id}",
            category="cara.broadcasting",
        )

        # Get channels before removal for cleanup
        channels_to_cleanup = self.user_channels.get(connection_id, set()).copy()

        # Call parent remove_connection method
        await super().remove_connection(connection_id)

        # Clean up Redis subscriptions if no more local subscribers
        channels_unsubscribed = []
        for channel in channels_to_cleanup:
            local_subscribers = self.channel_subscribers.get(channel, set())
            if not local_subscribers:  # No more local subscribers
                prefixed_channel = self._prefix_channel(channel)
                if prefixed_channel in self._redis_subscribed:
                    self._redis_subscribed.discard(prefixed_channel)
                    channels_unsubscribed.append(prefixed_channel)
                    if self._listener_pubsub:
                        try:
                            await self._listener_pubsub.unsubscribe(prefixed_channel)
                            Log.debug(
                                f"Unsubscribed from Redis channel '{prefixed_channel}'"
                            )
                        except Exception as e:
                            Log.error(
                                f"Error unsubscribing from Redis channel {prefixed_channel}: {e}"
                            )

        # If we unsubscribed from channels, log it
        if channels_unsubscribed:
            Log.info(
                f"Unsubscribed from {len(channels_unsubscribed)} Redis channels",
                category="cara.broadcasting",
            )

        # If no more Redis subscriptions, we can stop the listener task to save resources
        if not self._redis_subscribed and self._sub_task and not self._sub_task.done():
            Log.info(
                "No more Redis subscriptions, stopping listener task",
                category="cara.broadcasting",
            )
            try:
                self._sub_task.cancel()
                await asyncio.sleep(0.1)  # Give it time to cancel
            except Exception:
                pass
            finally:
                self._sub_task = None
                self._listener_ready.clear()

    async def _heartbeat_loop(self, connection_id: str):
        """Simple, reliable heartbeat - only remove on definitive connection close."""
        interval = self.heartbeat_interval
        failed_pings = 0
        max_failed_pings = 10  # Very tolerant

        while connection_id in self.connections:
            await asyncio.sleep(interval)
            websocket = self.connections.get(connection_id)
            if not websocket:
                break

            try:
                # Simple ping - no complex checks
                await websocket.send_json({"event": "ping", "timestamp": time.time()})
                failed_pings = 0  # Reset on success
                Log.debug(
                    f"💓 Heartbeat sent to {connection_id}", category="cara.broadcasting"
                )

            except Exception as e:
                failed_pings += 1
                error_msg = str(e)

                # Only remove on definitive connection errors
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
                    Log.info(f"💀 Connection {connection_id} closed, removing")
                    await self.remove_connection(connection_id)
                    break

                # Be very tolerant for other errors
                if failed_pings >= max_failed_pings:
                    Log.warning(
                        f"💀 Connection {connection_id} failed {max_failed_pings} pings, removing"
                    )
                    await self.remove_connection(connection_id)
                    break
                else:
                    Log.debug(
                        f"⚠️ Heartbeat failed for {connection_id} ({failed_pings}/{max_failed_pings}): {e}"
                    )
                    # Simple backoff
                    await asyncio.sleep(min(10, failed_pings * 2))

    async def cleanup(self):
        """Clean up Redis connections and tasks."""
        if self._sub_task and not self._sub_task.done():
            self._sub_task.cancel()
            try:
                await self._sub_task
            except asyncio.CancelledError:
                pass

        if self._listener_pubsub:
            try:
                await self._listener_pubsub.aclose()
            except Exception:
                pass
            self._listener_pubsub = None

        # Clean up all Redis connections for all event loops
        for loop_id, redis in self._redis_connections.items():
            try:
                await redis.aclose()
                Log.debug(
                    f"Closed Redis connection for loop {loop_id}",
                    category="cara.broadcasting",
                )
            except Exception:
                pass
        self._redis_connections.clear()

        for loop_id, pool in self._redis_pools.items():
            try:
                await pool.disconnect()
                Log.debug(
                    f"Closed Redis pool for loop {loop_id}", category="cara.broadcasting"
                )
            except Exception:
                pass
        self._redis_pools.clear()

    async def cleanup_old_connections(self):
        """Clean up old/inactive connections periodically."""
        try:
            current_time = time.time()
            cleanup_interval = self.config.get("websocket", {}).get(
                "cleanup_interval", 60
            )

            # Check for connections that haven't had activity in the last cleanup_interval seconds
            connections_to_remove = []
            for connection_id, metadata in self.connection_metadata.items():
                last_activity = metadata.get("last_activity", 0)
                if current_time - last_activity > cleanup_interval:
                    connections_to_remove.append(connection_id)

            for connection_id in connections_to_remove:
                Log.debug(
                    f"Cleaning up old connection {connection_id}",
                    category="cara.broadcasting",
                )
                await self.remove_connection(connection_id)

        except Exception as e:
            Log.error(
                f"Error during connection cleanup: {e}", category="cara.broadcasting"
            )
