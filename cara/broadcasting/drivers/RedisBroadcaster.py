"""
Redis broadcasting driver — cross-process pub/sub on top of
``ConnectionManager``'s in-process state.

Topology
--------
Every broadcasting node (every process running the API or services
worker that dispatches events) maintains:

1. A *publisher* Redis client used for ``PUBLISH`` calls. Pooled per
   running event loop so the connection isn't shared across loops
   that asyncio doesn't allow to share clients.

2. A *subscriber* pubsub object that listens for messages on the set
   of channels this node currently has local subscribers for. The
   listener task drains incoming messages into the local
   ``ConnectionManager`` so cross-process broadcasts reach this
   node's WebSocket clients.

3. A per-user pubsub channel ``__user:{user_id}`` that this node
   auto-subscribes to whenever it has a connection for that user.
   ``broadcast_to_user`` publishes to that channel, every node with
   a connection for that user delivers locally.

Self-broadcast deduplication
----------------------------
Every published payload carries the originating ``_node_id``. When
the listener receives a message originated from this node it skips
delivery — local subscribers were already served synchronously by
``broadcast`` before the message was put on the wire. Without this
guard every event would be double-delivered to local subscribers.

Reconnection
------------
The listener task auto-reconnects with exponential backoff. On every
reconnect it resubscribes to the full ``_redis_subscribed`` set so
no channel is silently lost during a Redis blip.
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from typing import Any, Dict, List, Optional, Set, Union

try:
    import redis.asyncio as redis_async
except ImportError:
    redis_async = None  # type: ignore[assignment]

from cara.broadcasting.ConnectionManager import (
    ConnectionManager,
    _is_connection_closed_error,
)
from cara.broadcasting.contracts.Broadcaster import Broadcaster
from cara.exceptions import BroadcastingConfigurationException
from cara.facades import Log


# Pubsub channel name for per-user broadcasts. ``broadcast_to_user``
# publishes here; nodes auto-subscribe whenever they hold a connection
# for the relevant user. The ``__user:`` prefix is private to the
# driver and doesn't collide with application channel names.
_USER_CHANNEL_PREFIX = "__user:"


class RedisBroadcaster(ConnectionManager, Broadcaster):
    """Redis-backed broadcaster.

    Implements the full ``Broadcaster`` contract. State is split
    between in-process bookkeeping (inherited ``ConnectionManager``)
    and the Redis transport (pubsub + per-loop client pool).
    """

    driver_name = "redis"

    def __init__(self, config: Dict[str, Any], redis_url: Optional[str] = None) -> None:
        super().__init__(config)

        if redis_async is None:
            raise BroadcastingConfigurationException(
                "redis is required for RedisBroadcaster. Install with: pip install redis"
            )
        self._redis_async = redis_async

        if redis_url:
            self._redis_url = redis_url
        else:
            conn = config.get("connection", {})
            host = conn.get("host", "localhost")
            port = conn.get("port", 6379)
            db = conn.get("db", 0)
            self._redis_url = f"redis://{host}:{port}/{db}"

        self._prefix: str = config.get("connection", {}).get("prefix", "cara_broadcast:")
        # Identity for "did this message originate from us?" check.
        self._node_id: str = uuid.uuid4().hex

        # Currently-subscribed Redis channels (prefixed). Source of
        # truth for resubscribe-on-reconnect.
        self._redis_subscribed: Set[str] = set()

        # Per-loop Redis clients. asyncio doesn't allow sharing a
        # client across event loops, so workers that briefly create
        # their own loop (queue runners, scripts) get their own.
        self._redis_clients: Dict[int, Any] = {}
        self._redis_pools: Dict[int, Any] = {}

        # Listener bookkeeping. ``_listener_task`` is the long-running
        # asyncio.Task that drains pubsub messages; ``_listener_pubsub``
        # is the active pubsub object it owns.
        self._listener_task: Optional[asyncio.Task] = None
        self._listener_pubsub: Any = None
        self._listener_ready: asyncio.Event = asyncio.Event()
        self._listener_lock: asyncio.Lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Redis client lifecycle
    # ------------------------------------------------------------------
    @staticmethod
    def _loop_id() -> int:
        try:
            return id(asyncio.get_running_loop())
        except RuntimeError:
            return 0

    async def _redis(self) -> Any:
        """Return a Redis client pinned to the current event loop.

        Lazily creates a connection pool + client on first use per
        loop. ``ping()`` validates the connection so callers see a
        real failure here rather than during the next publish.
        """
        loop_id = self._loop_id()
        if loop_id in self._redis_clients:
            return self._redis_clients[loop_id]

        if loop_id not in self._redis_pools:
            self._redis_pools[loop_id] = self._redis_async.ConnectionPool.from_url(
                self._redis_url, decode_responses=True, max_connections=10
            )
        client = self._redis_async.Redis(connection_pool=self._redis_pools[loop_id])
        await client.ping()
        self._redis_clients[loop_id] = client
        Log.debug(
            f"RedisBroadcaster: created client for loop {loop_id}",
            category="cara.broadcasting",
        )
        return client

    # ------------------------------------------------------------------
    # Channel name helpers
    # ------------------------------------------------------------------
    def _prefixed(self, channel: str) -> str:
        return channel if channel.startswith(self._prefix) else f"{self._prefix}{channel}"

    def _unprefixed(self, channel: str) -> str:
        return channel[len(self._prefix) :] if channel.startswith(self._prefix) else channel

    # ------------------------------------------------------------------
    # Broadcast
    # ------------------------------------------------------------------
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
        # Run all channels in parallel — most of the time is spent
        # awaiting Redis; serializing the publishes leaves throughput
        # on the table.
        await asyncio.gather(
            *(
                self._broadcast_one(channel, event, data, except_socket_id=except_socket_id)
                for channel in channels
            )
        )

    async def _broadcast_one(
        self,
        channel: str,
        event: str,
        data: Dict[str, Any],
        *,
        except_socket_id: Optional[str] = None,
    ) -> None:
        unprefixed = self._unprefixed(channel)

        # Local fan-out FIRST — never blocked by Redis. If the
        # publish below fails for transport reasons (Redis briefly
        # unreachable) at least the same-node subscribers got the
        # message.
        try:
            await self.broadcast_to_channel(
                unprefixed, event, data, except_socket_id=except_socket_id
            )
        except Exception as e:
            Log.warning(
                f"Local fan-out failed for '{event}' on {unprefixed}: {e}",
                category="cara.broadcasting",
            )

        # Cross-process publish. Skip-self is encoded into the payload
        # so the listener can drop messages we originated.
        try:
            payload = json.dumps(
                {
                    "event": event,
                    "channel": unprefixed,
                    "data": data,
                    "_node_id": self._node_id,
                    "_except_socket_id": except_socket_id,
                }
            )
            client = await self._redis()
            await client.publish(self._prefixed(unprefixed), payload)
        except Exception as e:
            Log.debug(
                f"Redis publish failed for {unprefixed} (local delivery succeeded): {e}",
                category="cara.broadcasting",
            )

    # ------------------------------------------------------------------
    # broadcast_to_user — cross-process via per-user Redis channel.
    # ------------------------------------------------------------------
    async def broadcast_to_user(
        self,
        user_id: str,
        event: str,
        data: Dict[str, Any],
        *,
        except_socket_id: Optional[str] = None,
    ) -> None:
        # Local delivery first — same reasoning as ``_broadcast_one``.
        await self.broadcast_to_user_local(
            user_id, event, data, except_socket_id=except_socket_id
        )
        # Then fan out across processes.
        try:
            payload = json.dumps(
                {
                    "event": event,
                    "user_id": user_id,
                    "data": data,
                    "_node_id": self._node_id,
                    "_except_socket_id": except_socket_id,
                }
            )
            client = await self._redis()
            await client.publish(self._prefixed(f"{_USER_CHANNEL_PREFIX}{user_id}"), payload)
        except Exception as e:
            Log.debug(
                f"Redis publish to user {user_id} failed (local delivery succeeded): {e}",
                category="cara.broadcasting",
            )

    # ------------------------------------------------------------------
    # Connection lifecycle (with cross-process per-user channel
    # subscription)
    # ------------------------------------------------------------------
    async def add_connection(
        self,
        connection_id: str,
        websocket: Any,
        user_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        await super().add_connection(connection_id, websocket, user_id, metadata)
        # First connection for this user → ensure our listener is on
        # the per-user pubsub channel so other nodes' broadcasts to
        # them reach us.
        if user_id and len(self.user_connections.get(user_id, set())) == 1:
            await self._ensure_redis_subscription(
                self._prefixed(f"{_USER_CHANNEL_PREFIX}{user_id}")
            )

    async def remove_connection(self, connection_id: str) -> None:
        meta = self.connection_metadata.get(connection_id, {})
        user_id = meta.get("user_id")
        channels = self.connection_channels.get(connection_id, set()).copy()

        await super().remove_connection(connection_id)

        # Unsubscribe Redis from channels with no remaining local
        # subscribers — reduces broker fan-out work and listener
        # message volume.
        for channel in channels:
            if not self.channel_subscribers.get(channel):
                await self._drop_redis_subscription(self._prefixed(channel))

        # User-specific channel: drop when this was the user's last
        # local connection.
        if user_id and not self.user_connections.get(user_id):
            await self._drop_redis_subscription(
                self._prefixed(f"{_USER_CHANNEL_PREFIX}{user_id}")
            )

    # ------------------------------------------------------------------
    # Subscription — also ensures the Redis pubsub channel is wired up.
    # ------------------------------------------------------------------
    async def subscribe(self, connection_id: str, channel: str) -> bool:
        if connection_id not in self.connections:
            return False
        if not await super().subscribe(connection_id, channel):
            return False
        # Wire the listener up to this channel if it isn't already.
        await self._ensure_redis_subscription(self._prefixed(channel))
        return True

    async def unsubscribe(self, connection_id: str, channel: str) -> bool:
        await super().unsubscribe(connection_id, channel)
        if not self.channel_subscribers.get(channel):
            await self._drop_redis_subscription(self._prefixed(channel))
        return True

    # ------------------------------------------------------------------
    # Listener task management
    # ------------------------------------------------------------------
    async def _ensure_redis_subscription(self, prefixed_channel: str) -> None:
        """Subscribe the Redis listener to ``prefixed_channel`` if not
        already. Lazily starts the listener task on first call."""
        if prefixed_channel in self._redis_subscribed:
            return

        async with self._listener_lock:
            if prefixed_channel in self._redis_subscribed:
                return  # Re-check inside the lock.

            self._redis_subscribed.add(prefixed_channel)

            # Start the listener task on first subscription. The task
            # subscribes to every channel in ``_redis_subscribed`` so
            # everything we've added so far is picked up.
            if self._listener_task is None or self._listener_task.done():
                self._listener_task = asyncio.create_task(self._listener_loop())
                # Wait for initial subscribe to complete so callers can
                # publish immediately after; bounded so we don't deadlock
                # when Redis is unreachable.
                try:
                    await asyncio.wait_for(self._listener_ready.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    Log.warning(
                        "Redis listener didn't become ready within 5s; "
                        "broadcasts may briefly miss subscribers",
                        category="cara.broadcasting",
                    )
            else:
                # Listener already running — just add the new channel.
                # Avoid the previous double-SUBSCRIBE bug by NOT calling
                # subscribe() again here unless we have an active pubsub.
                pubsub = self._listener_pubsub
                if pubsub is not None:
                    try:
                        await pubsub.subscribe(prefixed_channel)
                    except Exception as e:
                        Log.warning(
                            f"Failed to subscribe Redis listener to {prefixed_channel}: {e}",
                            category="cara.broadcasting",
                        )
                        self._redis_subscribed.discard(prefixed_channel)

    async def _drop_redis_subscription(self, prefixed_channel: str) -> None:
        if prefixed_channel not in self._redis_subscribed:
            return
        self._redis_subscribed.discard(prefixed_channel)
        pubsub = self._listener_pubsub
        if pubsub is not None:
            try:
                await pubsub.unsubscribe(prefixed_channel)
            except Exception as e:
                Log.debug(
                    f"Redis unsubscribe of {prefixed_channel} failed: {e}",
                    category="cara.broadcasting",
                )
        # Optimisation: if we no longer have any subscriptions, the
        # listener can shut down to free a connection.
        if not self._redis_subscribed and self._listener_task and not self._listener_task.done():
            Log.info(
                "Redis listener idle (no subscriptions); shutting down",
                category="cara.broadcasting",
            )
            self._listener_task.cancel()
            self._listener_task = None
            self._listener_ready.clear()

    async def _listener_loop(self) -> None:
        """Long-running task that drains pubsub messages and dispatches
        them to local subscribers. Auto-reconnects with exponential
        backoff on Redis failures."""
        attempt = 0
        while True:
            try:
                client = await self._redis()
                self._listener_pubsub = client.pubsub()
                if self._redis_subscribed:
                    await self._listener_pubsub.subscribe(*self._redis_subscribed)
                self._listener_ready.set()
                attempt = 0  # Reset after a successful (re)connect.

                async for message in self._listener_pubsub.listen():
                    if message.get("type") != "message":
                        continue
                    await self._dispatch_pubsub_message(message)
            except asyncio.CancelledError:
                Log.debug("Redis listener cancelled", category="cara.broadcasting")
                break
            except Exception as e:
                attempt += 1
                backoff = min(60, 2 ** min(attempt - 1, 6))
                Log.error(
                    f"Redis listener crashed (attempt {attempt}): {e}; "
                    f"retrying in {backoff}s",
                    category="cara.broadcasting",
                    exc_info=True,
                )
                await asyncio.sleep(backoff)
            finally:
                if self._listener_pubsub is not None:
                    try:
                        await self._listener_pubsub.aclose()
                    except Exception:
                        pass
                    self._listener_pubsub = None
                self._listener_ready.clear()

    async def _dispatch_pubsub_message(self, message: Dict[str, Any]) -> None:
        """Decode an incoming pubsub frame and deliver it locally.

        Skips frames originated by this node (already delivered
        synchronously during ``broadcast``) and routes per-user
        channels to ``broadcast_to_user_local`` while regular
        channels go to ``broadcast_to_channel``.
        """
        channel_raw = message.get("channel")
        if isinstance(channel_raw, bytes):
            channel_raw = channel_raw.decode("utf-8")
        unprefixed = self._unprefixed(channel_raw or "")

        try:
            payload = json.loads(message.get("data") or "{}")
        except Exception:
            Log.warning(
                f"Discarding non-JSON pubsub frame on {channel_raw}",
                category="cara.broadcasting",
            )
            return

        if not isinstance(payload, dict):
            return
        if payload.get("_node_id") == self._node_id:
            return  # Echo of our own publish — local already delivered.

        except_sid = payload.get("_except_socket_id")
        event = payload.get("event") or "message"
        data = payload.get("data") or {}

        if unprefixed.startswith(_USER_CHANNEL_PREFIX):
            user_id = unprefixed[len(_USER_CHANNEL_PREFIX) :]
            await self.broadcast_to_user_local(
                user_id, event, data, except_socket_id=except_sid
            )
        else:
            await self.broadcast_to_channel(
                unprefixed, event, data, except_socket_id=except_sid
            )

    # ------------------------------------------------------------------
    # Override base heartbeat with closed-connection tolerance — keeps
    # noisy disconnects out of error logs.
    # ------------------------------------------------------------------
    async def _heartbeat_loop(self, connection_id: str) -> None:
        interval = self.heartbeat_interval
        try:
            while connection_id in self.connections:
                await asyncio.sleep(interval)
                ws = self.connections.get(connection_id)
                if ws is None:
                    return
                try:
                    await ws.send_json({"event": "ping", "ts": time.time()})
                except Exception as e:
                    if _is_connection_closed_error(e):
                        Log.debug(
                            f"Heartbeat: {connection_id} closed",
                            category="cara.broadcasting",
                        )
                    else:
                        Log.warning(
                            f"Heartbeat to {connection_id} failed: {e}",
                            category="cara.broadcasting",
                        )
                    await self.remove_connection(connection_id)
                    return
        except asyncio.CancelledError:
            raise

    # ------------------------------------------------------------------
    # Cleanup — application shutdown hook.
    # ------------------------------------------------------------------
    async def cleanup(self) -> None:
        if self._listener_task and not self._listener_task.done():
            self._listener_task.cancel()
            try:
                await self._listener_task
            except (asyncio.CancelledError, Exception):
                pass

        if self._listener_pubsub is not None:
            try:
                await self._listener_pubsub.aclose()
            except Exception:
                pass
            self._listener_pubsub = None

        for client in self._redis_clients.values():
            try:
                await client.aclose()
            except Exception:
                pass
        self._redis_clients.clear()

        for pool in self._redis_pools.values():
            try:
                await pool.disconnect()
            except Exception:
                pass
        self._redis_pools.clear()

        await super().cleanup()
