"""
Redis broadcasting driver — cross-process pub/sub on top of
``ConnectionManager``'s in-process state.

Topology
--------
Every broadcasting node (every process running the API or services
worker that dispatches events) maintains:

1. A *publisher* (``RedisPublisher``) — one process-wide synchronous
   client for ``PUBLISH``, shared by every event loop. That module says
   why a per-loop async client leaked a socket per queued job, and why
   one writer keeps this process's frames in the order they were
   broadcast.

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
delivery — local subscribers are served by ``broadcast`` itself, next
to the publish. Without this guard every event would be
double-delivered to local subscribers.

Reconnection
------------
The listener task auto-reconnects with exponential backoff. On every
reconnect it resubscribes to the full ``_redis_subscribed`` set so
no channel is silently lost during a Redis blip.

A connection that dies without saying so (a dropped NAT flow, a
powered-off host) never fails a read on its own, so the listener
polls: after a quiet spell it sends a PING, and a PING nobody answers
is a dead connection, reconnected like any other failure.

The pubsub connection has ONE owner, the listener task. While it is
(re)connecting, ``_listener_pubsub`` is ``None`` and a subscription
change only edits ``_redis_subscribed``; the listener applies that set
to the new connection before it calls it live.
"""

from __future__ import annotations

import asyncio
import json
import socket
import uuid
import weakref
from collections import deque
from typing import Any

try:
    import redis.asyncio as redis_async
except ImportError:
    redis_async = None  # type: ignore[assignment]

import contextlib

from cara.broadcasting.ConnectionManager import ConnectionManager
from cara.broadcasting.contracts.Broadcaster import Broadcaster
from cara.broadcasting.drivers.RedisPublisher import RedisPublisher
from cara.exceptions import BroadcastingConfigurationException
from cara.facades import Log
from cara.support import json_dumps

from . import _RedisListener

# Pubsub channel name for per-user broadcasts. ``broadcast_to_user``
# publishes here; nodes auto-subscribe whenever they hold a connection
# for the relevant user. The ``__user:`` prefix is private to the
# driver and doesn't collide with application channel names.
_USER_CHANNEL_PREFIX = "__user:"

# Frames one channel may have waiting. A wedged subscriber is reaped within one
# send timeout, so this only has to outlast that against a burst.
_LANE_CAPACITY = 2048

# Kernel keepalive for the listener's socket, the layer under the PING: probe
# after 30 idle seconds, every 10 s, give up after 3. Only the options this
# platform has (Linux names the idle one TCP_KEEPIDLE, macOS TCP_KEEPALIVE).
_KEEPALIVE_OPTIONS: dict[int, int] = {
    getattr(socket, name): value
    for name, value in (
        ("TCP_KEEPIDLE", 30),
        ("TCP_KEEPALIVE", 30),
        ("TCP_KEEPINTVL", 10),
        ("TCP_KEEPCNT", 3),
    )
    if hasattr(socket, name)
}


class RedisBroadcaster(ConnectionManager, Broadcaster):
    """Redis-backed broadcaster.

    Implements the full ``Broadcaster`` contract. State is split
    between in-process bookkeeping (inherited ``ConnectionManager``)
    and the Redis transport (pubsub + per-loop client pool).
    """

    driver_name = "redis"

    def __init__(self, config: dict[str, Any], redis_url: str | None = None) -> None:
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
        self._connection_config: dict[str, Any] = dict(config.get("connection", {}))
        # Identity for "did this message originate from us?" check.
        self._node_id: str = uuid.uuid4().hex

        # Currently-subscribed Redis channels (prefixed). Source of
        # truth for resubscribe-on-reconnect.
        self._redis_subscribed: set[str] = set()

        # Per-loop async Redis clients — used ONLY by the long-lived
        # pub/sub listener. PUBLISH goes through ``RedisPublisher``.
        self._redis_clients: dict[int, Any] = {}
        self._redis_pools: dict[int, Any] = {}
        self._redis_loop_refs: dict[int, weakref.ref[asyncio.AbstractEventLoop]] = {}

        # Process-wide PUBLISH client, shared across every ephemeral
        # ``asyncio.run`` loop.
        self._publisher = RedisPublisher(self._redis_url, self._connection_config)

        # Listener bookkeeping. ``_listener_task`` is the long-running
        # asyncio.Task that drains pubsub messages. ``_listener_pubsub``
        # is the pubsub it owns, and is set ONLY while that pubsub is live:
        # connected and subscribed to all of ``_redis_subscribed``. ``None``
        # means the listener is (re)connecting and will apply the set itself.
        self._listener_task: asyncio.Task | None = None
        self._listener_pubsub: Any = None
        self._listener_ready: asyncio.Event = asyncio.Event()
        self._listener_lock: asyncio.Lock = asyncio.Lock()
        # Held for every SUBSCRIBE / UNSUBSCRIBE sent on the live pubsub from
        # outside the listener, and by the listener while it closes that
        # pubsub: a change in flight finishes before its connection goes away.
        self._pubsub_lock: asyncio.Lock = asyncio.Lock()
        # A SUBSCRIBE failed on the live pubsub: the listener rebuilds its
        # connection from ``_redis_subscribed`` at its next poll.
        self._listener_resync: bool = False
        self._poll_seconds: float = _RedisListener._POLL_SECONDS
        self._ping_after: float = _RedisListener._PING_AFTER
        self._pong_within: float = _RedisListener._PONG_WITHIN

        # Frames read but not yet delivered, one lane per pubsub channel.
        # See ``_enqueue_pubsub_message``.
        self._lanes: dict[Any, deque[dict[str, Any]]] = {}
        self._lane_tasks: set[asyncio.Task] = set()
        self._shedding_lanes: set[Any] = set()

    # ------------------------------------------------------------------
    # Redis client lifecycle
    # ------------------------------------------------------------------
    async def _publish_redis(self, channel: str, payload: str) -> None:
        """PUBLISH via the sync client off the event loop thread.

        Kept as a single seam so tests can stub the wire hop without
        standing up a real Redis, and so the async listener's client
        pool is never touched by the publish path.
        """
        await self._publisher.publish(channel, payload)

    @staticmethod
    def _loop_id() -> int:
        try:
            return id(asyncio.get_running_loop())
        except RuntimeError:
            return 0

    async def _reap_stale_async_pools(self) -> None:
        """Disconnect async pools whose event loops have closed.

        Belt-and-suspenders for the listener path: if anything briefly
        creates a loop, uses ``_redis()``, then lets the loop die, we
        must not keep its TCP connections around.
        """
        stale: list[int] = []
        for loop_id, ref in list(self._redis_loop_refs.items()):
            loop = ref()
            if loop is None or loop.is_closed():
                stale.append(loop_id)
        for loop_id in stale:
            await self._dispose_loop_redis(loop_id)

    async def _dispose_loop_redis(self, loop_id: int) -> None:
        self._redis_loop_refs.pop(loop_id, None)
        client = self._redis_clients.pop(loop_id, None)
        pool = self._redis_pools.pop(loop_id, None)
        if client is not None:
            with contextlib.suppress(
                OSError, RuntimeError, AttributeError, ConnectionError
            ):
                await client.aclose()
        if pool is not None:
            with contextlib.suppress(
                OSError, RuntimeError, AttributeError, ConnectionError
            ):
                await pool.disconnect()

    async def _redis(self) -> Any:
        """Return a Redis client pinned to the current event loop.

        Lazily creates a connection pool + client on first use per
        loop. Used by the pub/sub listener only — publish goes through
        ``_publish_redis``. ``ping()`` validates the connection so
        callers see a real failure here rather than during listen.
        """
        await self._reap_stale_async_pools()
        loop = asyncio.get_running_loop()
        loop_id = id(loop)
        if loop_id in self._redis_clients:
            return self._redis_clients[loop_id]

        if loop_id not in self._redis_pools:
            # Bounded socket timeouts: without them ``ping`` / a SUBSCRIBE
            # block on the OS TCP timeout (minutes) when Redis black-holes
            # (dropped node, network partition) rather than refusing. They do
            # NOT bound the listener's wait for the next frame; that is the
            # watchdog in ``_read_until_silent``.
            pool_kwargs: dict[str, Any] = {
                "decode_responses": True,
                "max_connections": 10,
                "socket_connect_timeout": 5,
                "socket_timeout": 5,
                "socket_keepalive": True,
                "socket_keepalive_options": _KEEPALIVE_OPTIONS,
                "health_check_interval": 30,
            }
            if self._redis_url.startswith("rediss://"):
                pool_kwargs.update(
                    {
                        "ssl_ca_certs": self._connection_config.get("ssl_ca_certs")
                        or None,
                        "ssl_certfile": self._connection_config.get("ssl_certfile")
                        or None,
                        "ssl_keyfile": self._connection_config.get("ssl_keyfile") or None,
                        "ssl_cert_reqs": self._connection_config.get(
                            "ssl_cert_reqs", "required"
                        ),
                    }
                )
            self._redis_pools[loop_id] = self._redis_async.ConnectionPool.from_url(
                self._redis_url,
                **pool_kwargs,
            )
        client = self._redis_async.Redis(connection_pool=self._redis_pools[loop_id])
        await client.ping()
        self._redis_clients[loop_id] = client
        self._redis_loop_refs[loop_id] = weakref.ref(loop)
        Log.debug(
            "RedisBroadcaster: created listener client for loop %s",
            loop_id,
            category="cara.broadcasting",
        )
        return client

    # ------------------------------------------------------------------
    # Channel name helpers
    # ------------------------------------------------------------------
    def _prefixed(self, channel: str) -> str:
        return channel if channel.startswith(self._prefix) else f"{self._prefix}{channel}"

    def _unprefixed(self, channel: str) -> str:
        return (
            channel[len(self._prefix) :] if channel.startswith(self._prefix) else channel
        )

    # ------------------------------------------------------------------
    # Broadcast
    # ------------------------------------------------------------------
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
        # Run all channels in parallel — most of the time is spent
        # awaiting Redis; serializing the publishes leaves throughput
        # on the table.
        await asyncio.gather(
            *(
                self._broadcast_one(
                    channel, event, data, except_socket_id=except_socket_id
                )
                for channel in channels
            )
        )

    async def _broadcast_one(
        self,
        channel: str,
        event: str,
        data: dict[str, Any],
        *,
        except_socket_id: str | None = None,
    ) -> None:
        unprefixed = self._unprefixed(channel)

        # Skip-self is encoded into the payload so the listener can drop
        # messages we originated.
        #
        # Encoding comes first and sits outside any try: a payload cara
        # cannot encode is not a Redis blip — it is a programming error.
        # Swallowing it into a ``Log.debug`` dropped the broadcast on every
        # node and told nobody, which is how a Decimal could ship as a float
        # here for as long as it did. Raised here, before anyone is served,
        # it fails this one broadcast and nothing else.
        payload = json_dumps(
            {
                "event": event,
                "channel": unprefixed,
                "data": data,
                "_node_id": self._node_id,
                "_except_socket_id": except_socket_id,
            },
        )
        await self._publish_and_deliver(
            self._prefixed(unprefixed),
            payload,
            self.broadcast_to_channel(
                unprefixed, event, data, except_socket_id=except_socket_id
            ),
            label=unprefixed,
        )

    async def _publish_and_deliver(
        self, prefixed_channel: str, payload: str, deliver: Any, *, label: str
    ) -> None:
        """Put the frame on the wire and serve local sockets, side by side.

        The publish used to wait behind the local fan-out, so one local client
        that had stopped reading held the frame back from every other node for
        a whole send timeout. Neither waits on the other now: a Redis blip
        still never costs same-node subscribers their frame, and a slow local
        socket never costs the other nodes theirs.

        The publish is listed FIRST on purpose. ``gather`` starts its tasks in
        that order and the publisher takes a frame's place in its queue before
        its first await, so frames leave this process in the order they were
        broadcast (see ``RedisPublisher``).
        """
        published, delivered = await asyncio.gather(
            self._publish_redis(prefixed_channel, payload),
            deliver,
            return_exceptions=True,
        )
        if isinstance(delivered, BaseException):
            Log.warning(
                "Local fan-out failed on %s: %s",
                label,
                delivered,
                category="cara.broadcasting",
            )
        if isinstance(published, BaseException):
            Log.debug(
                "Redis publish failed for %s (local delivery is unaffected): %s",
                label,
                published,
                category="cara.broadcasting",
            )

    # ------------------------------------------------------------------
    # broadcast_to_user — cross-process via per-user Redis channel.
    # ------------------------------------------------------------------
    async def broadcast_to_user(
        self,
        user_id: str,
        event: str,
        data: dict[str, Any],
        *,
        except_socket_id: str | None = None,
    ) -> None:
        # Encode, then publish and deliver side by side — same reasoning as
        # ``_broadcast_one``.
        payload = json_dumps(
            {
                "event": event,
                "user_id": user_id,
                "data": data,
                "_node_id": self._node_id,
                "_except_socket_id": except_socket_id,
            },
        )
        await self._publish_and_deliver(
            self._prefixed(f"{_USER_CHANNEL_PREFIX}{user_id}"),
            payload,
            self.broadcast_to_user_local(
                user_id, event, data, except_socket_id=except_socket_id
            ),
            label=f"user:{user_id}",
        )

    # ------------------------------------------------------------------
    # Connection lifecycle (with cross-process per-user channel
    # subscription)
    # ------------------------------------------------------------------
    async def add_connection(
        self,
        connection_id: str,
        websocket: Any,
        user_id: str | None = None,
        metadata: dict[str, Any] | None = None,
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
    # The subscriber side (who owns the pubsub connection, the reconnect and
    # its watchdog) lives in ``_RedisListener``; bound here so callers and
    # tests keep talking to one object.
    _ensure_redis_subscription = _RedisListener._ensure_redis_subscription
    _drop_redis_subscription = _RedisListener._drop_redis_subscription
    _listener_loop = _RedisListener._listener_loop
    _subscribe_until_settled = _RedisListener._subscribe_until_settled
    _read_until_silent = _RedisListener._read_until_silent
    _retire_pubsub = _RedisListener._retire_pubsub

    def _enqueue_pubsub_message(self, message: dict[str, Any]) -> None:
        """Hand a frame to its channel's lane without waiting on any socket.

        The listener used to await each frame's whole fan-out before reading
        the next, so one client that stopped reading held every workspace's
        stream for a send timeout per frame. A lane per pubsub channel keeps a
        channel's own frames in order and shares nothing between channels.
        Past the lane's capacity the OLDEST frame goes: a frame says something
        moved, and the newest is the one worth having.
        """
        channel = message.get("channel")
        lane = self._lanes.get(channel)
        if lane is None:
            lane = deque(maxlen=_LANE_CAPACITY)
            self._lanes[channel] = lane
            task = asyncio.create_task(self._drain_lane(channel, lane))
            self._lane_tasks.add(task)
            task.add_done_callback(self._lane_tasks.discard)
        elif len(lane) == _LANE_CAPACITY and channel not in self._shedding_lanes:
            self._shedding_lanes.add(channel)
            Log.warning(
                "Realtime lane %s is full; dropping its oldest frames",
                channel,
                category="cara.broadcasting",
            )
        lane.append(message)

    async def _drain_lane(self, channel: Any, lane: deque[dict[str, Any]]) -> None:
        """Deliver one channel's frames in order, until none are left."""
        try:
            while lane:
                try:
                    await self._dispatch_pubsub_message(lane.popleft())
                except Exception as e:
                    Log.warning(
                        "Delivering a pubsub frame on %s failed: %s",
                        channel,
                        e,
                        category="cara.broadcasting",
                    )
        finally:
            # No await between the empty check and here, so a frame can't land
            # in a lane nobody is draining.
            self._lanes.pop(channel, None)
            self._shedding_lanes.discard(channel)

    async def _dispatch_pubsub_message(self, message: dict[str, Any]) -> None:
        """Decode an incoming pubsub frame and deliver it locally.

        Skips frames originated by this node (``broadcast`` serves its
        own local subscribers itself) and routes per-user
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
                "Discarding non-JSON pubsub frame on %s",
                channel_raw,
                category="cara.broadcasting",
            )
            return

        if not isinstance(payload, dict):
            return
        if payload.get("_node_id") == self._node_id:
            return  # Echo of our own publish — ``broadcast`` delivers locally.

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
    # Cleanup — application shutdown hook.
    # ------------------------------------------------------------------
    async def cleanup(self) -> None:
        if self._listener_task and not self._listener_task.done():
            self._listener_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._listener_task

        for task in list(self._lane_tasks):
            task.cancel()
        await asyncio.gather(*self._lane_tasks, return_exceptions=True)
        self._lane_tasks.clear()
        self._lanes.clear()

        if self._listener_pubsub is not None:
            with contextlib.suppress(
                OSError, RuntimeError, AttributeError, ConnectionError
            ):
                await self._listener_pubsub.aclose()
            self._listener_pubsub = None

        for client in list(self._redis_clients.values()):
            with contextlib.suppress(
                OSError, RuntimeError, AttributeError, ConnectionError
            ):
                await client.aclose()
        self._redis_clients.clear()

        for pool in list(self._redis_pools.values()):
            with contextlib.suppress(
                OSError, RuntimeError, AttributeError, ConnectionError
            ):
                await pool.disconnect()
        self._redis_pools.clear()
        self._redis_loop_refs.clear()

        self._publisher.close()

        await super().cleanup()
