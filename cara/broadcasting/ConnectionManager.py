"""
WebSocket Connection Manager — base for in-process broadcasting drivers.

Tracks live WebSocket connections, channel subscriptions, per-user
indexes, and last-activity metadata. The Memory and Redis drivers
both inherit from this class; Redis adds cross-process pub/sub on
top of the same in-process state.

Key invariants
--------------
* Every connection has a stable, opaque ``connection_id`` (chosen
  by the Socket layer, typically ``ws_<uuid>``) and a separate
  ``socket_id`` exposed to the client. ``socket_id`` is what the
  client sends back as ``X-Socket-Id`` for "don't echo back to me"
  semantics.
* All shared dict / set state is mutated only inside ``async`` methods
  on the same event loop, so cooperative scheduling between awaits is
  enough to prevent torn reads. Where a method iterates a collection
  and may block (``await ws.send_json(...)``), the collection is
  snapshotted first so concurrent ``subscribe`` / ``remove_connection``
  calls can't mutate it underneath us.
* Removing a connection drops it from every channel index and from
  the metadata dict. Heartbeat / cleanup tasks are cancelled so they
  don't keep the dead connection alive in memory.
"""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set

from cara.facades import Log


class ConnectionManager:
    """In-process connection registry.

    Separated from the broadcasting driver interface so the Memory
    and Redis drivers can both inherit and add their own transport-
    specific behaviour without re-implementing the bookkeeping.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config

        # connection_id → websocket-like object (anything with .send_json).
        self.connections: Dict[str, Any] = {}

        # Reverse index: which connection_ids are subscribed to a channel.
        # ``defaultdict(set)`` simplifies subscribe/unsubscribe but means
        # ``"foo" in self.channel_subscribers`` is True even for empty
        # entries — clean-up code below explicitly deletes empty keys.
        self.channel_subscribers: Dict[str, Set[str]] = defaultdict(set)

        # Forward index: which channels a connection is on. Lets us
        # find every channel a dropped connection needs to leave in O(1).
        self.connection_channels: Dict[str, Set[str]] = defaultdict(set)

        # Per-connection metadata: user_id, IP, connected_at, last_activity.
        # Kept separate from ``self.connections`` so we can preserve a
        # post-mortem trail (e.g. for telemetry) briefly after a
        # connection drops without leaking its websocket object.
        self.connection_metadata: Dict[str, Dict[str, Any]] = {}

        # User → set of connection_ids index. Lets ``broadcast_to_user``
        # avoid scanning ``connection_metadata`` on every call. Index is
        # maintained by ``add_connection`` / ``remove_connection``.
        self.user_connections: Dict[str, Set[str]] = defaultdict(set)

        # connection_id → public socket_id (UUID-like). Exposed to the
        # client as the ``connection.established`` payload so it can
        # echo it back via ``X-Socket-Id`` for skip-self broadcasts.
        self.socket_ids: Dict[str, str] = {}

        # Reverse: socket_id → connection_id. Cheap lookup for the
        # except_socket_id filter at broadcast time.
        self.connections_by_socket_id: Dict[str, str] = {}

        ws_cfg = config.get("websocket", {})
        self.max_connections: int = int(ws_cfg.get("max_connections", 1000))
        self.heartbeat_interval: int = int(ws_cfg.get("heartbeat_interval", 30))
        self.idle_timeout: float = float(ws_cfg.get("idle_timeout", 300))
        self.cleanup_interval: int = int(ws_cfg.get("cleanup_interval", 60))
        self.max_connections_per_user: int = int(ws_cfg.get("max_connections_per_user", 10))

        # Background tasks. Indexed by connection_id so we can cancel
        # the heartbeat for a single dropped connection without
        # touching the others.
        self._heartbeat_tasks: Dict[str, asyncio.Task] = {}
        self._cleanup_task: Optional[asyncio.Task] = None

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------
    async def add_connection(
        self,
        connection_id: str,
        websocket: Any,
        user_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Register a new WebSocket connection.

        Refuses the connection (``ConnectionError``) when the global
        cap is exceeded. When the per-user cap is exceeded the OLDEST
        connection for that user is dropped, mirroring Laravel
        Echo's "kick the previous tab" behaviour.
        """
        if connection_id in self.connections:
            # Idempotency: a re-add with the same id replaces the old
            # connection. Mirrors how a browser tab reload arrives at
            # the same conn_id under some ASGI servers.
            Log.debug(
                f"Connection {connection_id} re-added; replacing prior entry",
                category="cara.broadcasting",
            )
            await self.remove_connection(connection_id)

        if user_id and self.max_connections_per_user > 0:
            user_set = self.user_connections.get(user_id, set())
            if len(user_set) >= self.max_connections_per_user:
                # Pick the oldest by ``connected_at`` instead of relying
                # on dict insertion order, which gets jumbled after
                # repeated add/remove cycles.
                oldest = min(
                    user_set,
                    key=lambda cid: self.connection_metadata.get(cid, {}).get(
                        "connected_at", 0
                    ),
                )
                Log.info(
                    f"User {user_id} hit per-user cap "
                    f"({self.max_connections_per_user}); dropping oldest {oldest}",
                    category="cara.broadcasting",
                )
                await self.remove_connection(oldest)

        if len(self.connections) >= self.max_connections:
            Log.warning(
                f"Max connections ({self.max_connections}) reached, rejecting {connection_id}",
                category="cara.broadcasting",
            )
            raise ConnectionError(f"Maximum connections ({self.max_connections}) exceeded")

        now = time.time()
        self.connections[connection_id] = websocket

        # Pull socket_id out of metadata if the Socket layer provided one;
        # otherwise fall back to connection_id as the public id. The
        # Socket layer always sets it.
        socket_id = (metadata or {}).get("socket_id") or connection_id
        self.socket_ids[connection_id] = socket_id
        self.connections_by_socket_id[socket_id] = connection_id

        self.connection_metadata[connection_id] = {
            "user_id": user_id,
            "connected_at": now,
            "last_activity": now,
            "socket_id": socket_id,
            **{k: v for k, v in (metadata or {}).items() if k != "socket_id"},
        }

        if user_id:
            self.user_connections[user_id].add(connection_id)

        Log.debug(
            f"Connection added: {connection_id} (user={user_id or '-'}, "
            f"total={len(self.connections)})",
            category="cara.broadcasting",
        )

        if self.heartbeat_interval > 0:
            self._heartbeat_tasks[connection_id] = asyncio.create_task(
                self._heartbeat_loop(connection_id)
            )

        # First connection lazily starts the cleanup task. Avoids
        # idle background work in apps that don't actually use WS.
        if self._cleanup_task is None or self._cleanup_task.done():
            self._cleanup_task = asyncio.create_task(self._cleanup_loop())

    async def remove_connection(self, connection_id: str) -> None:
        """Remove a connection and clean up every index it appears in.

        Cleanup steps must happen in this order so a concurrent
        ``broadcast_to_channel`` iterating a snapshot never sees a
        partially-removed connection:
          1. Drop from every channel index.
          2. Cancel the heartbeat task.
          3. Drop the websocket object and the metadata.
        """
        if connection_id not in self.connections:
            return

        # Step 1 — leave every channel.
        channels = self.connection_channels.pop(connection_id, set()).copy()
        for channel in channels:
            subs = self.channel_subscribers.get(channel)
            if subs is not None:
                subs.discard(connection_id)
                if not subs:
                    # ``defaultdict(set)`` doesn't auto-prune empty
                    # sets; do it explicitly so empty channels don't
                    # linger as keys.
                    self.channel_subscribers.pop(channel, None)

        # Step 2 — cancel heartbeat.
        task = self._heartbeat_tasks.pop(connection_id, None)
        if task and not task.done():
            task.cancel()

        # Step 3 — drop the websocket + metadata + indexes.
        self.connections.pop(connection_id, None)
        meta = self.connection_metadata.pop(connection_id, None)
        socket_id = self.socket_ids.pop(connection_id, None)
        if socket_id:
            # Only remove if it still points at this connection_id —
            # defensive against a re-add that re-mapped the socket_id.
            if self.connections_by_socket_id.get(socket_id) == connection_id:
                self.connections_by_socket_id.pop(socket_id, None)

        if meta and meta.get("user_id"):
            user_set = self.user_connections.get(meta["user_id"])
            if user_set is not None:
                user_set.discard(connection_id)
                if not user_set:
                    self.user_connections.pop(meta["user_id"], None)

        Log.debug(
            f"Connection removed: {connection_id} (remaining={len(self.connections)})",
            category="cara.broadcasting",
        )

    # ------------------------------------------------------------------
    # Subscription management
    # ------------------------------------------------------------------
    async def subscribe(self, connection_id: str, channel: str) -> bool:
        """Attach ``connection_id`` to ``channel``.

        Returns ``False`` if the connection is unknown (caller should
        treat that as a no-op rather than raise — clients can race
        subscribe against disconnect).
        """
        if connection_id not in self.connections:
            return False
        self.channel_subscribers[channel].add(connection_id)
        self.connection_channels[connection_id].add(channel)
        return True

    async def unsubscribe(self, connection_id: str, channel: str) -> bool:
        """Detach ``connection_id`` from ``channel``. Always returns
        ``True`` — unsubscribing a non-subscribed connection is a
        no-op, not an error."""
        subs = self.channel_subscribers.get(channel)
        if subs is not None:
            subs.discard(connection_id)
            if not subs:
                self.channel_subscribers.pop(channel, None)
        chans = self.connection_channels.get(connection_id)
        if chans is not None:
            chans.discard(channel)
        return True

    # ------------------------------------------------------------------
    # Activity tracking — replaces Socket's old reach-around into the
    # driver's metadata dict (layer violation removed).
    # ------------------------------------------------------------------
    def touch(self, connection_id: str) -> None:
        """Mark the connection as having received a message right now."""
        meta = self.connection_metadata.get(connection_id)
        if meta is not None:
            meta["last_activity"] = time.time()

    # ------------------------------------------------------------------
    # Broadcasting (in-process). Cross-process broadcast is implemented
    # by the Redis driver on top of these.
    # ------------------------------------------------------------------
    async def broadcast_to_channel(
        self,
        channel: str,
        event: str,
        data: Dict[str, Any],
        *,
        except_socket_id: Optional[str] = None,
    ) -> int:
        """Deliver ``event`` to every local subscriber of ``channel``.

        ``except_socket_id`` skips the connection whose public socket
        id matches — the "don't echo back to sender" pattern.

        Returns the number of successful deliveries. Failed sends
        trigger an immediate ``remove_connection`` for the dead
        connection so it doesn't accumulate dead entries in the
        index.
        """
        # Snapshot the subscriber set BEFORE the first await so a
        # concurrent subscribe/unsubscribe / remove_connection can't
        # mutate the set we're iterating.
        subscribers = list(self.channel_subscribers.get(channel, set()))
        if not subscribers:
            return 0

        # Resolve the connection_id to skip once, outside the loop.
        skip_conn_id: Optional[str] = None
        if except_socket_id:
            skip_conn_id = self.connections_by_socket_id.get(except_socket_id)

        message = {"event": event, "channel": channel, "data": data}
        delivered = 0
        failed: List[str] = []

        for connection_id in subscribers:
            if skip_conn_id and connection_id == skip_conn_id:
                continue
            ws = self.connections.get(connection_id)
            if ws is None:
                # Stale entry — was removed between snapshot and now.
                continue
            try:
                await ws.send_json(message)
                delivered += 1
            except Exception as e:
                if _is_connection_closed_error(e):
                    Log.debug(
                        f"Subscriber {connection_id} closed mid-send on {channel}",
                        category="cara.broadcasting",
                    )
                else:
                    Log.warning(
                        f"Send to {connection_id} on {channel} failed: {e}",
                        category="cara.broadcasting",
                    )
                failed.append(connection_id)

        for connection_id in failed:
            await self.remove_connection(connection_id)

        if delivered:
            Log.debug(
                f"Delivered '{event}' to {delivered}/{len(subscribers)} on {channel}",
                category="cara.broadcasting",
            )
        return delivered

    async def broadcast_to_user_local(
        self,
        user_id: str,
        event: str,
        data: Dict[str, Any],
        *,
        except_socket_id: Optional[str] = None,
    ) -> int:
        """Deliver to every LOCAL connection belonging to ``user_id``.

        Drivers that need cross-process delivery (Redis) extend this
        by also publishing to a per-user pubsub channel. The base
        implementation only knows about in-process connections.
        """
        connection_ids = list(self.user_connections.get(user_id, set()))
        if not connection_ids:
            return 0

        skip_conn_id: Optional[str] = None
        if except_socket_id:
            skip_conn_id = self.connections_by_socket_id.get(except_socket_id)

        message = {"event": event, "data": data}
        delivered = 0
        failed: List[str] = []
        for connection_id in connection_ids:
            if skip_conn_id and connection_id == skip_conn_id:
                continue
            ws = self.connections.get(connection_id)
            if ws is None:
                continue
            try:
                await ws.send_json(message)
                delivered += 1
            except Exception as e:
                if _is_connection_closed_error(e):
                    Log.debug(
                        f"User-channel send to {connection_id} closed mid-send",
                        category="cara.broadcasting",
                    )
                else:
                    Log.warning(
                        f"User-channel send to {connection_id} failed: {e}",
                        category="cara.broadcasting",
                    )
                failed.append(connection_id)

        for connection_id in failed:
            await self.remove_connection(connection_id)
        return delivered

    # ------------------------------------------------------------------
    # Read-only accessors
    # ------------------------------------------------------------------
    def get_channel_subscribers(self, channel: str) -> List[str]:
        return list(self.channel_subscribers.get(channel, set()))

    def get_connection_channels(self, connection_id: str) -> List[str]:
        return list(self.connection_channels.get(connection_id, set()))

    def get_user_connection_ids(self, user_id: str) -> List[str]:
        return list(self.user_connections.get(user_id, set()))

    def get_connection_count(self) -> int:
        return len(self.connections)

    def get_channel_count(self) -> int:
        return len(self.channel_subscribers)

    def get_socket_id(self, connection_id: str) -> Optional[str]:
        return self.socket_ids.get(connection_id)

    def get_stats(self) -> Dict[str, Any]:
        return {
            "total_connections": self.get_connection_count(),
            "total_channels": self.get_channel_count(),
            "total_users": len(self.user_connections),
            "channels": {
                channel: len(subs) for channel, subs in self.channel_subscribers.items()
            },
        }

    # ------------------------------------------------------------------
    # Background tasks
    # ------------------------------------------------------------------
    async def _heartbeat_loop(self, connection_id: str) -> None:
        """Periodic ping so dead connections surface within
        ``heartbeat_interval`` seconds instead of lingering until the
        next broadcast attempt fails."""
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
            # Normal shutdown path when ``remove_connection`` cancels us.
            raise

    async def _cleanup_loop(self) -> None:
        """Periodic idle-connection sweep. Removes any connection whose
        ``last_activity`` is older than ``idle_timeout`` seconds, and
        prunes orphan metadata for connections whose websocket object
        is already gone."""
        try:
            while True:
                await asyncio.sleep(self.cleanup_interval)
                await self._sweep_idle_connections()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            Log.error(
                f"Connection cleanup loop crashed: {e}",
                category="cara.broadcasting",
            )

    async def _sweep_idle_connections(self) -> None:
        now = time.time()
        # Snapshot keys so removals during iteration don't error.
        for connection_id in list(self.connection_metadata.keys()):
            meta = self.connection_metadata.get(connection_id)
            if meta is None:
                continue
            last = meta.get("last_activity") or meta.get("connected_at") or 0
            if now - last > self.idle_timeout:
                Log.debug(
                    f"Sweeping idle connection {connection_id} "
                    f"(last activity {now - last:.0f}s ago)",
                    category="cara.broadcasting",
                )
                await self.remove_connection(connection_id)

    async def cleanup(self) -> None:
        """Tear down everything. Called by the application during
        graceful shutdown so background tasks don't leak."""
        if self._cleanup_task and not self._cleanup_task.done():
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except (asyncio.CancelledError, Exception):
                pass
        for task in list(self._heartbeat_tasks.values()):
            if task and not task.done():
                task.cancel()
        self._heartbeat_tasks.clear()
        self.connections.clear()
        self.channel_subscribers.clear()
        self.connection_channels.clear()
        self.connection_metadata.clear()
        self.user_connections.clear()
        self.socket_ids.clear()
        self.connections_by_socket_id.clear()


# Surface-level pattern matching for "the client just dropped" errors.
# Centralised here so heartbeat / broadcast send paths share the same
# benign-error filter.
_CONNECTION_CLOSED_MARKERS = (
    "ConnectionClosed",
    "WebSocket is closed",
    "Connection closed",
    "ClientDisconnected",
    "Connection is closed",
    "websocket.close",
    "ASGI message",
)


def _is_connection_closed_error(exc: Exception) -> bool:
    msg = str(exc)
    return any(marker in msg for marker in _CONNECTION_CLOSED_MARKERS)
