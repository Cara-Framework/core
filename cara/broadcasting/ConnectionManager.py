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
from typing import Any

from cara.facades import Log


class ConnectionManager:
    """In-process connection registry.

    Separated from the broadcasting driver interface so the Memory
    and Redis drivers can both inherit and add their own transport-
    specific behaviour without re-implementing the bookkeeping.
    """

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config

        # connection_id → websocket-like object (anything with .send_json).
        self.connections: dict[str, Any] = {}

        # Reverse index: which connection_ids are subscribed to a channel.
        # ``defaultdict(set)`` simplifies subscribe/unsubscribe but means
        # ``"foo" in self.channel_subscribers`` is True even for empty
        # entries — clean-up code below explicitly deletes empty keys.
        self.channel_subscribers: dict[str, set[str]] = defaultdict(set)

        # Forward index: which channels a connection is on. Lets us
        # find every channel a dropped connection needs to leave in O(1).
        self.connection_channels: dict[str, set[str]] = defaultdict(set)

        # Per-connection metadata: user_id, IP, connected_at, last_activity.
        # Kept separate from ``self.connections`` so we can preserve a
        # post-mortem trail (e.g. for telemetry) briefly after a
        # connection drops without leaking its websocket object.
        self.connection_metadata: dict[str, dict[str, Any]] = {}

        # User → set of connection_ids index. Lets ``broadcast_to_user``
        # avoid scanning ``connection_metadata`` on every call. Index is
        # maintained by ``add_connection`` / ``remove_connection``.
        self.user_connections: dict[str, set[str]] = defaultdict(set)

        # connection_id → public socket_id (UUID-like). Exposed to the
        # client as the ``connection.established`` payload so it can
        # echo it back via ``X-Socket-Id`` for skip-self broadcasts.
        self.socket_ids: dict[str, str] = {}

        # Reverse: socket_id → connection_id. Cheap lookup for the
        # except_socket_id filter at broadcast time.
        self.connections_by_socket_id: dict[str, str] = {}

        ws_cfg = config.get("websocket", {})
        self.max_connections: int = int(ws_cfg.get("max_connections", 1000))
        self.heartbeat_interval: int = int(ws_cfg.get("heartbeat_interval", 30))
        self.idle_timeout: float = float(ws_cfg.get("idle_timeout", 300))
        self.cleanup_interval: int = int(ws_cfg.get("cleanup_interval", 60))
        self.max_connections_per_user: int = int(
            ws_cfg.get("max_connections_per_user", 10)
        )

        # Background tasks. Indexed by connection_id so we can cancel
        # the heartbeat for a single dropped connection without
        # touching the others.
        self._heartbeat_tasks: dict[str, asyncio.Task] = {}
        self._cleanup_task: asyncio.Task | None = None

        # Serializes ``add_connection`` so the cap-check → evict → add span
        # (which straddles awaits: the evicted-tab notice + remove_connection)
        # can't race a concurrent connect for the SAME user into exceeding the
        # per-user / global cap. ``remove_connection`` stays lock-free
        # (idempotent) and is CALLED inside this lock, so it must not re-acquire
        # it. Connection setup is infrequent relative to messages, so
        # serialising it is cheap.
        self._registry_lock = asyncio.Lock()

        # Per-recipient send timeout for broadcast fan-out. A client whose TCP
        # send window is full would otherwise stall its ``send_json`` (and thus
        # the whole ``gather`` fan-out, and — on the Redis driver — the cross-pod
        # publish gated behind it) indefinitely. A send that overruns this bound
        # is treated as a dead connection and reaped.
        self._broadcast_send_timeout: float = float(
            ws_cfg.get("broadcast_send_timeout", 5.0)
        )

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------
    async def add_connection(
        self,
        connection_id: str,
        websocket: Any,
        user_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Register a new WebSocket connection.

        Refuses the connection (``ConnectionError``) when the global
        cap is exceeded. When the per-user cap is exceeded the OLDEST
        connection for that user is dropped, mirroring Laravel
        Echo's "kick the previous tab" behaviour.

        Serialized under ``_registry_lock``: the cap-check → evict → add span
        straddles awaits, so without the lock two concurrent connects for the
        same user both read ``len == cap``, both evict the same oldest tab, and
        both add — silently exceeding the cap.
        """
        async with self._registry_lock:
            await self._add_connection_locked(
                connection_id, websocket, user_id, metadata
            )

    async def _add_connection_locked(
        self,
        connection_id: str,
        websocket: Any,
        user_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        if connection_id in self.connections:
            # Idempotency: a re-add with the same id replaces the old
            # connection. Mirrors how a browser tab reload arrives at
            # the same conn_id under some ASGI servers.
            Log.debug("Connection %s re-added; replacing prior entry", connection_id, category='cara.broadcasting')
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
                Log.info("User %s hit per-user cap (%s); dropping oldest %s", user_id, self.max_connections_per_user, oldest, category='cara.broadcasting')
                # Tell the dropped tab WHY it lost the socket so the
                # client can show "you signed in elsewhere" instead of
                # silently retry-storming. Best-effort: a closed peer
                # just raises here and the broadcast-closed marker
                # filters the noise.
                evicted_ws = self.connections.get(oldest)
                if evicted_ws is not None:
                    try:
                        await evicted_ws.send_json(
                            {
                                "event": "connection.evicted",
                                "data": {
                                    "reason": "max_connections_per_user",
                                    "limit": self.max_connections_per_user,
                                },
                            }
                        )
                    except Exception as e:
                        if not _is_connection_closed_error(e):
                            Log.debug("Eviction notice to %s failed: %s", oldest, e, category='cara.broadcasting')
                await self.remove_connection(oldest)

        if len(self.connections) >= self.max_connections:
            Log.warning("Max connections (%s) reached, rejecting %s", self.max_connections, connection_id, category='cara.broadcasting')
            raise ConnectionError(
                f"Maximum connections ({self.max_connections}) exceeded"
            )

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

        Log.debug("Connection added: %s (user=%s, total=%s)", connection_id, user_id or '-', len(self.connections), category='cara.broadcasting')

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

        Log.debug("Connection removed: %s (remaining=%s)", connection_id, len(self.connections), category='cara.broadcasting')

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
        data: dict[str, Any],
        *,
        except_socket_id: str | None = None,
    ) -> int:
        """Deliver ``event`` to every local subscriber of ``channel``.

        ``except_socket_id`` skips the connection whose public socket
        id matches — the "don't echo back to sender" pattern.

        Returns the number of successful deliveries. Failed sends
        trigger an immediate ``remove_connection`` for the dead
        connection so it doesn't accumulate dead entries in the
        index.

        Sends fan out via ``asyncio.gather`` rather than a sequential
        ``await`` loop. The previous implementation made total
        broadcast latency = sum of per-send time, so a single
        slow/stuck client (head-of-line blocking) delayed every other
        subscriber on a popular channel; with 1k subscribers and a
        50 ms p50, that was 50 s end-to-end just to fan out.
        Concurrent dispatch caps end-to-end at the slowest single
        send.
        """
        # Snapshot the subscriber set BEFORE the first await so a
        # concurrent subscribe/unsubscribe / remove_connection can't
        # mutate the set we're iterating.
        subscribers = list(self.channel_subscribers.get(channel, set()))
        if not subscribers:
            return 0

        # Resolve the connection_id to skip once, outside the loop.
        skip_conn_id: str | None = None
        if except_socket_id:
            skip_conn_id = self.connections_by_socket_id.get(except_socket_id)

        message = {"event": event, "channel": channel, "data": data}
        return await self._fan_out_send(
            subscribers, message, skip_conn_id=skip_conn_id, label=channel
        )

    async def broadcast_to_user_local(
        self,
        user_id: str,
        event: str,
        data: dict[str, Any],
        *,
        except_socket_id: str | None = None,
    ) -> int:
        """Deliver to every LOCAL connection belonging to ``user_id``.

        Drivers that need cross-process delivery (Redis) extend this
        by also publishing to a per-user pubsub channel. The base
        implementation only knows about in-process connections.
        """
        connection_ids = list(self.user_connections.get(user_id, set()))
        if not connection_ids:
            return 0

        skip_conn_id: str | None = None
        if except_socket_id:
            skip_conn_id = self.connections_by_socket_id.get(except_socket_id)

        message = {"event": event, "data": data}
        return await self._fan_out_send(
            connection_ids, message, skip_conn_id=skip_conn_id, label=f"user:{user_id}"
        )

    async def _fan_out_send(
        self,
        connection_ids: list[str],
        message: dict[str, Any],
        *,
        skip_conn_id: str | None,
        label: str,
    ) -> int:
        """Concurrently dispatch ``message`` to every connection id.

        Shared between channel + user-channel fan-out. ``label`` is
        only used for diagnostics so the log line carries the channel
        / user id that fanned out.
        """
        # Pair each (connection_id, websocket) so we can map gather
        # results back to ids without re-looking-up after the await.
        targets: list[tuple] = []
        for connection_id in connection_ids:
            if skip_conn_id and connection_id == skip_conn_id:
                continue
            ws = self.connections.get(connection_id)
            if ws is None:
                # Stale entry — was removed between snapshot and now.
                continue
            targets.append((connection_id, ws))

        if not targets:
            return 0

        # ``return_exceptions=True`` so a single dead client never
        # tears down the whole fan-out — we collect failures and
        # remove them after. Each send is bounded by
        # ``_broadcast_send_timeout``: a client whose TCP send window is full
        # would otherwise stall its ``send_json`` indefinitely, and ``gather``
        # would not resolve until it did — holding the broadcast caller (and,
        # on the Redis driver, the cross-pod publish gated behind it) hostage to
        # the single slowest socket. A timed-out send is collected as a failure
        # and the connection is reaped below, same as any other dead client.
        results = await asyncio.gather(
            *(
                asyncio.wait_for(
                    ws.send_json(message), timeout=self._broadcast_send_timeout
                )
                for _, ws in targets
            ),
            return_exceptions=True,
        )

        delivered = 0
        failed: list[str] = []
        for (connection_id, _), result in zip(targets, results, strict=False):
            if isinstance(result, Exception):
                if _is_connection_closed_error(result):
                    Log.debug("Subscriber %s closed mid-send on %s", connection_id, label, category='cara.broadcasting')
                else:
                    Log.warning("Send to %s on %s failed: %s", connection_id, label, result, category='cara.broadcasting')
                failed.append(connection_id)
            else:
                delivered += 1

        # Remove dead connections in parallel too — a 100-subscriber
        # broadcast where 50 failed would otherwise serially walk
        # the cleanup, holding the broadcast caller for the duration.
        if failed:
            await asyncio.gather(
                *(self.remove_connection(cid) for cid in failed),
                return_exceptions=True,
            )

        if delivered:
            Log.debug("Delivered to %s/%s on %s", delivered, len(targets), label, category='cara.broadcasting')
        return delivered

    # ------------------------------------------------------------------
    # Read-only accessors
    # ------------------------------------------------------------------
    def get_channel_subscribers(self, channel: str) -> list[str]:
        return list(self.channel_subscribers.get(channel, set()))

    def get_connection_channels(self, connection_id: str) -> list[str]:
        return list(self.connection_channels.get(connection_id, set()))

    def get_user_connection_ids(self, user_id: str) -> list[str]:
        return list(self.user_connections.get(user_id, set()))

    def get_connection_count(self) -> int:
        return len(self.connections)

    def get_channel_count(self) -> int:
        return len(self.channel_subscribers)

    def get_socket_id(self, connection_id: str) -> str | None:
        return self.socket_ids.get(connection_id)

    def get_stats(self) -> dict[str, Any]:
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
                        Log.debug("Heartbeat: %s closed", connection_id, category='cara.broadcasting')
                    else:
                        Log.warning("Heartbeat to %s failed: %s", connection_id, e, category='cara.broadcasting')
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
            Log.error("Connection cleanup loop crashed: %s", e, category='cara.broadcasting', exc_info=True)

    async def _sweep_idle_connections(self) -> None:
        now = time.time()
        # Snapshot keys so removals during iteration don't error.
        for connection_id in list(self.connection_metadata.keys()):
            meta = self.connection_metadata.get(connection_id)
            if meta is None:
                continue
            last_activity = meta.get("last_activity")
            connected_at = meta.get("connected_at")
            last = (
                last_activity
                if last_activity is not None
                else (connected_at if connected_at is not None else 0)
            )
            if now - last > self.idle_timeout:
                Log.debug("Sweeping idle connection %s (last activity %.0fs ago)", connection_id, now - last, category='cara.broadcasting')
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
