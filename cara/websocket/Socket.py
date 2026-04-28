"""
Socket — per-connection WebSocket wrapper, mirror of cara's HTTP Request.

Every accepted WebSocket connection is wrapped in one Socket object
that carries the matched route, parsed parameters, the authenticated
user (if any), and the read/write lifecycle. Controllers and
middleware receive the Socket and only the Socket — they never touch
the raw ASGI receive/send callables directly.

Wire protocol
-------------
On connect (after the auth middleware accepts), the framework sends::

    { "event": "connection.established",
      "data":  { "socket_id": "<uuid>" } }

The client should echo ``socket_id`` as the ``X-Socket-Id`` header on
HTTP requests that trigger broadcasts so the originating connection
isn't sent its own event back (the Laravel "broadcast()->toOthers()"
pattern).

Subscribe / unsubscribe / pong are all carried as JSON objects:

    { "action": "subscribe",   "channel": "private-user.42" }
    { "action": "unsubscribe", "channel": "deals" }
    { "action": "pong" }

Each subscribe goes through ``Broadcasting.authorize_subscription``
so private/presence channels honour the channel-auth registry.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from typing import Any, Dict, List, Optional, Set
from urllib.parse import parse_qs

from cara.exceptions.types.websocket import WebSocketException
from cara.facades import Broadcast, Log


class Socket:
    """ASGI WebSocket connection wrapper."""

    # Default per-connection idle timeout. Override per-instance with
    # ``socket.receive_timeout = N``.
    receive_timeout: int = 120

    def __init__(self, application: Any, scope: Dict[str, Any], receive: Any, send: Any) -> None:
        self.application = application
        self.scope: Dict[str, Any] = scope
        self._receive = receive
        self._send = send

        self.route: Any = None
        self.params: Dict[str, Any] = {}
        self._user: Any = None

        # ``connection_id`` is the internal identifier the broadcasting
        # driver uses as the dict key for this connection. ``socket_id``
        # is the public id we emit to the client and accept back via
        # the ``X-Socket-Id`` header for skip-self broadcasts.
        self._connection_id: str = f"ws_{uuid.uuid4().hex}"
        self._socket_id: str = uuid.uuid4().hex

        self._ws_connected: bool = False
        self._closed: bool = False
        self._error: Optional[str] = None
        self._close_code: Optional[int] = None

        self._subscribed_channels: Set[str] = set()
        self._connection_registered: bool = False

    # ------------------------------------------------------------------
    # Routing/state setup — called by the WebsocketConductor.
    # ------------------------------------------------------------------
    def load(self, route: Any = None, params: Optional[Dict[str, Any]] = None) -> "Socket":
        if route is not None:
            self.route = route
        if params is not None:
            self.params = params
        return self

    def set_route(self, route: Any) -> "Socket":
        self.route = route
        return self

    def load_params(self, params: Dict[str, Any]) -> "Socket":
        self.params = params
        return self

    # ------------------------------------------------------------------
    # Read-only request-style accessors.
    # ------------------------------------------------------------------
    @property
    def path(self) -> str:
        return self.scope.get("path", "/")

    @property
    def query_params(self) -> Dict[str, Any]:
        raw = self.scope.get("query_string", b"").decode()
        parsed = parse_qs(raw)
        return {k: (v[0] if len(v) == 1 else v) for k, v in parsed.items()}

    def param(self, name: str, default: Any = "") -> Any:
        return self.params.get(name, default)

    def header(self, name: str, default: Optional[str] = None) -> Optional[str]:
        """Look up a connection header (case-insensitive)."""
        for k, v in self.scope.get("headers", []):
            if k.decode().lower() == name.lower():
                return v.decode()
        return default

    def set_user(self, user: Any) -> "Socket":
        self._user = user
        return self

    def user(self) -> Any:
        return self._user

    @property
    def connection_id(self) -> str:
        """Driver-side connection identifier — usually internal."""
        return self._connection_id

    @property
    def socket_id(self) -> str:
        """Public connection id, echoed to the client and accepted via
        ``X-Socket-Id`` for skip-self broadcasts."""
        return self._socket_id

    @property
    def id(self) -> str:
        # Backwards-compatible alias for ``connection_id``.
        return self._connection_id

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def error(self) -> Optional[str]:
        return self._error

    @property
    def close_code(self) -> Optional[int]:
        return self._close_code

    @property
    def is_connected(self) -> bool:
        return self._ws_connected and not self._closed

    @property
    def subscribed_channels(self) -> Set[str]:
        """Read-only view of channels this socket is subscribed to."""
        return set(self._subscribed_channels)

    # ------------------------------------------------------------------
    # Connection lifecycle (ASGI verbs).
    # ------------------------------------------------------------------
    async def accept(self, subprotocol: Optional[str] = None) -> None:
        """Accept the incoming WebSocket handshake."""
        if self._ws_connected:
            raise WebSocketException("WebSocket already accepted", 4003)
        if self._closed:
            raise WebSocketException("WebSocket already closed", 4003)

        msg: Dict[str, Any] = {"type": "websocket.accept"}
        if subprotocol is not None:
            msg["subprotocol"] = subprotocol
        try:
            await self._send(msg)
            self._ws_connected = True
        except Exception as e:
            raise WebSocketException(f"Failed to accept WebSocket: {e}", 4002) from e

    async def send_text(self, data: str) -> None:
        if not self._ws_connected:
            raise WebSocketException("WebSocket not accepted", 4003)
        if self._closed:
            # Client already gone — silently discard. Subsequent reads
            # in the controller loop will see ``is_connected`` False
            # and exit cleanly.
            return
        try:
            await self._send({"type": "websocket.send", "text": data})
        except Exception:
            self._closed = True

    async def send_json(self, obj: Any) -> None:
        try:
            payload = json.dumps(obj, default=_json_default)
        except Exception as e:
            raise WebSocketException(f"Failed to serialize JSON: {e}", 4009) from e
        await self.send_text(payload)

    async def close(self, code: int = 1000, reason: str = "") -> None:
        if self._closed:
            return
        msg: Dict[str, Any] = {"type": "websocket.close", "code": code}
        if reason:
            msg["reason"] = reason
        try:
            await self._send(msg)
        finally:
            self._closed = True
            self._close_code = code
            self._error = reason or None

    async def receive_message(self) -> Optional[Dict[str, Any]]:
        """Read the next ASGI message.

        Returns ``None`` when the client disconnects. Pings are
        answered with a pong inline and a synthetic ``{"type": "ping"}``
        is returned so the caller's loop can ``continue``.
        """
        if not self._ws_connected:
            raise WebSocketException("WebSocket not accepted", 4003)
        if self._closed:
            raise WebSocketException("WebSocket closed", 4000)

        try:
            message = await asyncio.wait_for(self._receive(), timeout=self.receive_timeout)
        except asyncio.TimeoutError:
            self._closed = True
            raise WebSocketException(
                f"Client idle for {self.receive_timeout}s, closing", 4000
            )
        except Exception as e:
            raise WebSocketException(f"Failed to receive message: {e}", 4002) from e

        # Last-activity touch lives on the broadcasting driver — we
        # used to reach into the driver's metadata dict directly here,
        # which was a layer violation. Now we just call the driver's
        # ``touch`` helper.
        try:
            driver = Broadcast.driver()
            touch = getattr(driver, "touch", None)
            if callable(touch):
                touch(self._connection_id)
        except Exception:
            # Touching is purely informational — never fail receive over it.
            pass

        msg_type = message.get("type")
        if msg_type == "websocket.disconnect":
            self._closed = True
            self._close_code = message.get("code", 1000)
            return None
        if msg_type == "websocket.ping":
            await self._send({"type": "websocket.pong"})
            return {"type": "ping"}
        if msg_type == "websocket.connect":
            return {"type": "connect"}
        return message

    async def receive_text(self) -> str:
        message = await self.receive_message()
        if not message:
            return ""
        if message.get("type") in ("connect", "ping"):
            return json.dumps({"type": message["type"]})
        text = message.get("text")
        if text is not None:
            return text
        bytes_data = message.get("bytes")
        if bytes_data is not None:
            try:
                return bytes_data.decode("utf-8")
            except UnicodeDecodeError as e:
                raise WebSocketException(
                    "Received binary data that cannot be decoded as text", 4009
                ) from e
        raise WebSocketException("Received message without text or binary data", 4009)

    async def receive_json(self) -> Any:
        text = await self.receive_text()
        if not text:
            return None
        try:
            return json.loads(text)
        except json.JSONDecodeError as e:
            raise WebSocketException(f"Invalid JSON message: {e}", 4009) from e

    # ------------------------------------------------------------------
    # Broadcasting integration.
    # ------------------------------------------------------------------
    async def announce(self) -> None:
        """Send the ``connection.established`` frame so the client
        knows its socket id (and can echo it via ``X-Socket-Id`` to
        skip self in broadcasts).

        Idempotent — safe to call from controllers that want to
        guarantee the announcement landed before sending other frames.
        """
        if not self._connection_registered:
            user_id = self._resolve_user_id()
            await Broadcast.add_connection(
                self._connection_id,
                self,
                user_id,
                {"socket_id": self._socket_id},
            )
            self._connection_registered = True

        await self.send_json(
            {"event": "connection.established", "data": {"socket_id": self._socket_id}}
        )

    async def subscribe_channel(self, channel: str) -> bool:
        """Subscribe this connection to ``channel``.

        Channel-auth gating: ``private-`` / ``presence-`` channels
        consult the registered ``Broadcast.channel(...)`` callback.
        Public channels go through unconditionally. Returns ``False``
        and emits a ``subscription_denied`` frame on auth failure.
        """
        if not self._connection_registered:
            user_id = self._resolve_user_id()
            await Broadcast.add_connection(
                self._connection_id,
                self,
                user_id,
                {"socket_id": self._socket_id},
            )
            self._connection_registered = True

        if channel in self._subscribed_channels:
            return True

        try:
            allowed = await Broadcast.authorize_subscription(channel, self._user)
        except Exception as e:
            Log.warning(
                f"Channel auth callback raised for {channel}: {e}",
                category="cara.websocket",
            )
            allowed = False

        if not allowed:
            await self.send_json(
                {"event": "subscription_denied", "channel": channel, "reason": "unauthorized"}
            )
            return False

        success = await Broadcast.subscribe(self._connection_id, channel)
        if success:
            self._subscribed_channels.add(channel)
            # If auth returned a presence dict, broadcast it on the
            # presence channel so existing members see who joined.
            if isinstance(allowed, dict):
                try:
                    await Broadcast.broadcast(
                        channel, "presence.joined", {"user": allowed}
                    )
                except Exception as e:
                    Log.debug(
                        f"Presence join broadcast failed on {channel}: {e}",
                        category="cara.websocket",
                    )
        return success

    async def unsubscribe_channel(self, channel: str) -> bool:
        success = await Broadcast.unsubscribe(self._connection_id, channel)
        if success:
            self._subscribed_channels.discard(channel)
        return success

    async def handle_subscription_request(
        self, data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Process a client-sent ``{"action": ...}`` frame and return
        the response frame to be sent back."""
        if not isinstance(data, dict):
            return {"error": "Invalid request format"}

        action = data.get("action")
        channel = data.get("channel")

        if action == "pong":
            return {"event": "pong_received"}

        if not action or not channel:
            return {"error": "Missing action or channel"}

        if action == "subscribe":
            if len(self._subscribed_channels) >= self._max_subscriptions():
                return {
                    "error": (
                        f"Maximum subscriptions per connection exceeded "
                        f"({self._max_subscriptions()})"
                    )
                }
            success = await self.subscribe_channel(channel)
            return {
                "event": "subscribed" if success else "subscription_failed",
                "channel": channel,
            }
        if action == "unsubscribe":
            await self.unsubscribe_channel(channel)
            return {"event": "unsubscribed", "channel": channel}
        return {"error": f"Unknown action: {action}"}

    async def cleanup_broadcasting(self) -> None:
        """Idempotent broadcasting tear-down — leave every channel,
        drop the driver-side connection entry, clear local indices.

        Safe to call multiple times; subsequent calls are no-ops."""
        if not self._connection_registered and not self._subscribed_channels:
            return

        try:
            for channel in list(self._subscribed_channels):
                try:
                    await Broadcast.unsubscribe(self._connection_id, channel)
                except Exception as e:
                    Log.debug(
                        f"unsubscribe({channel}) during cleanup raised: {e}",
                        category="cara.websocket",
                    )
            self._subscribed_channels.clear()

            if self._connection_registered:
                try:
                    await Broadcast.remove_connection(self._connection_id)
                except Exception as e:
                    Log.debug(
                        f"remove_connection during cleanup raised: {e}",
                        category="cara.websocket",
                    )
                self._connection_registered = False
        except Exception as e:
            Log.error(
                f"Broadcasting cleanup for {self._connection_id} failed: {e}",
                category="cara.websocket",
            )

    # ------------------------------------------------------------------
    # Helpers.
    # ------------------------------------------------------------------
    def _resolve_user_id(self) -> Optional[str]:
        if self._user is None:
            return None
        for attr in ("id", "user_id"):
            val = getattr(self._user, attr, None)
            if val is not None:
                return str(val)
        return None

    def _max_subscriptions(self) -> int:
        try:
            from cara.facades import Config

            ws_cfg = Config.get("broadcasting", {}).get("WEBSOCKET", {})
            return int(ws_cfg.get("max_subscriptions_per_connection", 25))
        except Exception:
            return 25

    # Compatibility surface for code that still uses the old name.
    async def send(self, message: Dict[str, Any]) -> None:
        """Direct ASGI send. Use ``send_text`` / ``send_json`` /
        ``close`` instead — kept to avoid breaking custom middleware
        that expects the raw send."""
        await self._send(message)

    @property
    def receive(self) -> Any:
        """Direct ASGI receive callable. Same caveat as ``send``."""
        return self._receive


# ---------------------------------------------------------------------
# JSON encoder default — handles types stdlib json doesn't (Decimal,
# datetime). Centralised so every send_json call accepts the same set
# of inputs.
# ---------------------------------------------------------------------
def _json_default(value: Any) -> Any:
    from datetime import date, datetime
    from decimal import Decimal

    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


__all__ = ["Socket"]
