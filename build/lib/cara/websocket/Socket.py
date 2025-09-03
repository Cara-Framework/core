"""
WebSocket Socket Object for the Cara framework.

This module provides the Socket class, encapsulating WebSocket connection data and utility methods for
WebSocket handling, mirroring the HTTP Request API with Laravel-style patterns.
"""

import json
import time
import uuid
from typing import Any, Dict, Optional
from urllib.parse import parse_qs

from cara.exceptions.types.websocket import WebSocketException
from cara.facades import Broadcast, Log


class Socket:
    """
    WebSocket Socket object for ASGI-based APIs.

    Features:
    - Route and parameter handling
    - Message sending/receiving
    - Connection lifecycle management
    - User authentication support
    - Query parameter parsing
    - JSON message handling
    - Error handling with proper codes
    """

    def __init__(self, application, scope: Dict[str, Any], receive: Any, send: Any):
        """Initialize WebSocket socket with ASGI scope."""
        self.application = application
        self.scope: Dict[str, Any] = scope
        self.receive = receive
        self.send = send

        self.route = None
        self.params: Dict[str, Any] = {}
        self._user: Any = None
        self._socket_id = str(uuid.uuid4())
        self._ws_connected = False
        self._closed = False
        self._error: Optional[str] = None
        self._close_code: Optional[int] = None

    def load(self, route=None, params=None) -> "Socket":
        """Initialize socket data after routing is matched."""
        if route:
            self.route = route
        if params:
            self.params = params
        return self

    def set_route(self, route) -> "Socket":
        """Set the matched route for this socket."""
        self.route = route
        return self

    def load_params(self, params: Dict[str, Any]) -> "Socket":
        """Load route parameters into socket."""
        self.params = params
        return self

    @property
    def path(self) -> str:
        """Return WebSocket path."""
        return self.scope.get("path", "/")

    @property
    def query_params(self) -> Dict[str, Any]:
        """Return parsed query parameters as a dict."""
        raw_qs = self.scope.get("query_string", b"").decode()
        parsed = parse_qs(raw_qs)
        # Convert single-item lists to single values
        return {k: v[0] if len(v) == 1 else v for k, v in parsed.items()}

    def param(self, name: str, default: Any = "") -> Any:
        """Retrieve a named route parameter."""
        return self.params.get(name, default)

    def set_user(self, user: Any) -> "Socket":
        """Set authenticated user object."""
        self._user = user
        return self

    def user(self) -> Any:
        """Return authenticated user, if set."""
        return self._user

    @property
    def socket_id(self) -> str:
        """Return unique socket ID."""
        return self._socket_id

    @property
    def id(self) -> str:
        """Alias for socket_id."""
        return self._socket_id

    @property
    def closed(self) -> bool:
        """Return whether the WebSocket connection is closed."""
        return self._closed

    @property
    def error(self) -> Optional[str]:
        """Return last error message if any."""
        return self._error

    @property
    def close_code(self) -> Optional[int]:
        """Return close code if connection was closed."""
        return self._close_code

    @property
    def is_connected(self) -> bool:
        """Return whether the WebSocket connection is active."""
        return self._ws_connected and not self._closed

    async def accept(self, subprotocol: Optional[str] = None) -> None:
        """
        Send the websocket.accept event.

        Args:
            subprotocol: Optional subprotocol to accept

        Raises:
            WebSocketException: If connection is already accepted or closed
        """
        if self._ws_connected:
            raise WebSocketException("WebSocket already accepted", 4003)
        if self._closed:
            raise WebSocketException("WebSocket already closed", 4003)

        msg: Dict[str, Any] = {"type": "websocket.accept"}
        if subprotocol is not None:
            msg["subprotocol"] = subprotocol

        try:
            await self.send(msg)
            self._ws_connected = True
        except Exception as e:
            raise WebSocketException(f"Failed to accept WebSocket: {e}", 4002)

    async def send_text(self, data: str) -> None:
        """
        Send a text frame over the WebSocket.

        Args:
            data: Text data to send

        Raises:
            WebSocketException: If connection is not accepted or is closed
        """
        if not self._ws_connected:
            raise WebSocketException("WebSocket not accepted", 4003)
        if self._closed:
            raise WebSocketException("WebSocket closed", 4000)

        try:
            await self.send({"type": "websocket.send", "text": data})
        except Exception as e:
            raise WebSocketException(f"Failed to send text: {e}", 4002)

    async def send_json(self, obj: Any) -> None:
        """
        Serialize object to JSON and send as a text frame.

        Args:
            obj: Object to serialize and send

        Raises:
            WebSocketException: If connection is not accepted or is closed
            json.JSONDecodeError: If object cannot be serialized
        """
        try:
            payload = json.dumps(obj)
        except Exception as e:
            raise WebSocketException(f"Failed to serialize JSON: {e}", 4009)

        await self.send_text(payload)

    async def close(self, code: int = 1000, reason: str = "") -> None:
        """
        Close the WebSocket connection.

        Args:
            code: Close status code
            reason: Optional close reason

        Raises:
            WebSocketException: If connection is already closed
        """
        if self._closed:
            return

        msg = {"type": "websocket.close", "code": code}
        if reason:
            msg["reason"] = reason

        try:
            await self.send(msg)
            self._closed = True
            self._close_code = code
            self._error = reason if reason else None
        except Exception as e:
            raise WebSocketException(f"Failed to close WebSocket: {e}", 4002)

    async def receive_message(self) -> Dict[str, Any]:
        """
        Receive the next ASGI message.

        Returns:
            Dict containing the message data

        Raises:
            WebSocketException: If connection is not accepted or is closed
        """
        if not self._ws_connected:
            raise WebSocketException("WebSocket not accepted", 4003)
        if self._closed:
            raise WebSocketException("WebSocket closed", 4000)

        try:
            message = await self.receive()
            Log.debug(
                f"Raw WebSocket message received: {message}", category="cara.websocket"
            )

            # Mark this connection as active to prevent cleanup
            try:
                connection_id = f"ws_{self._socket_id}"
                if hasattr(Broadcast, "driver"):
                    driver = Broadcast.driver()
                    if hasattr(driver, "connection_metadata"):
                        meta = driver.connection_metadata.get(connection_id, {})
                        meta["last_activity"] = time.time()
                        driver.connection_metadata[connection_id] = meta
            except Exception as _:
                pass

            # Handle close messages
            if message.get("type") == "websocket.disconnect":
                self._closed = True
                self._close_code = message.get("code", 1000)
                return None

            # Handle ping messages
            if message.get("type") == "websocket.ping":
                await self.send({"type": "websocket.pong"})
                return {"type": "ping"}

            # Handle lifecycle messages
            if message.get("type") == "websocket.connect":
                return {"type": "connect"}

            if message.get("type") == "websocket.receive":
                return message

            return message
        except Exception as e:
            raise WebSocketException(f"Failed to receive message: {e}", 4002)

    async def receive_text(self) -> str:
        """
        Receive a text frame.

        Returns:
            The text content of the message

        Raises:
            WebSocketException: If message is not a text frame
        """
        message = await self.receive_message()
        if not message:
            return ""

        # Handle lifecycle messages
        if message.get("type") == "connect":
            return json.dumps({"type": "connect"})

        if message.get("type") == "ping":
            return json.dumps({"type": "ping"})

        # Handle both text and binary messages
        text = message.get("text")
        if text is not None:
            return text

        # Try to decode binary data as text
        bytes_data = message.get("bytes")
        if bytes_data is not None:
            try:
                return bytes_data.decode("utf-8")
            except UnicodeDecodeError:
                raise WebSocketException(
                    "Received binary data that cannot be decoded as text", 4009
                )

        Log.debug(f"Message type: {message.get('type')}", category="cara.websocket")
        raise WebSocketException("Received message without text or binary data", 4009)

    async def receive_json(self) -> Any:
        """
        Receive a JSON message.

        Returns:
            The parsed JSON data

        Raises:
            WebSocketException: If message is not valid JSON
        """
        text = await self.receive_text()
        if not text:
            return None

        try:
            return json.loads(text)
        except json.JSONDecodeError as e:
            raise WebSocketException(f"Invalid JSON message: {e}", 4009)

    async def subscribe_channel(self, channel: str) -> bool:
        """Subscribe this socket to a broadcasting channel with duplicate prevention."""
        connection_id = f"ws_{self._socket_id}"

        # Ensure connection is registered
        if not hasattr(self, "_connection_registered"):
            user_id = None
            if self._user:
                user_id = getattr(self._user, "user_id", None)
            # Added debug log before registering connection
            try:
                from cara.facades import Broadcast as _Broadcast

                driver_name = _Broadcast.driver().__class__.__name__
            except Exception:
                driver_name = "<unknown>"
            Log.info(
                f"WS subscribe_channel: registering connection {connection_id} (user_id={user_id}) on driver={driver_name} for channel={channel}",
                category="cara.websocket.connections",
            )
            await Broadcast.add_connection(connection_id, self, user_id)
            self._connection_registered = True

        # Check if already subscribed
        current_channels = getattr(self, "_subscribed_channels", set())
        if channel in current_channels:
            Log.debug(
                f"Socket {self._socket_id} already subscribed to {channel}",
                category="cara.websocket",
            )
            return True

        success = await Broadcast.subscribe(connection_id, channel)
        if success:
            if not hasattr(self, "_subscribed_channels"):
                self._subscribed_channels = set()
            self._subscribed_channels.add(channel)
            try:
                subscribers = Broadcast.get_channel_subscribers(channel)
                Log.debug(
                    f"WS subscribed: channel={channel}, local_subscriber_count={len(subscribers)} (conn={connection_id})",
                    category="cara.websocket.connections",
                )
            except Exception as e:
                Log.debug(
                    f"WS subscribed: could not fetch subscribers for {channel}: {e}"
                )
            Log.debug(
                f"Socket {self._socket_id} subscribed to {channel}",
                category="cara.websocket",
            )
        return success

    async def unsubscribe_channel(self, channel: str) -> bool:
        """Unsubscribe this socket from a broadcasting channel."""
        connection_id = f"ws_{self._socket_id}"
        success = await Broadcast.unsubscribe(connection_id, channel)

        if success and hasattr(self, "_subscribed_channels"):
            self._subscribed_channels.discard(channel)
            Log.debug(
                f"Socket {self._socket_id} unsubscribed from {channel}",
                category="cara.websocket",
            )
        return success

    async def handle_subscription_request(self, data: dict) -> dict:
        """Handle subscribe/unsubscribe requests with better error handling."""
        if not isinstance(data, dict):
            return {"error": "Invalid request format"}

        action = data.get("action")
        channel = data.get("channel")

        if not action or not channel:
            return {"error": "Missing action or channel"}

        # Rate limiting: max subscriptions per connection (from config)
        if action == "subscribe":
            current_channels = getattr(self, "_subscribed_channels", set())
            max_subscriptions = 25  # Default fallback

            # Try to get from config
            try:
                from cara.facades import Config

                broadcasting_config = Config.get("broadcasting", {})
                websocket_config = broadcasting_config.get("WEBSOCKET", {})
                max_subscriptions = websocket_config.get(
                    "max_subscriptions_per_connection", 25
                )
            except Exception:
                pass  # Use default

            if len(current_channels) >= max_subscriptions:
                return {
                    "error": f"Maximum subscriptions per connection exceeded ({max_subscriptions})"
                }

            success = await self.subscribe_channel(channel)
            return {
                "event": "subscribed" if success else "subscription_failed",
                "channel": channel,
            }
        elif action == "unsubscribe":
            success = await self.unsubscribe_channel(channel)
            return {"event": "unsubscribed", "channel": channel}
        elif action == "pong":
            # Handle pong response to ping
            return {"event": "pong_received"}
        else:
            return {"error": f"Unknown action: {action}"}

    async def cleanup_broadcasting(self):
        """Clean up broadcasting connections for this socket with proper cleanup."""
        try:
            connection_id = f"ws_{self._socket_id}"

            # Unsubscribe from all channels first
            if hasattr(self, "_subscribed_channels"):
                for channel in self._subscribed_channels.copy():
                    await self.unsubscribe_channel(channel)

            # Remove connection from broadcaster
            await Broadcast.remove_connection(connection_id)

            # Clean up local state
            if hasattr(self, "_subscribed_channels"):
                self._subscribed_channels.clear()
            if hasattr(self, "_connection_registered"):
                delattr(self, "_connection_registered")

            Log.debug(
                f"Cleaned up broadcasting for socket {self._socket_id}",
                category="cara.websocket",
            )
        except Exception as e:
            Log.error(
                f"Error cleaning up broadcasting for socket {self._socket_id}: {e}",
                category="cara.websocket",
            )
