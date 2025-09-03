from typing import Callable, List

from cara.exceptions.types.websocket import WebSocketException
from cara.facades import Log
from cara.middleware import Middleware
from cara.websocket import Socket


class Authenticate(Middleware):
    """JWT authentication middleware for WebSocket connections."""

    def __init__(self, application, guards: List[str] = None):
        super().__init__(application)
        self.guards = guards or [
            application.make("auth").get_default_guard()  # usually "jwt"
        ]

    async def handle(self, socket: Socket, next: Callable):
        """Authenticate the WebSocket handshake using configured guards."""
        try:
            if await self._authenticate_socket(socket):
                return await next(socket)
        except Exception as e:
            Log.error(f"WebSocket auth error: {e}", category="cara.websocket")
            # Don't try to send close message on error - just raise the exception
            raise WebSocketException("Unauthorized WebSocket", 4006)

        # If we get here authentication failed
        try:
            await socket.send({"type": "websocket.close", "code": 4006})
        except Exception as e:
            # Connection might already be closed, just log and continue
            Log.debug(
                f"Could not send close message to WebSocket: {e}",
                category="cara.websocket",
            )

        raise WebSocketException("Unauthorized WebSocket", 4006)

    async def _authenticate_socket(self, socket: Socket) -> bool:
        """Authenticate using token from header or query param."""
        token = self._extract_token(socket)
        if not token:
            Log.debug("WebSocket auth: no token found", category="cara.websocket")
            return False

        auth_manager = self.application.make("auth")
        # Try guards in order
        for guard_name in self.guards:
            guard = auth_manager.guard(guard_name)
            try:
                if hasattr(guard, "validate_token") and guard.validate_token(token):
                    # Set guard state for this connection
                    user = guard._resolve_user_from_token(token)  # type: ignore
                    if not user:
                        continue
                    guard._user = user  # type: ignore
                    socket._user = user  # attach to socket
                    Log.debug(
                        f"WebSocket auth succeeded via {guard_name} for user {user}",
                        category="cara.websocket",
                    )
                    return True
            except Exception as e:
                Log.debug(f"Guard {guard_name} failed: {e}", category="cara.websocket")
                continue
        return False

    def _extract_token(self, socket: Socket) -> str | None:
        """Extract JWT token from Authorization header or subprotocol."""
        headers = {k.decode(): v.decode() for k, v in socket.scope.get("headers", [])}
        token_val: str | None = None
        auth_header = headers.get("authorization")
        if auth_header and auth_header.lower().startswith("bearer "):
            token_val = auth_header[7:]

        # subprotocols
        if not token_val:
            for proto in socket.scope.get("subprotocols", []):
                if isinstance(proto, bytes):
                    proto = proto.decode()
                if proto.lower().startswith("bearer "):
                    token_val = proto[7:]
                    break

        # query string
        if not token_val:
            qs_raw = socket.scope.get("query_string", b"").decode()
            if qs_raw:
                from urllib.parse import parse_qs

                qs = parse_qs(qs_raw)
                token_val = (qs.get("token") or qs.get("access_token") or [None])[0]
        return token_val
