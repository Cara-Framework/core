from __future__ import annotations

from typing import Callable, List, Optional

from cara.configuration import config
from cara.exceptions.types.websocket import WebSocketException
from cara.facades import Log
from cara.middleware import Middleware
from cara.websocket import Socket


class Authenticate(Middleware):
    """JWT authentication middleware for WebSocket connections."""

    def __init__(self, application, guards: Optional[List[str]] = None):
        super().__init__(application)
        self.guards = guards or [
            application.make("auth").get_default_guard()  # usually "jwt"
        ]

    async def handle(self, socket: Socket, next: Callable):
        """Authenticate the WebSocket handshake using configured guards."""
        # Origin allowlist — defence-in-depth on top of the JWT check.
        # Browser WebSocket API doesn't let third-party JS set arbitrary
        # headers (so auth tokens can't normally be forged from another
        # origin), but a stolen token can still be replayed if the
        # middleware accepts every Origin. Read the allowlist from
        # config so it stays empty (= unrestricted, matches legacy
        # behaviour) until ops opts in.
        if not self._origin_is_allowed(socket):
            Log.warning(
                f"WebSocket origin rejected: {self._read_origin(socket)!r}",
                category="cara.websocket",
            )
            try:
                await socket.send({"type": "websocket.close", "code": 4003})
            except Exception:
                pass
            raise WebSocketException("Origin not allowed", 4003)

        try:
            if await self._authenticate_socket(socket):
                return await next(socket)
        except Exception as e:
            Log.error(f"WebSocket auth error: {e}", category="cara.websocket")
            # Don't try to send close message on error - just raise the exception
            raise WebSocketException("Unauthorized WebSocket", 4006) from e

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
                Log.warning(f"Guard {guard_name} failed: {e}", category="cara.websocket")
                continue
        return False

    @staticmethod
    def _read_origin(socket: Socket) -> str:
        try:
            for k, v in socket.scope.get("headers", []):
                if k == b"origin":
                    return v.decode("latin-1", errors="replace")
        except Exception:
            pass
        return ""

    def _origin_is_allowed(self, socket: Socket) -> bool:
        """Origin check — opt-in via ``broadcasting.allowed_origins`` /
        ``websocket.allowed_origins``. An empty / missing list means no
        check is performed (legacy permissive default). When a list is
        configured, an exact-match comparison is performed against the
        Origin header; missing Origin (non-browser client) is allowed
        because curl/Postman/etc. don't send it and there's no clean
        way to distinguish a malicious browser from a server-side client
        without UA fingerprinting."""
        try:
            allowed = config(
                "broadcasting.WEBSOCKET.allowed_origins",
                config("websocket.allowed_origins", None),
            ) or []
        except Exception:
            allowed = []
        if not allowed:
            return True
        origin = self._read_origin(socket)
        # Non-browser client — no Origin header. Pass.
        if not origin:
            return True
        return origin in allowed

    def _extract_token(self, socket: Socket) -> Optional[str]:
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
