"""WebSocket authentication-state cleanup at connection termination."""

from __future__ import annotations

from typing import Any

from cara.facades import Log
from cara.websocket import Socket

from ..Middleware import Middleware


class ResetWebSocketAuth(Middleware):
    """Clear per-connection and singleton-guard authentication caches."""

    async def handle(self, socket: Socket, next_fn: Any) -> Any:
        return await next_fn(socket)

    async def terminate(self, socket: Socket) -> None:
        try:
            socket.set_user(None)
            socket.jwt_claims = {}

            auth_manager = self.application.make("auth")
            if hasattr(auth_manager, "_user"):
                auth_manager._user = None
            guard_names = list(getattr(auth_manager, "guards", {}).keys()) or ["jwt"]
            for guard_name in guard_names:
                try:
                    guard = auth_manager.guard(guard_name)
                except Exception as exc:
                    Log.debug(
                        "WebSocket auth guard %s was unavailable during cleanup: %s",
                        guard_name,
                        exc,
                        category="cara.websocket",
                    )
                    continue
                for attribute in ("_user", "_token", "_last_payload"):
                    if hasattr(guard, attribute):
                        setattr(guard, attribute, None)
        except Exception as exc:
            Log.warning(
                "WebSocket auth cleanup failed: %s",
                exc,
                category="cara.websocket",
                exc_info=True,
            )
