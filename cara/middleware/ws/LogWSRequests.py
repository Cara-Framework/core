import time
from typing import Callable

from cara.facades import Log
from cara.middleware import Middleware
from cara.websocket import Socket


class LogWSRequests(Middleware):
    """Emit a visible line for every WebSocket lifecycle event.

    Mirrors the HTTP request log format so operators can see connect/disconnect
    events in the same server output as ``LogHttpRequests``.
    """

    async def handle(self, socket: Socket, next: Callable):
        client = socket.scope.get("client")
        if client and isinstance(client, (tuple, list)) and len(client) == 2:
            ip, port = client
        else:
            ip, port = "-", "-"

        path = socket.path
        started = time.perf_counter()
        # Routine connect/close are debug-level — per-connection traffic would
        # flood the log otherwise. Only abnormal closes are elevated.
        Log.debug(
            f"🔌 WS: {ip}:{port} -> CONNECT {path}",
            category="cara.websocket",
        )

        try:
            result = await next(socket)
        except Exception as e:
            elapsed = (time.perf_counter() - started) * 1000
            # Client-close race on send raises WebSocketException(4002) — benign.
            name = type(e).__name__
            code = getattr(e, "code", None)
            is_benign = name == "WebSocketException" and code == 4002
            msg = (
                f"🔌 WS: {ip}:{port} -> CLOSE {path} ✗ {name} "
                f"| {elapsed:.2f}ms"
            )
            if is_benign:
                Log.debug(msg, category="cara.websocket")
            else:
                Log.warning(msg, category="cara.websocket")
            raise
        else:
            elapsed = (time.perf_counter() - started) * 1000
            Log.debug(
                f"🔌 WS: {ip}:{port} -> CLOSE {path} ✓ | {elapsed:.2f}ms",
                category="cara.websocket",
            )
            return result
