from __future__ import annotations

import time
from collections.abc import Awaitable, Callable
from typing import Any

from cara.facades import Log
from cara.middleware import Middleware
from cara.support.Str import mask_ip
from cara.websocket import Socket


class LogWSRequests(Middleware):
    """Emit a visible line for every WebSocket lifecycle event.

    Mirrors the HTTP request log format so operators can see connect/disconnect
    events in the same server output as ``LogHttpRequests``.
    """

    async def handle(
        self, socket: Socket, next_fn: Callable[[Any], Awaitable[Any]]
    ) -> Any:
        client = socket.scope.get("client")
        if client and isinstance(client, (tuple, list)) and len(client) == 2:
            ip, port = client
        else:
            ip, port = "-", "-"

        masked = mask_ip(str(ip)) if ip != "-" else "-"
        path = socket.path
        started = time.perf_counter()
        # Routine connect/close are debug-level — per-connection traffic would
        # flood the log otherwise. Only abnormal closes are elevated.
        Log.debug("🔌 WS: %s:%s -> CONNECT %s", masked, port, path, category='cara.websocket')

        try:
            result = await next_fn(socket)
        except Exception as e:
            elapsed = (time.perf_counter() - started) * 1000
            # Client-close race on send raises WebSocketException(4002) — benign.
            # RouteNotFoundException = client connected to an unregistered /ws
            # path (e.g. a live-feed client pointed at this jobs process
            # instead of api/:8300). Not a server fault — debug, not warning.
            name = type(e).__name__
            code = getattr(e, "code", None)
            is_benign = (
                name == "WebSocketException" and code == 4002
            ) or name == "RouteNotFoundException"
            msg = f"🔌 WS: {masked}:{port} -> CLOSE {path} ✗ {name} | {elapsed:.2f}ms"
            if is_benign:
                Log.debug(msg, category="cara.websocket")
            else:
                Log.warning(msg, category="cara.websocket")
            raise
        else:
            elapsed = (time.perf_counter() - started) * 1000
            Log.debug("🔌 WS: %s:%s -> CLOSE %s ✓ | %.2fms", masked, port, path, elapsed, category='cara.websocket')
            return result
