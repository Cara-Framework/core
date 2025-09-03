from typing import Callable

from cara.facades import Log
from cara.middleware import Middleware
from cara.websocket import Socket


class LogWSRequests(Middleware):
    async def handle(self, socket: Socket, next: Callable):
        # IP ve port
        client = socket.scope.get("client")
        if client and isinstance(client, (tuple, list)) and len(client) == 2:
            ip, port = client
        else:
            ip, port = "-", "-"

        # Path
        path = socket.path

        # Log the WebSocket connection
        message = f'{ip}:{port} - "WS CONNECT {path}"'
        Log.debug(message, category="cara.websocket.connections")

        return await next(socket)
