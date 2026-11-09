"""A connection the registry drops has to be told, and has to end.

``ConnectionManager`` drops a connection in two places: the per-user cap and a
fan-out send that overran its bound. Both only forget the entry, and the live
run (2026-11-09) showed what that leaves behind: the socket stays open, its
controller and its 30-second authorization fence keep running, it gets no event
and no heartbeat ever again, and it no longer counts against any cap — one
account held 26 served sockets under a cap of 10.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest

from cara.broadcasting.ConnectionManager import ConnectionManager
from cara.websocket.Socket import Socket


def _manager(**websocket: Any) -> ConnectionManager:
    return ConnectionManager(
        {
            "websocket": {
                "heartbeat_interval": 0,
                "cleanup_interval": 3600,
                "max_connections_per_user": 1,
                "broadcast_send_timeout": 0.05,
                **websocket,
            }
        }
    )


def _socket(sent: list[dict] | None = None, *, wedged: bool = False) -> Socket:
    """A real ``Socket`` over a fake ASGI send; ``wedged`` never drains a frame."""
    frames = [] if sent is None else sent

    async def send(message: dict) -> None:
        if wedged and message["type"] == "websocket.send":
            await asyncio.Event().wait()
        frames.append(message)

    async def receive() -> dict:
        await asyncio.Event().wait()
        return {}

    socket = Socket(None, {"type": "websocket", "path": "/ws"}, receive, send)
    socket._ws_connected = True
    return socket


@pytest.mark.asyncio
async def test_a_tab_evicted_by_the_per_user_cap_is_closed() -> None:
    manager = _manager()
    sent: list[dict] = []
    oldest = _socket(sent)
    try:
        await manager.add_connection("oldest", oldest, "7", {"socket_id": "a"})
        await manager.add_connection("newest", _socket(), "7", {"socket_id": "b"})

        assert "oldest" not in manager.connections
        assert json.loads(sent[0]["text"])["event"] == "connection.evicted"
        assert oldest.closed is True
        assert [m for m in sent if m["type"] == "websocket.close"]
    finally:
        await manager.cleanup()


@pytest.mark.asyncio
async def test_a_socket_reaped_by_a_send_timeout_is_closed() -> None:
    manager = _manager(max_connections_per_user=5)
    wedged = _socket(wedged=True)
    try:
        await manager.add_connection("wedged", wedged, "7", {"socket_id": "a"})
        await manager.subscribe("wedged", "private-x")

        delivered = await manager.broadcast_to_channel("private-x", "e", {})

        assert delivered == 0
        assert "wedged" not in manager.connections
        assert wedged.closed is True
    finally:
        await manager.cleanup()


@pytest.mark.asyncio
async def test_one_wedged_tab_cannot_hold_every_other_handshake() -> None:
    """The eviction notice is the one unbounded send, and it runs under the
    process-wide ``_registry_lock``: every other connect queues behind it."""
    manager = _manager()
    try:
        await manager.add_connection(
            "stuck", _socket(wedged=True), "7", {"socket_id": "a"}
        )
        evicting = asyncio.create_task(
            manager.add_connection("next-tab", _socket(), "7", {"socket_id": "b"})
        )
        await asyncio.sleep(0.01)

        await asyncio.wait_for(
            manager.add_connection("other", _socket(), "8", {"socket_id": "c"}),
            timeout=1.0,
        )

        assert "other" in manager.connections
        await asyncio.wait_for(evicting, timeout=1.0)
    finally:
        for task in asyncio.all_tasks() - {asyncio.current_task()}:
            task.cancel()
        await manager.cleanup()
