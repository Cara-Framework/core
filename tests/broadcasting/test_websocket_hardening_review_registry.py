"""What the registry may wait on while it lets connections go."""

from __future__ import annotations

import asyncio
import time
from importlib import import_module
from typing import Any

import pytest

from cara.broadcasting.ConnectionManager import ConnectionManager
from cara.exceptions import WebSocketException
from cara.websocket.Socket import Socket

retirement_module = import_module("cara.broadcasting.SocketRetirement")


def _manager(**websocket: Any) -> ConnectionManager:
    return ConnectionManager(
        {
            "websocket": {
                "heartbeat_interval": 0,
                "cleanup_interval": 3600,
                "max_connections_per_user": 1,
                **websocket,
            }
        }
    )


def _wedged_socket() -> Socket:
    async def send(_message: dict) -> None:
        await asyncio.Event().wait()

    async def receive() -> dict:
        await asyncio.Event().wait()
        return {}

    socket = Socket(None, {"type": "websocket", "path": "/ws"}, receive, send)
    socket._ws_connected = True
    return socket


class _NeverCloses:
    async def send_json(self, _message: dict) -> None:
        return None

    async def close(self, _code: int, _reason: str) -> None:
        await asyncio.Event().wait()


@pytest.mark.asyncio
async def test_an_eviction_waits_on_its_tab_outside_the_registry_lock(
    monkeypatch,
) -> None:
    """A BOUNDED wait under the lock is still every handshake in the process
    waiting a send timeout, so the send bound must not be what lets this pass."""
    monkeypatch.setattr(retirement_module, "_SETTLE_SECONDS", 0.5)
    manager = _manager(broadcast_send_timeout=30)
    try:
        await manager.add_connection("stuck", _wedged_socket(), "7", {"socket_id": "a"})
        evicting = asyncio.create_task(
            manager.add_connection("next", _NeverCloses(), "7", {"socket_id": "b"})
        )
        await asyncio.sleep(0.01)

        started = time.monotonic()
        await manager.add_connection("other", _NeverCloses(), "8", {"socket_id": "c"})

        assert time.monotonic() - started < 0.25
        await asyncio.wait_for(evicting, timeout=1)
    finally:
        await manager.cleanup()


@pytest.mark.asyncio
async def test_a_sweep_settles_its_closes_together(monkeypatch) -> None:
    """Twenty peers that will not finish closing cost one settle, not twenty."""
    monkeypatch.setattr(retirement_module, "_SETTLE_SECONDS", 0.05)
    manager = _manager(max_connections_per_user=50, idle_timeout=1)
    try:
        for number in range(20):
            connection_id = f"c{number}"
            await manager.add_connection(
                connection_id, _NeverCloses(), "7", {"socket_id": connection_id}
            )
            manager.connection_metadata[connection_id]["last_activity"] = time.time() - 60

        started = time.monotonic()
        await manager._sweep_idle_connections()

        assert time.monotonic() - started < 0.5
        assert not manager.connections
    finally:
        await manager.cleanup()


def test_an_eviction_is_not_a_malformed_frame() -> None:
    """A client told to stop reconnecting on eviction must not also stop on
    every code the socket layer raises for a bad frame."""
    with pytest.raises(WebSocketException) as malformed:
        Socket._decode_json("{not json")

    assert malformed.value.code != retirement_module.SocketRetirement.CLOSE_EVICTED
