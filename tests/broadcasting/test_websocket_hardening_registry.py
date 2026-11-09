"""Every way the registry lets go of a connection ends the connection."""

from __future__ import annotations

import asyncio
import time
from importlib import import_module
from typing import Any

import pytest

from cara.broadcasting.ConnectionManager import ConnectionManager
from cara.broadcasting.drivers.RedisBroadcaster import RedisBroadcaster
from cara.websocket.Socket import Socket

retirement_module = import_module("cara.broadcasting.SocketRetirement")


def _settings(**websocket: Any) -> dict:
    return {
        "websocket": {
            "heartbeat_interval": 0,
            "cleanup_interval": 3600,
            "max_connections_per_user": 1,
            "broadcast_send_timeout": 0.05,
            **websocket,
        },
        "connection": {"prefix": "p:"},
    }


def _socket(sent: list[dict] | None = None, *, wedged: bool = False) -> Socket:
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


async def _until(predicate, timeout: float = 1.0) -> None:
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.005)


@pytest.mark.asyncio
async def test_each_drop_tells_the_client_why_with_its_own_code() -> None:
    manager = ConnectionManager(_settings())
    evicted = _socket()
    reaped = _socket(wedged=True)
    try:
        await manager.add_connection("evicted", evicted, "7", {"socket_id": "a"})
        await manager.add_connection("next", _socket(), "7", {"socket_id": "b"})
        await manager.add_connection("reaped", reaped, "8", {"socket_id": "c"})
        await manager.subscribe("reaped", "private-x")
        await manager.broadcast_to_channel("private-x", "e", {})

        assert evicted.close_code == 4012
        assert reaped.close_code == 1013
    finally:
        await manager.cleanup()


@pytest.mark.asyncio
async def test_a_wedged_socket_cannot_park_the_heartbeat() -> None:
    manager = ConnectionManager(_settings(max_connections_per_user=5))
    manager.heartbeat_interval = 0.01
    wedged = _socket(wedged=True)
    try:
        await manager.add_connection("w", wedged, "7", {"socket_id": "a"})
        task = manager._heartbeat_tasks["w"]

        await _until(lambda: task.done())

        assert not task.cancelled()
        assert "w" not in manager.connections
        assert wedged.close_code == 1013
    finally:
        await manager.cleanup()


@pytest.mark.asyncio
async def test_an_idle_sweep_closes_what_it_forgets() -> None:
    manager = ConnectionManager(_settings(max_connections_per_user=5, idle_timeout=1))
    idle = _socket()
    try:
        await manager.add_connection("idle", idle, "7", {"socket_id": "a"})
        manager.connection_metadata["idle"]["last_activity"] = time.time() - 60

        await manager._sweep_idle_connections()

        assert "idle" not in manager.connections
        assert idle.closed is True
    finally:
        await manager.cleanup()


@pytest.mark.asyncio
async def test_a_slow_close_is_left_to_finish_rather_than_cancelled(monkeypatch) -> None:
    """The server's own close timeout is what drops a peer that stopped
    reading; cancelling its close at a bound of ours would skip it."""
    monkeypatch.setattr(retirement_module, "_SETTLE_SECONDS", 0.01)
    finish = asyncio.Event()
    outcome: list[str] = []

    class _SlowClose:
        async def send_json(self, _message: dict) -> None:
            await asyncio.Event().wait()

        async def close(self, _code: int, _reason: str) -> None:
            try:
                await finish.wait()
                outcome.append("finished")
            except asyncio.CancelledError:
                outcome.append("cancelled")
                raise

    manager = ConnectionManager(_settings(max_connections_per_user=5))
    try:
        await manager.add_connection("slow", _SlowClose(), "7", {"socket_id": "a"})
        await manager.subscribe("slow", "private-x")

        await asyncio.wait_for(manager.broadcast_to_channel("private-x", "e", {}), 1)
        assert outcome == []

        finish.set()
        await _until(lambda: bool(outcome))
        assert outcome == ["finished"]
    finally:
        await manager.cleanup()


@pytest.mark.asyncio
async def test_an_object_that_cannot_be_closed_is_still_dropped() -> None:
    class _SendOnly:
        async def send_json(self, _message: dict) -> None:
            raise ConnectionError("Connection closed")

    manager = ConnectionManager(_settings(max_connections_per_user=5))
    try:
        await manager.add_connection("a", _SendOnly(), "7", {"socket_id": "a"})
        await manager.subscribe("a", "private-x")

        assert await manager.broadcast_to_channel("private-x", "e", {}) == 0
        assert "a" not in manager.connections
    finally:
        await manager.cleanup()


@pytest.mark.asyncio
async def test_a_heartbeat_drop_finishes_the_redis_unsubscribe() -> None:
    """``remove_connection`` used to cancel the heartbeat running it, so the
    pubsub unsubscribe behind the first real await never went out."""

    class _PubSub:
        def __init__(self) -> None:
            self.unsubscribed: list[str] = []

        async def unsubscribe(self, channel: str) -> None:
            await asyncio.sleep(0)
            self.unsubscribed.append(channel)

    broadcaster = RedisBroadcaster(
        _settings(max_connections_per_user=5), redis_url="redis://127.0.0.1:1/0"
    )
    broadcaster.heartbeat_interval = 0.01
    pubsub = _PubSub()
    broadcaster._listener_pubsub = pubsub
    broadcaster._redis_subscribed.update({"p:__user:7", "p:private-x"})

    async def _no_redis(_channel: str) -> None:
        return None

    broadcaster._ensure_redis_subscription = _no_redis  # type: ignore[method-assign]
    await broadcaster.add_connection("w", _socket(wedged=True), "7", {"socket_id": "a"})
    await broadcaster.subscribe("w", "private-x")
    try:
        task = broadcaster._heartbeat_tasks["w"]
        await _until(lambda: task.done())

        assert sorted(pubsub.unsubscribed) == ["p:__user:7", "p:private-x"]
    finally:
        broadcaster._listener_pubsub = None
        await broadcaster.cleanup()
