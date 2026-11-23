"""A channel fan-out encodes its frame once, and a bad payload reaps nobody.

Live (2026-11-23): every subscriber encoded the same frame for itself inside
``Socket.send_json``. A payload cara cannot encode therefore failed once per
subscriber, each failure looked like a dead socket, and every healthy tab on
the channel was closed with 1013 for the publisher's programming error. The
same loop made a 500-subscriber frame cost 500 encodes.

Real ``Socket`` objects over a recording ASGI ``send``: the defect lived
between the registry and the socket, so a fake socket would hide it.
"""

from __future__ import annotations

import importlib
import json
from types import SimpleNamespace

import pytest

from cara.broadcasting.drivers.MemoryBroadcaster import MemoryBroadcaster
from cara.broadcasting.drivers.RedisBroadcaster import RedisBroadcaster
from tests._money_wire import connected_socket

manager_module = importlib.import_module("cara.broadcasting.ConnectionManager")
# Where the fan-out, its encoder and its ``Log`` live.
fan_out_module = importlib.import_module("cara.broadcasting._FanOut")

_CONFIG = {
    "websocket": {
        "heartbeat_interval": 0,
        "cleanup_interval": 3600,
        "max_connections_per_user": 0,
    }
}


class _Order:
    """Something the wire encoder refuses."""


async def _tabs(driver, count: int) -> dict[str, tuple[object, list[dict]]]:
    tabs: dict[str, tuple[object, list[dict]]] = {}
    for index in range(count):
        sent: list[dict] = []
        socket = connected_socket(sent)
        name = f"tab-{index}"
        # The registry's own methods: no Redis subscription to stand up.
        await manager_module.ConnectionManager.add_connection(driver, name, socket)
        await manager_module.ConnectionManager.subscribe(driver, name, "orders")
        tabs[name] = (socket, sent)
    return tabs


@pytest.mark.asyncio
async def test_an_unencodable_payload_fails_one_broadcast_and_closes_nobody(
    monkeypatch,
) -> None:
    errors: list[tuple] = []
    quiet = lambda *_args, **_kwargs: None  # noqa: E731
    monkeypatch.setattr(
        fan_out_module,
        "Log",
        SimpleNamespace(
            error=lambda *args, **_kwargs: errors.append(args),
            warning=quiet,
            info=quiet,
            debug=quiet,
        ),
    )
    driver = MemoryBroadcaster(_CONFIG)
    tabs = await _tabs(driver, 3)
    try:
        await driver.broadcast("orders", "order.updated", {"order": _Order()})

        assert sorted(driver.connections) == ["tab-0", "tab-1", "tab-2"]
        assert sorted(driver.get_channel_subscribers("orders")) == sorted(tabs)
        for socket, sent in tabs.values():
            assert not socket.closed
            assert sent == []
        # Said once, with the event and the channel, not once per subscriber.
        assert len(errors) == 1
        assert errors[0][1:3] == ("order.updated", "orders")

        # The channel still works for the next, well-formed frame.
        assert await driver.broadcast_to_channel("orders", "order.paid", {"n": 1}) == 3
        for _socket, sent in tabs.values():
            assert json.loads(sent[-1]["text"])["event"] == "order.paid"
    finally:
        await driver.cleanup()


@pytest.mark.asyncio
async def test_the_redis_driver_raises_it_before_anyone_is_served() -> None:
    driver = RedisBroadcaster(_CONFIG, redis_url="redis://127.0.0.1:1/0")
    published: list[str] = []

    async def _publish_redis(_channel: str, payload: str) -> None:
        published.append(payload)

    driver._publish_redis = _publish_redis  # type: ignore[method-assign]
    tabs = await _tabs(driver, 3)
    try:
        with pytest.raises(TypeError, match="not JSON serializable"):
            await driver.broadcast("orders", "order.updated", {"order": _Order()})

        assert published == []
        assert sorted(driver.connections) == sorted(tabs)
        for socket, sent in tabs.values():
            assert not socket.closed
            assert sent == []
    finally:
        await driver.cleanup()


@pytest.mark.asyncio
async def test_a_frame_is_encoded_once_however_many_subscribers(monkeypatch) -> None:
    encodes: list[object] = []
    real = fan_out_module.json_dumps

    def _counting(obj, **kwargs):
        encodes.append(obj)
        return real(obj, **kwargs)

    # Both doors a fan-out frame could be encoded through.
    monkeypatch.setattr(fan_out_module, "json_dumps", _counting)
    socket_module = importlib.import_module("cara.websocket.Socket")
    monkeypatch.setattr(socket_module, "json_dumps", _counting)

    driver = MemoryBroadcaster({"websocket": {**_CONFIG["websocket"]}})
    tabs = await _tabs(driver, 40)
    try:
        delivered = await driver.broadcast_to_channel("orders", "order.paid", {"n": 1})

        assert delivered == 40
        assert len(encodes) == 1
        frames = {sent[-1]["text"] for _socket, sent in tabs.values()}
        assert len(frames) == 1
        assert json.loads(frames.pop()) == {
            "event": "order.paid",
            "channel": "orders",
            "data": {"n": 1},
        }
    finally:
        await driver.cleanup()


@pytest.mark.asyncio
async def test_a_socket_that_only_speaks_send_json_still_gets_the_message() -> None:
    class _JsonOnly:
        def __init__(self) -> None:
            self.frames: list[dict] = []

        async def send_json(self, message: dict) -> None:
            self.frames.append(message)

    driver = MemoryBroadcaster(_CONFIG)
    socket = _JsonOnly()
    await driver.add_connection("legacy", socket)
    await driver.subscribe("legacy", "orders")
    try:
        assert await driver.broadcast_to_channel("orders", "order.paid", {"n": 1}) == 1
        assert socket.frames == [
            {"event": "order.paid", "channel": "orders", "data": {"n": 1}}
        ]
    finally:
        await driver.cleanup()
