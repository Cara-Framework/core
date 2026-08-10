"""A broadcast must not round the money on its way between processes.

``RedisBroadcaster`` carried a private ``_SafeEncoder`` whose ``default``
was ``float(Decimal)``. That made the *same* event two different values
depending on where the subscriber happened to be connected: a browser on
the publishing node got the exact price (delivered locally, straight to
``Socket.send_json``) and a browser on any other node got the double,
because its copy had been through Redis.

So these tests drive the whole hop for real — publish bytes out of node
A, feed those exact bytes to node B's pubsub dispatcher, and read the
frame node B writes to its client's ASGI channel. Hand-assembling the
dict on node B would prove nothing about the serialization it crossed.
"""

from __future__ import annotations

import json
from decimal import Decimal

import pytest

from cara.broadcasting.ConnectionManager import ConnectionManager
from cara.broadcasting.drivers.RedisBroadcaster import RedisBroadcaster
from cara.websocket.Socket import Socket

# NUMERIC(17,6) ceiling — ``float(MONEY)`` is ``100000000000.0``.
MONEY = Decimal("99999999999.999999")

_CONFIG = {"websocket": {"heartbeat_interval": 0, "max_connections": 10}}


class _RecordingRedis:
    """Stands in for the broker. Records exactly what was PUBLISHed."""

    def __init__(self) -> None:
        self.published: list[tuple[str, str]] = []

    async def publish(self, channel: str, payload: str) -> None:
        self.published.append((channel, payload))


def _node() -> tuple[RedisBroadcaster, _RecordingRedis]:
    node = RedisBroadcaster(_CONFIG)
    broker = _RecordingRedis()

    async def _redis():
        return broker

    node._redis = _redis
    return node, broker


def _connected_socket(sent: list[dict]) -> Socket:
    async def _send(message: dict) -> None:
        sent.append(message)

    async def _receive() -> dict:
        return {"type": "websocket.receive"}

    socket = Socket(
        application=None, scope={"type": "websocket"}, receive=_receive, send=_send
    )
    socket._ws_connected = True
    return socket


@pytest.mark.asyncio
async def test_published_payload_carries_exact_decimal_digits() -> None:
    node, broker = _node()

    await node.broadcast("orders", "price.changed", {"total": MONEY})

    channel, payload = broker.published[-1]
    assert channel == "cara_broadcast:orders"
    data = json.loads(payload)["data"]
    assert data["total"] == "99999999999.999999"
    assert Decimal(data["total"]) == MONEY
    assert data["total"] != float(MONEY)


@pytest.mark.asyncio
async def test_a_subscriber_on_another_node_sees_the_same_money() -> None:
    """The full cross-process hop: publish → Redis frame → remote frame."""
    publisher, broker = _node()
    subscriber, _ = _node()
    assert publisher._node_id != subscriber._node_id  # else the echo guard drops it

    sent: list[dict] = []
    # Register through the base manager: the driver's own subscribe()
    # would start a listener task against a Redis that isn't there.
    await ConnectionManager.add_connection(subscriber, "conn-1", _connected_socket(sent))
    await ConnectionManager.subscribe(subscriber, "conn-1", "orders")

    try:
        await publisher.broadcast("orders", "price.changed", {"total": MONEY})
        channel, payload = broker.published[-1]

        # The bytes that came off the wire, not a reconstruction of them.
        await subscriber._dispatch_pubsub_message({"channel": channel, "data": payload})

        assert sent, "the remote node delivered nothing to its local client"
        delivered = json.loads(sent[-1]["text"])
        assert delivered["event"] == "price.changed"
        assert delivered["data"]["total"] == "99999999999.999999"
        assert Decimal(delivered["data"]["total"]) == MONEY
    finally:
        await subscriber.cleanup()


@pytest.mark.asyncio
async def test_local_and_remote_subscribers_receive_identical_frames() -> None:
    """The property the private encoder broke: where you connect must not
    change what the price is."""
    publisher, broker = _node()
    subscriber, _ = _node()

    local_frames: list[dict] = []
    remote_frames: list[dict] = []
    await ConnectionManager.add_connection(
        publisher, "local-1", _connected_socket(local_frames)
    )
    await ConnectionManager.subscribe(publisher, "local-1", "orders")
    await ConnectionManager.add_connection(
        subscriber, "remote-1", _connected_socket(remote_frames)
    )
    await ConnectionManager.subscribe(subscriber, "remote-1", "orders")

    try:
        await publisher.broadcast("orders", "price.changed", {"total": MONEY})
        channel, payload = broker.published[-1]
        await subscriber._dispatch_pubsub_message({"channel": channel, "data": payload})

        assert local_frames and remote_frames
        assert json.loads(local_frames[-1]["text"]) == json.loads(
            remote_frames[-1]["text"]
        )
    finally:
        await publisher.cleanup()
        await subscriber.cleanup()


@pytest.mark.asyncio
async def test_an_unencodable_broadcast_fails_loudly_not_silently() -> None:
    """Encoding sits outside the Redis ``try`` on purpose.

    While it sat inside, a payload cara could not encode was swallowed
    into a ``Log.debug`` alongside genuine broker blips — the broadcast
    vanished on every node and nothing reported it.
    """

    class _Order:
        pass

    node, broker = _node()

    with pytest.raises(TypeError, match="not JSON serializable"):
        await node.broadcast("orders", "price.changed", {"order": _Order()})

    assert broker.published == []
