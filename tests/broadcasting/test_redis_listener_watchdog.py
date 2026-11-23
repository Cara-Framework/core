"""The Redis listener notices a dead connection, and owns its pubsub alone.

Two live defects (2026-11-23), both silent:

* redis-py's ``listen()`` waits for the next frame with no read timeout. Behind
  a half-open TCP connection (a dropped NAT flow, a powered-off Redis host) it
  waited for ever: 78 s into a black hole the listener had attempted no
  reconnect, and every cross-process frame for the node was lost until the
  process restarted.
* A tab that subscribed while the listener was between connections sent its
  SUBSCRIBE on the half-built pubsub, which opened a second connection. The
  listener read one of the two; the channels on the other went quiet.

No Redis here: the fake pubsub is the wire, and says what was asked of it.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest

from cara.broadcasting.drivers.RedisBroadcaster import RedisBroadcaster


class _PubSub:
    def __init__(self, gate: asyncio.Event | None) -> None:
        self.subscribe_calls: list[tuple[str, ...]] = []
        self.unsubscribe_calls: list[tuple[str, ...]] = []
        self.subscribed: set[str] = set()
        self.inbox: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.pings = 0
        self.closed = False
        # What the test can do to this connection.
        self.answers_ping = True
        self.broken = False
        self.refuses_subscribe = False
        self._gate = gate

    async def subscribe(self, *channels: str) -> None:
        self.subscribe_calls.append(tuple(sorted(channels)))
        if self._gate is not None:
            await self._gate.wait()
        if self.refuses_subscribe:
            raise ConnectionError("connection reset under SUBSCRIBE")
        self.subscribed.update(channels)

    async def unsubscribe(self, *channels: str) -> None:
        self.unsubscribe_calls.append(tuple(sorted(channels)))
        self.subscribed.difference_update(channels)

    async def get_message(self, timeout: float = 0.0) -> dict[str, Any] | None:
        if self.broken:
            raise ConnectionError("Connection closed by server.")
        try:
            return await asyncio.wait_for(self.inbox.get(), timeout)
        except TimeoutError:
            return None

    async def ping(self) -> None:
        self.pings += 1
        if self.answers_ping:
            # What redis-py really hands back for a PONG on a RESP3 text
            # connection: not a "message", but proof the line is alive.
            self.inbox.put_nowait({"type": "P", "channel": "O", "data": "N"})

    async def aclose(self) -> None:
        self.closed = True


class _Client:
    def __init__(self) -> None:
        self.pubsubs: list[_PubSub] = []
        self.next_gate: asyncio.Event | None = None

    def pubsub(self) -> _PubSub:
        pubsub = _PubSub(self.next_gate)
        self.next_gate = None
        self.pubsubs.append(pubsub)
        return pubsub


class _Socket:
    def __init__(self) -> None:
        self.frames: list[dict] = []

    async def send_json(self, message: dict) -> None:
        self.frames.append(message)


def _node() -> tuple[RedisBroadcaster, _Client]:
    node = RedisBroadcaster(
        {"websocket": {"heartbeat_interval": 0}, "connection": {"prefix": "p:"}},
        redis_url="redis://127.0.0.1:1/0",
    )
    client = _Client()

    async def _redis() -> _Client:
        return client

    node._redis = _redis  # type: ignore[method-assign]
    # The production cadence (poll 1 s, ping after 15 s, pong within 10 s),
    # shrunk so a test does not sit through it.
    node._poll_seconds = 0.01
    node._ping_after = 0.05
    node._pong_within = 0.05
    return node, client


async def _until(condition, timeout: float = 5.0) -> None:
    async with asyncio.timeout(timeout):
        while not condition():
            await asyncio.sleep(0.005)


def _frame(channel: str) -> dict[str, Any]:
    return {
        "type": "message",
        "channel": f"p:{channel}",
        "data": json.dumps(
            {"event": "e", "channel": channel, "data": {}, "_node_id": "other-node"}
        ),
    }


@pytest.mark.asyncio
async def test_a_silent_connection_is_pinged_then_replaced_with_every_channel() -> None:
    node, client = _node()
    socket = _Socket()
    await super(RedisBroadcaster, node).add_connection("c", socket)
    await super(RedisBroadcaster, node).subscribe("c", "orders")
    try:
        await node._ensure_redis_subscription("p:orders")
        await node._ensure_redis_subscription("p:stock")
        first = client.pubsubs[0]
        assert node._listener_pubsub is first

        first.answers_ping = False  # the black hole: bytes go in, none come out
        await _until(lambda: len(client.pubsubs) == 2 and node._listener_ready.is_set())

        second = client.pubsubs[1]
        assert first.pings >= 1
        assert first.closed
        assert second.subscribed == {"p:orders", "p:stock"}
        assert node._listener_pubsub is second

        second.inbox.put_nowait(_frame("orders"))
        await _until(lambda: socket.frames)
        assert socket.frames[0]["event"] == "e"
    finally:
        await node.cleanup()


@pytest.mark.asyncio
async def test_a_quiet_connection_that_answers_its_ping_is_left_alone() -> None:
    node, client = _node()
    try:
        await node._ensure_redis_subscription("p:orders")
        first = client.pubsubs[0]

        # Several ping cycles of nothing but PONGs: idle is not dead.
        await _until(lambda: first.pings >= 3)

        assert len(client.pubsubs) == 1
        assert not first.closed
        assert node._listener_pubsub is first
        assert node._listener_ready.is_set()
    finally:
        await node.cleanup()


@pytest.mark.asyncio
async def test_changes_made_during_a_reconnect_land_on_the_new_connection() -> None:
    node, client = _node()
    try:
        await node._ensure_redis_subscription("p:orders")
        await node._ensure_redis_subscription("p:stock")
        first = client.pubsubs[0]
        asked_of_first = list(first.subscribe_calls)

        # Redis restarts. The next connection's SUBSCRIBE is held open, the
        # window the second connection used to be opened in.
        held = asyncio.Event()
        client.next_gate = held
        first.broken = True
        await _until(lambda: node._listener_pubsub is None)

        # A tab subscribes during the back-off: nothing to send it on.
        await asyncio.wait_for(node._ensure_redis_subscription("p:late"), 0.5)

        await _until(
            lambda: len(client.pubsubs) == 2 and client.pubsubs[1].subscribe_calls
        )
        second = client.pubsubs[1]
        # ... and two more changes while the new connection is still subscribing.
        await asyncio.wait_for(node._ensure_redis_subscription("p:later"), 0.5)
        await asyncio.wait_for(node._drop_redis_subscription("p:stock"), 0.5)
        assert node._listener_pubsub is None  # not live, so not handed out

        held.set()
        await _until(lambda: node._listener_ready.is_set())

        assert first.subscribe_calls == asked_of_first
        assert first.unsubscribe_calls == []
        assert first.closed
        assert second.subscribed == {"p:orders", "p:late", "p:later"}
        assert node._listener_pubsub is second
        assert len(client.pubsubs) == 2
    finally:
        await node.cleanup()


@pytest.mark.asyncio
async def test_a_subscribe_that_fails_on_the_live_connection_is_retried() -> None:
    node, client = _node()
    try:
        await node._ensure_redis_subscription("p:orders")
        first = client.pubsubs[0]
        first.refuses_subscribe = True

        await node._ensure_redis_subscription("p:invoices")

        # The channel is wanted, the live connection does not carry it: the
        # listener rebuilds from the wanted set rather than reading on.
        await _until(lambda: len(client.pubsubs) == 2 and node._listener_ready.is_set())
        assert first.closed
        assert client.pubsubs[1].subscribed == {"p:orders", "p:invoices"}
    finally:
        await node.cleanup()


@pytest.mark.asyncio
async def test_an_unsubscribe_on_the_live_connection_still_goes_straight_out() -> None:
    node, client = _node()
    try:
        await node._ensure_redis_subscription("p:orders")
        await node._ensure_redis_subscription("p:stock")
        first = client.pubsubs[0]

        await node._drop_redis_subscription("p:stock")

        assert first.unsubscribe_calls == [("p:stock",)]
        assert first.subscribed == {"p:orders"}
        assert len(client.pubsubs) == 1
    finally:
        await node.cleanup()
