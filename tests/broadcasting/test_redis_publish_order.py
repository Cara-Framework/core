"""Frames leave a process in the order they were broadcast, and without
waiting on that process's own slow sockets.

Live (2026-11-23), two defects on the PUBLISH path:

* Publishes ran on sixteen threads, one round trip each. Two frames broadcast
  back to back raced to the wire and reached the other nodes reversed 48 % of
  the time.
* The PUBLISH was sent only after the local fan-out returned, so one local
  client that had stopped reading held the frame back from every other node
  for a whole send timeout.

The fake below is the sync Redis client's wire: it records the order frames
arrived in and how many round trips carried them.
"""

from __future__ import annotations

import asyncio
import json
import threading
import time

import pytest

from cara.broadcasting.ConnectionManager import ConnectionManager
from cara.broadcasting.drivers.RedisBroadcaster import RedisBroadcaster

_CONFIG = {"websocket": {"heartbeat_interval": 0, "broadcast_send_timeout": 1.0}}


class _Pipeline:
    def __init__(self, wire: _Wire) -> None:
        self._wire = wire
        self._frames: list[str] = []

    def publish(self, _channel: str, payload: str) -> None:
        self._frames.append(payload)

    def execute(self) -> list[int]:
        self._wire.round_trip(self._frames)
        return [1] * len(self._frames)


class _Wire:
    def __init__(self) -> None:
        self.frames: list[str] = []
        self.round_trips: list[int] = []
        self.transactions: list[bool] = []
        self.entered = threading.Event()
        self.open = threading.Event()
        self.open.set()
        self.failures: list[Exception] = []

    def round_trip(self, frames: list[str]) -> None:
        self.entered.set()
        assert self.open.wait(5), "the test never reopened the wire"
        if self.failures:
            raise self.failures.pop(0)
        self.frames.extend(frames)
        self.round_trips.append(len(frames))

    def publish(self, _channel: str, payload: str) -> int:
        self.round_trip([payload])
        return 1

    def pipeline(self, transaction: bool = True) -> _Pipeline:
        self.transactions.append(transaction)
        return _Pipeline(self)

    def close(self) -> None:
        return None


def _node() -> tuple[RedisBroadcaster, _Wire]:
    node = RedisBroadcaster(_CONFIG, redis_url="redis://127.0.0.1:1/0")
    wire = _Wire()
    node._publisher._client = wire
    return node, wire


def _sequence(wire: _Wire) -> list[int]:
    return [json.loads(frame)["data"]["n"] for frame in wire.frames]


@pytest.mark.asyncio
async def test_back_to_back_broadcasts_reach_the_wire_in_broadcast_order() -> None:
    node, wire = _node()

    # Detached, the way ``helpers.broadcast`` schedules them: nothing awaits
    # one frame before issuing the next.
    await asyncio.gather(
        *(
            asyncio.create_task(node.broadcast("orders", "order.updated", {"n": n}))
            for n in range(300)
        )
    )

    assert _sequence(wire) == list(range(300))


@pytest.mark.asyncio
async def test_frames_that_queue_behind_a_round_trip_share_the_next_one() -> None:
    node, wire = _node()
    wire.open.clear()
    first = asyncio.create_task(node._publisher.publish("p:orders", '{"data": {"n": 0}}'))
    assert await asyncio.to_thread(wire.entered.wait, 5)

    # The writer is busy with frame 0; twenty more arrive meanwhile.
    rest = [
        asyncio.create_task(
            node._publisher.publish("p:orders", json.dumps({"data": {"n": n}}))
        )
        for n in range(1, 21)
    ]
    await asyncio.sleep(0)  # every one of them has taken its place
    wire.open.set()
    await asyncio.gather(first, *rest)

    assert _sequence(wire) == list(range(21))
    assert wire.round_trips == [1, 20]
    assert wire.transactions == [False]


@pytest.mark.asyncio
async def test_a_failed_round_trip_fails_its_callers_and_not_the_next_frame() -> None:
    node, wire = _node()
    wire.failures.append(ConnectionError("Redis went away"))

    with pytest.raises(ConnectionError):
        await node._publisher.publish("p:orders", '{"data": {"n": 0}}')
    await node._publisher.publish("p:orders", '{"data": {"n": 1}}')

    assert _sequence(wire) == [1]


@pytest.mark.asyncio
async def test_a_caller_cancelled_while_it_waits_has_still_published() -> None:
    node, wire = _node()
    wire.open.clear()
    first = asyncio.create_task(node._publisher.publish("p:orders", '{"data": {"n": 0}}'))
    assert await asyncio.to_thread(wire.entered.wait, 5)
    second = asyncio.create_task(
        node._publisher.publish("p:orders", '{"data": {"n": 1}}')
    )
    third = asyncio.create_task(node._publisher.publish("p:orders", '{"data": {"n": 2}}'))
    await asyncio.sleep(0)

    second.cancel()
    await asyncio.gather(second, return_exceptions=True)
    wire.open.set()
    await asyncio.gather(first, third)

    # Its place in the order was taken when it published; cancelling the wait
    # does not take the frame back, and does not strand the one behind it.
    assert _sequence(wire) == [0, 1, 2]


class _Wedged:
    """A local client whose TCP window is full: the send never returns."""

    async def send_json(self, _message: dict) -> None:
        await asyncio.Event().wait()


@pytest.mark.asyncio
async def test_the_publish_does_not_wait_for_a_wedged_local_socket() -> None:
    node, _wire = _node()
    published: list[tuple[float, int]] = []

    async def _publish_redis(_channel: str, payload: str) -> None:
        published.append((time.monotonic(), json.loads(payload)["data"]["n"]))

    node._publish_redis = _publish_redis  # type: ignore[method-assign]
    await ConnectionManager.add_connection(node, "wedged", _Wedged())
    await ConnectionManager.subscribe(node, "wedged", "orders")
    try:
        started = time.monotonic()
        slow = asyncio.create_task(node.broadcast("orders", "order.updated", {"n": 0}))
        quick = asyncio.create_task(node.broadcast("stock", "stock.updated", {"n": 1}))
        await asyncio.gather(slow, quick)
        finished = time.monotonic()

        # Both frames were on the wire at once, in broadcast order, while the
        # first broadcast was still waiting out its wedged subscriber.
        assert [n for _at, n in published] == [0, 1]
        assert all(at - started < 0.3 for at, _n in published)
        assert finished - started >= 0.9
        assert "wedged" not in node.connections
    finally:
        await node.cleanup()


@pytest.mark.asyncio
async def test_a_redis_failure_still_delivers_to_local_subscribers() -> None:
    node, _wire = _node()

    async def _publish_redis(_channel: str, _payload: str) -> None:
        raise ConnectionError("Redis went away")

    node._publish_redis = _publish_redis  # type: ignore[method-assign]

    class _Socket:
        def __init__(self) -> None:
            self.frames: list[dict] = []

        async def send_json(self, message: dict) -> None:
            self.frames.append(message)

    socket = _Socket()
    await ConnectionManager.add_connection(node, "c", socket, user_id="7")
    await ConnectionManager.subscribe(node, "c", "orders")
    try:
        await node.broadcast("orders", "order.updated", {"n": 0})
        await node.broadcast_to_user("7", "note", {"n": 1})

        assert [frame["event"] for frame in socket.frames] == ["order.updated", "note"]
    finally:
        await node.cleanup()
