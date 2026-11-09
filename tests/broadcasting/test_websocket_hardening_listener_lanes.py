"""The pubsub listener reads; per-channel lanes deliver."""

from __future__ import annotations

import asyncio
import json
from importlib import import_module
from typing import Any

import pytest

from cara.broadcasting.drivers.RedisBroadcaster import RedisBroadcaster

# By module path: the package's lazy barrel shadows it with the class.
driver = import_module("cara.broadcasting.drivers.RedisBroadcaster")


def _broadcaster() -> RedisBroadcaster:
    return RedisBroadcaster(
        {"websocket": {"heartbeat_interval": 0}, "connection": {"prefix": "p:"}},
        redis_url="redis://127.0.0.1:1/0",
    )


def _frame(channel: str, seq: int) -> dict[str, Any]:
    return {
        "type": "message",
        "channel": f"p:{channel}",
        "data": json.dumps({"event": "e", "data": {"seq": seq}, "_node_id": "other"}),
    }


async def _until(predicate, timeout: float = 1.0) -> None:
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.005)


class _Socket:
    def __init__(self) -> None:
        self.seqs: list[int] = []
        self.gate = asyncio.Event()
        self.gate.set()

    async def send_json(self, message: dict) -> None:
        await self.gate.wait()
        self.seqs.append(message["data"]["seq"])


async def _subscribe(broadcaster: RedisBroadcaster, cid: str, channel: str) -> _Socket:
    socket = _Socket()
    await super(RedisBroadcaster, broadcaster).add_connection(cid, socket)
    await super(RedisBroadcaster, broadcaster).subscribe(cid, channel)
    return socket


@pytest.mark.asyncio
async def test_a_channel_keeps_its_own_order_behind_a_slow_send() -> None:
    broadcaster = _broadcaster()
    socket = await _subscribe(broadcaster, "a", "tenant-a")
    socket.gate.clear()
    try:
        for seq in range(50):
            broadcaster._enqueue_pubsub_message(_frame("tenant-a", seq))
            await asyncio.sleep(0)
        socket.gate.set()

        await _until(lambda: len(socket.seqs) == 50)

        assert socket.seqs == list(range(50))
    finally:
        await broadcaster.cleanup()


@pytest.mark.asyncio
async def test_a_drained_lane_is_gone_and_a_later_frame_gets_a_new_one() -> None:
    broadcaster = _broadcaster()
    socket = await _subscribe(broadcaster, "a", "tenant-a")
    try:
        broadcaster._enqueue_pubsub_message(_frame("tenant-a", 1))
        await _until(lambda: socket.seqs == [1])
        await _until(lambda: not broadcaster._lanes and not broadcaster._lane_tasks)

        broadcaster._enqueue_pubsub_message(_frame("tenant-a", 2))
        await _until(lambda: socket.seqs == [1, 2])
    finally:
        await broadcaster.cleanup()


@pytest.mark.asyncio
async def test_a_full_lane_loses_its_oldest_frame(monkeypatch) -> None:
    monkeypatch.setattr(driver, "_LANE_CAPACITY", 3)
    broadcaster = _broadcaster()
    socket = await _subscribe(broadcaster, "a", "tenant-a")
    socket.gate.clear()
    try:
        broadcaster._enqueue_pubsub_message(_frame("tenant-a", 0))
        await asyncio.sleep(0.01)  # 0 is in flight, parked on the gate
        for seq in range(1, 7):
            broadcaster._enqueue_pubsub_message(_frame("tenant-a", seq))
        socket.gate.set()

        await _until(lambda: not broadcaster._lanes)

        assert socket.seqs == [0, 4, 5, 6]
    finally:
        await broadcaster.cleanup()


@pytest.mark.asyncio
async def test_a_frame_that_cannot_be_delivered_does_not_end_the_lane(
    monkeypatch,
) -> None:
    broadcaster = _broadcaster()
    delivered: list[int] = []

    async def dispatch(message: dict) -> None:
        seq = json.loads(message["data"])["data"]["seq"]
        if seq == 0:
            raise RuntimeError("one bad frame")
        delivered.append(seq)

    monkeypatch.setattr(broadcaster, "_dispatch_pubsub_message", dispatch)
    try:
        broadcaster._enqueue_pubsub_message(_frame("tenant-a", 0))
        broadcaster._enqueue_pubsub_message(_frame("tenant-a", 1))

        await _until(lambda: delivered == [1])
    finally:
        await broadcaster.cleanup()


@pytest.mark.asyncio
async def test_shutdown_ends_a_lane_parked_on_a_socket() -> None:
    broadcaster = _broadcaster()
    socket = await _subscribe(broadcaster, "a", "tenant-a")
    socket.gate.clear()
    broadcaster._enqueue_pubsub_message(_frame("tenant-a", 0))
    await asyncio.sleep(0.01)
    tasks = list(broadcaster._lane_tasks)

    await asyncio.wait_for(broadcaster.cleanup(), 1)

    assert tasks and all(task.done() for task in tasks)
    assert not broadcaster._lanes
