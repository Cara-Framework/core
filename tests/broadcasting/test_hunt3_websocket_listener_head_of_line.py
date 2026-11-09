"""One wedged socket must not stall every other workspace's realtime.

The Redis listener awaits each frame's whole fan-out before it reads the next.
Live run (2026-11-09): a client of workspace A that stopped reading held a
workspace B probe for 5.0 s, every frame behind it queued the same way, and a
busier stall overran Redis's 32 MB pub/sub buffer, which cut the listener and
lost 114 frames for which no client was ever told to reconcile.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import pytest

from cara.broadcasting.drivers.RedisBroadcaster import RedisBroadcaster


class _Socket:
    def __init__(self, *, wedged: bool = False) -> None:
        self.wedged = wedged
        self.frames: list[tuple[float, dict]] = []

    async def send_json(self, message: dict) -> None:
        if self.wedged:
            await asyncio.Event().wait()
        self.frames.append((time.monotonic(), message))


class _PubSub:
    def __init__(self, frames: list[dict]) -> None:
        self.frames = frames

    async def subscribe(self, *_channels: str) -> None:
        return None

    async def aclose(self) -> None:
        return None

    async def listen(self):
        for frame in self.frames:
            yield frame
        await asyncio.Event().wait()


class _Client:
    def __init__(self, pubsub: _PubSub) -> None:
        self._pubsub = pubsub

    def pubsub(self) -> _PubSub:
        return self._pubsub


def _frame(channel: str) -> dict[str, Any]:
    return {
        "type": "message",
        "channel": f"p:{channel}",
        "data": json.dumps(
            {"event": "e", "channel": channel, "data": {}, "_node_id": "other-node"}
        ),
    }


@pytest.mark.asyncio
async def test_a_wedged_subscriber_does_not_delay_another_channels_frame() -> None:
    broadcaster = RedisBroadcaster(
        {
            "websocket": {"heartbeat_interval": 0, "broadcast_send_timeout": 1.0},
            "connection": {"prefix": "p:"},
        },
        redis_url="redis://127.0.0.1:1/0",
    )
    healthy = _Socket()
    await super(RedisBroadcaster, broadcaster).add_connection("a", _Socket(wedged=True))
    await super(RedisBroadcaster, broadcaster).add_connection("b", healthy)
    await super(RedisBroadcaster, broadcaster).subscribe("a", "tenant-a")
    await super(RedisBroadcaster, broadcaster).subscribe("b", "tenant-b")
    pubsub = _PubSub([_frame("tenant-a"), _frame("tenant-b")])

    async def _redis():
        return _Client(pubsub)

    broadcaster._redis = _redis  # type: ignore[method-assign]
    broadcaster._redis_subscribed.add("p:tenant-a")
    started = time.monotonic()
    listener = asyncio.create_task(broadcaster._listener_loop())
    try:
        async with asyncio.timeout(2.0):
            while not healthy.frames:
                await asyncio.sleep(0.01)
        assert healthy.frames[0][0] - started < 0.3
    finally:
        listener.cancel()
        await asyncio.gather(listener, return_exceptions=True)
        await super(RedisBroadcaster, broadcaster).cleanup()
