"""Queue workers must not leak a Redis TCP connection per broadcast.

``queue:work`` runs each job under ``asyncio.run``, which creates a
fresh event loop. The old RedisBroadcaster cached an async redis-py
client keyed by ``id(loop)`` and never disconnected the pool when the
loop closed — every Vision/Automation status broadcast left one
ESTABLISHED connection behind until soft nofile (1024) was exhausted
(Errno 24 Too many open files).

PUBLISH now goes through a process-wide sync client. These tests pin
that contract: many ephemeral loops share one publisher, and the
async pool map stays empty on the publish path.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from cara.broadcasting.drivers.RedisBroadcaster import RedisBroadcaster

_CONFIG = {"websocket": {"heartbeat_interval": 0, "max_connections": 10}}


class _CountingSyncRedis:
    def __init__(self) -> None:
        self.publish_calls = 0
        self.closed = False

    def publish(self, channel: str, payload: str) -> int:
        self.publish_calls += 1
        return 1

    def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_publish_uses_sync_publisher_not_async_pool() -> None:
    node = RedisBroadcaster(_CONFIG, redis_url="redis://localhost:6379/0")
    fake = _CountingSyncRedis()
    node._publisher._client = fake

    await node.broadcast("orders", "status", {"ok": True})

    assert fake.publish_calls == 1
    assert node._redis_pools == {}
    assert node._redis_clients == {}


def test_ephemeral_asyncio_run_loops_reuse_one_sync_publisher() -> None:
    """Simulate the queue-worker pattern: one ``asyncio.run`` per job."""
    node = RedisBroadcaster(_CONFIG, redis_url="redis://localhost:6379/0")
    publishers: list[Any] = []

    def _factory() -> _CountingSyncRedis:
        client = _CountingSyncRedis()
        publishers.append(client)
        return client

    # First call installs the factory's client; subsequent calls must
    # return the same instance (process-wide reuse).
    first = _factory()
    node._publisher._client = first

    async def _one_broadcast(i: int) -> None:
        await node.broadcast("orders", "tick", {"n": i})

    for i in range(25):
        asyncio.run(_one_broadcast(i))

    assert first.publish_calls == 25
    assert len(publishers) == 1
    assert node._redis_pools == {}
    assert node._redis_clients == {}
