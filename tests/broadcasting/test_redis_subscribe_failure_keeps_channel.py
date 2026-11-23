"""A SUBSCRIBE that fails during a Redis outage is retried by the reconnect.

``_redis_subscribed`` is the set the listener re-subscribes after it
reconnects. A failed live SUBSCRIBE used to remove the channel from it while
the local subscription stood, so a tab that subscribed during an outage was
never wired again once Redis came back.
"""

from __future__ import annotations

import asyncio
import importlib
from types import SimpleNamespace

from cara.broadcasting.drivers import RedisBroadcaster

module = importlib.import_module("cara.broadcasting.drivers._RedisListener")


class _DeadPubSub:
    async def subscribe(self, *_channels):
        raise ConnectionError("Redis is down")


def test_a_failed_live_subscribe_stays_in_the_resubscribe_set(monkeypatch) -> None:
    monkeypatch.setattr(
        module, "Log", SimpleNamespace(warning=lambda *_args, **_kwargs: None)
    )

    async def scenario() -> tuple[set[str], bool]:
        broadcaster = RedisBroadcaster(
            {"websocket": {"heartbeat_interval": 0}}, redis_url="redis://127.0.0.1:1/0"
        )
        broadcaster._redis_subscribed = {"app:orders"}
        # A live listener whose connection dies under the SUBSCRIBE.
        broadcaster._listener_task = SimpleNamespace(done=lambda: False)
        broadcaster._listener_pubsub = _DeadPubSub()

        await broadcaster._ensure_redis_subscription("app:invoices")
        return broadcaster._redis_subscribed, broadcaster._listener_resync

    subscribed, resync = asyncio.run(scenario())
    assert subscribed == {"app:orders", "app:invoices"}
    # ... and the listener is told to rebuild its connection from that set:
    # redis-py may have reconnected this one itself, without the new channel.
    assert resync is True
