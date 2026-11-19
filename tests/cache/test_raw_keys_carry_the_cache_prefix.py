"""Keys written through the raw Redis connection carry the cache's prefix.

Every codec key lives under the configured ``prefix``; the two primitives that
need the raw client (``ConcurrencyLimited``'s EVAL'd sorted set, a product's
usage hash) wrote bare names instead, so two deployments sharing one Redis
database shared one concurrency ceiling and one usage counter (B2 Redis
audit, 2026-11-19). ``RedisCacheDriver.raw_key`` / ``Cache.raw_key`` name
them under the prefix, outside the codec namespace a cache flush scans, so a
flush still never frees a held slot.
"""

from __future__ import annotations

import asyncio

from cara.cache.Cache import Cache
from cara.cache.drivers.RedisCacheDriver import RedisCacheDriver
from cara.queues.middleware.ConcurrencyLimited import ConcurrencyLimited


def _driver(prefix: str) -> RedisCacheDriver:
    # ``redis.Redis`` is lazy: constructing it opens no socket.
    return RedisCacheDriver(
        host="localhost",
        port=6379,
        db=0,
        password=None,
        prefix=prefix,
        signing_key="test-signing-key-for-the-cache-codec",
    )


def test_two_prefixes_name_two_keys() -> None:
    first = _driver("deploy_a:").raw_key("cara:concurrency:x")
    second = _driver("deploy_b:").raw_key("cara:concurrency:x")

    assert first == "deploy_a:raw:cara:concurrency:x"
    assert second == "deploy_b:raw:cara:concurrency:x"


def test_a_raw_key_is_outside_the_namespace_a_flush_scans() -> None:
    driver = _driver("deploy_a:")

    assert not driver.raw_key("k").startswith(driver._prefix)


def test_the_manager_names_raw_keys_through_its_driver() -> None:
    manager = Cache(None, "redis")
    manager.add_driver("redis", _driver("deploy_a:"))

    assert manager.raw_key("k") == "deploy_a:raw:k"


class _Redis:
    def __init__(self) -> None:
        self.keys: list[str] = []

    def eval(self, _script, _numkeys, key, *_argv):
        self.keys.append(key)
        return 1

    def zrem(self, key, _member):
        self.keys.append(key)
        return 1


def test_the_concurrency_ceiling_is_keyed_under_the_prefix(monkeypatch) -> None:
    redis = _Redis()
    driver = _driver("deploy_a:")
    monkeypatch.setattr(driver, "connection", lambda: redis)
    manager = Cache(None, "redis")
    manager.add_driver("redis", driver)
    middleware = ConcurrencyLimited(max_concurrent=1, key="upstream")
    monkeypatch.setattr(
        ConcurrencyLimited, "_resolve_cache", staticmethod(lambda: manager)
    )

    async def _handler(_job):
        return "done"

    assert asyncio.run(middleware.handle(object(), _handler)) == "done"
    assert set(redis.keys) == {"deploy_a:raw:cara:concurrency:upstream"}
