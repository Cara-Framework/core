"""``RedisCacheDriver.connection()`` is the ONE way to reach the client.

``ConcurrencyLimited`` needed a raw connection to run its Lua semaphore and
went looking for one by guessing attribute names — ``cache._redis``,
``cache.store._redis``, ``cache.store.redis``, ``cache.connection()``,
``cache.redis``. This driver has never carried any of them (its client is
``_client``), so the guess returned ``None`` on every call and a hard
concurrency ceiling silently enforced nothing for its entire life.

A named accessor makes that class of miss impossible: it either exists or
the caller fails loudly. This test pins the name so a future rename cannot
quietly re-open the hole.
"""

from __future__ import annotations

import pytest

from cara.cache.drivers.RedisCacheDriver import RedisCacheDriver


@pytest.fixture
def driver() -> RedisCacheDriver:
    # ``redis.Redis`` is lazy — constructing it opens no socket, so this
    # needs no live server.
    return RedisCacheDriver(
        host="localhost",
        port=6379,
        db=0,
        password=None,
        signing_key="test-signing-key-for-the-cache-codec",
    )


def test_connection_returns_the_client_the_driver_writes_through(driver) -> None:
    assert driver.connection() is driver._client


def test_the_accessor_is_public_and_callable(driver) -> None:
    """The middleware feature-detects ``connection`` by name.

    ``ConcurrencyLimited._connection`` refuses a driver without it, so this
    is a load-bearing part of the public driver surface, not an accessor of
    convenience.
    """
    accessor = getattr(driver, "connection", None)
    assert callable(accessor)


def test_the_middleware_resolves_it_through_the_cache_manager(driver) -> None:
    """End-to-end: manager → ``driver()`` → ``connection()``.

    This is exactly the path that returned ``None`` before, and it is the
    reason the fix lives on the driver rather than in more duck-typing.
    """
    from cara.queues.middleware.ConcurrencyLimited import ConcurrencyLimited

    class _Manager:
        def driver(self, name: str | None = None):
            return driver

    assert ConcurrencyLimited._connection(_Manager()) is driver._client
