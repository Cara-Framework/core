"""``ConcurrencyLimited`` must actually FIND Redis, and must not allow on fault.

This ceiling was inert for its entire life. ``_get_redis`` duck-typed five
private attribute names on the Cache manager (``_redis``, ``store._redis``,
``store.redis``, ``connection()``, ``redis``); the manager has none of them,
so the probe returned ``None``, ``_try_acquire`` returned ``True`` before it
ever spoke to Redis, and every job declaring ``max_concurrent`` ran uncapped
while every dashboard showed the cap as enforced.

Three separate defects lived in that one method and each has a test here:

* **resolution** — the connection is reached through ``Cache.driver()`` and
  the driver's own ``connection()``, not by guessing attribute names;
* **dead handlers** — the ``except`` clauses named the BUILTIN
  ``ConnectionError``/``TimeoutError``, which redis-py never raises (its own
  classes descend from ``redis.exceptions.RedisError``), so the degradation
  paths were unreachable code;
* **fail-open** — a Redis fault, and a cache driver that cannot run ``EVAL``
  at all, both returned "you may run" (DOCTRINE §9 forbids an unconfigured
  or unavailable gate that allows).
"""

from __future__ import annotations

import asyncio

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from cara.queues.middleware.ConcurrencyBackendUnavailable import (
    ConcurrencyBackendUnavailable,
)
from cara.queues.middleware.ConcurrencyExceeded import ConcurrencyExceeded
from cara.queues.middleware.ConcurrencyLimited import ConcurrencyLimited


class _Job:
    pass


class _FakeRedis:
    """Records EVAL/ZREM traffic; ``eval_result`` drives the verdict."""

    def __init__(
        self,
        eval_result: int | None = 1,
        raises: Exception | None = None,
        release_raises: Exception | None = None,
    ):
        self.eval_result = eval_result
        self.raises = raises
        self.release_raises = release_raises
        self.evals: list[tuple] = []
        self.zrems: list[tuple[str, str]] = []

    def eval(self, script, numkeys, *args):
        if self.raises is not None:
            raise self.raises
        self.evals.append((script, numkeys, *args))
        return self.eval_result

    def zrem(self, key, member):
        if self.release_raises is not None:
            raise self.release_raises
        self.zrems.append((key, member))
        return 1


class _RedisLikeDriver:
    """Stands in for ``RedisCacheDriver`` — exposes the named accessor."""

    driver_name = "redis"

    def __init__(self, connection: _FakeRedis) -> None:
        self._connection = connection

    def connection(self):
        return self._connection


class _FileLikeDriver:
    """Stands in for ``FileCacheDriver`` — no ``connection()`` at all."""

    driver_name = "file"


class _CacheManager:
    """The shape ``app.make("cache")`` returns: a manager with ``driver()``."""

    def __init__(self, driver) -> None:
        self._driver = driver

    def driver(self, name: str | None = None):
        return self._driver


async def _run(middleware: ConcurrencyLimited, ran: list[str]):
    async def _handler(job):
        ran.append("body")
        return "done"

    return await middleware.handle(_Job(), _handler)


def test_the_connection_is_reached_through_the_driver_accessor() -> None:
    """The EVAL must actually be issued.

    Old behaviour: the attribute-name guessing returned ``None`` and
    ``_try_acquire`` returned True without a single Redis call, so
    ``evals`` stayed empty and the cap was fiction.
    """
    redis = _FakeRedis(eval_result=1)
    middleware = ConcurrencyLimited(max_concurrent=2, key="upstream")
    ran: list[str] = []

    result = asyncio.run(
        _run_with_cache(middleware, _CacheManager(_RedisLikeDriver(redis)), ran)
    )

    assert result == "done"
    assert ran == ["body"]
    assert len(redis.evals) == 1, "the ceiling never spoke to Redis"
    script, numkeys, key, *argv = redis.evals[0]
    assert numkeys == 1
    assert key == "cara:concurrency:upstream"
    assert argv[2] == "2", "max_concurrent must reach the script"


def test_the_slot_is_released_when_the_job_finishes() -> None:
    redis = _FakeRedis(eval_result=1)
    middleware = ConcurrencyLimited(max_concurrent=1, key="upstream")
    ran: list[str] = []

    asyncio.run(_run_with_cache(middleware, _CacheManager(_RedisLikeDriver(redis)), ran))

    assert [key for key, _ in redis.zrems] == ["cara:concurrency:upstream"]


def test_release_backend_fault_is_reported_but_ttl_owns_recovery(caplog) -> None:
    redis = _FakeRedis(release_raises=RedisConnectionError("redis is down"))
    middleware = ConcurrencyLimited(max_concurrent=1, key="upstream")
    ran: list[str] = []

    with caplog.at_level("WARNING"):
        result = asyncio.run(
            _run_with_cache(middleware, _CacheManager(_RedisLikeDriver(redis)), ran)
        )

    assert result == "done"
    assert ran == ["body"]
    assert any(
        "concurrency backend unavailable for upstream" in record.getMessage()
        for record in caplog.records
    )


def test_a_full_cap_throttles_without_running_the_body() -> None:
    redis = _FakeRedis(eval_result=0)
    middleware = ConcurrencyLimited(max_concurrent=1, key="upstream", retry_delay=0)
    ran: list[str] = []

    with pytest.raises(ConcurrencyExceeded) as raised:
        asyncio.run(
            _run_with_cache(middleware, _CacheManager(_RedisLikeDriver(redis)), ran)
        )

    assert ran == []
    assert raised.value.is_throttle is True, "a throttle must not spend the budget"


def test_a_redis_fault_fails_closed_and_says_so(caplog) -> None:
    """A redis-py error must throttle, not allow — and must be audible.

    Old behaviour: ``except OSError, ConnectionError, ...`` named the
    BUILTIN ``ConnectionError``, so redis-py's own class slipped past both
    handlers and escaped ``handle`` as a raw driver exception. Neither the
    intended "degrade gracefully" nor any log line ever happened.
    """
    redis = _FakeRedis(raises=RedisConnectionError("redis is down"))
    middleware = ConcurrencyLimited(max_concurrent=1, key="upstream", retry_delay=0)
    ran: list[str] = []

    with caplog.at_level("WARNING"), pytest.raises(ConcurrencyExceeded):
        asyncio.run(
            _run_with_cache(middleware, _CacheManager(_RedisLikeDriver(redis)), ran)
        )

    assert ran == [], "the ceiling must not admit a job it could not evaluate"
    assert any(
        "concurrency backend unavailable for upstream" in record.getMessage()
        for record in caplog.records
    ), "an unbounded throttle loop with no log is as invisible as the old bug"


def test_a_non_redis_driver_refuses_instead_of_running_uncapped() -> None:
    """Misconfiguration is loud, not permissive (§9).

    A job that declares a hard ceiling and is quietly handed none is the
    exact failure this middleware exists to prevent, and it is invisible
    from the outside.
    """
    middleware = ConcurrencyLimited(max_concurrent=1, key="upstream")
    ran: list[str] = []

    with pytest.raises(ConcurrencyBackendUnavailable) as raised:
        asyncio.run(_run_with_cache(middleware, _CacheManager(_FileLikeDriver()), ran))

    assert ran == []
    assert "'file'" in str(raised.value)
    assert raised.value.is_throttle is False, (
        "a deployment mistake must spend attempts and reach the DLQ"
    )


def test_no_cache_binding_fails_closed() -> None:
    middleware = ConcurrencyLimited(max_concurrent=1, key="unbound-key")
    ran: list[str] = []

    with pytest.raises(ConcurrencyBackendUnavailable, match="cache binding"):
        asyncio.run(_run_with_cache(middleware, None, ran))

    assert ran == []


@pytest.mark.parametrize(
    ("kwargs", "exception_type"),
    [
        ({"max_concurrent": 0}, ValueError),
        ({"max_concurrent": True}, TypeError),
        ({"retry_delay": -1}, ValueError),
        ({"retry_delay": 1.5}, TypeError),
        ({"slot_ttl": 0}, ValueError),
        ({"slot_ttl": False}, TypeError),
        ({"key": "  "}, ValueError),
    ],
)
def test_invalid_limiter_configuration_is_rejected(kwargs, exception_type) -> None:
    with pytest.raises(exception_type):
        ConcurrencyLimited(**kwargs)


async def _run_with_cache(middleware, cache, ran: list[str]):
    """Drive ``handle`` with ``_resolve_cache`` pinned to ``cache``.

    Patching the CACHE resolution (not the connection lookup) is
    deliberate: the connection lookup is the thing that was broken, so a
    test that stubs it out cannot see this class of bug at all.
    """
    original = ConcurrencyLimited._resolve_cache
    ConcurrencyLimited._resolve_cache = staticmethod(lambda: cache)
    try:
        return await _run(middleware, ran)
    finally:
        ConcurrencyLimited._resolve_cache = original
