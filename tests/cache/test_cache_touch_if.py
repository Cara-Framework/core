"""``touch_if`` renews a lease only for its owner and never recreates one.

A heartbeat lease is a short TTL its owner keeps pushing forward. The
renewal must be a compare-and-set: an owner whose lease lapsed, or was
re-taken by another owner, must neither extend the new owner's key nor bring
its own back. Every driver and the test fake answer the same way.
"""

from __future__ import annotations

import time
from unittest.mock import MagicMock

import pytest

from cara.cache.codecs import JsonCacheCodec
from cara.cache.drivers import FileCacheDriver, RedisCacheDriver
from cara.exceptions import CacheConfigurationException
from cara.testing.fakes.CacheFake import CacheFake

_KEY = b"cache-touch-if-signing-key-32-bytes!"


def test_file_driver_renews_only_its_own_live_value(tmp_path) -> None:
    driver = FileCacheDriver(str(tmp_path), signing_key=_KEY)
    driver.add("lease", {"owner": "a"}, ttl=5)

    assert driver.touch_if("lease", {"owner": "a"}, 600) is True
    assert driver.ttl("lease") > 5
    assert driver.touch_if("lease", {"owner": "b"}, 600) is False
    assert driver.get("lease") == {"owner": "a"}


def test_file_driver_never_recreates_a_lapsed_lease(tmp_path) -> None:
    driver = FileCacheDriver(str(tmp_path), signing_key=_KEY)

    assert driver.touch_if("lease", "owner", 60) is False
    assert driver.has("lease") is False

    driver.put("lease", "owner", ttl=1)
    time.sleep(1.05)
    assert driver.touch_if("lease", "owner", 60) is False
    assert driver.has("lease") is False


def test_redis_driver_compares_the_exact_encoding_and_sets_the_ttl() -> None:
    driver = object.__new__(RedisCacheDriver)
    driver._value_prefix = "test_cache:j1:v:"
    driver._codec = JsonCacheCodec(_KEY)
    driver._client = MagicMock()
    driver._client.eval.return_value = 1

    assert driver.touch_if("lease", {"owner": "a"}, 120) is True

    script, numkeys, key, token, ttl = driver._client.eval.call_args.args
    assert script == RedisCacheDriver._TOUCH_LOCK_LUA
    assert "expire" in script and "get" in script
    assert (numkeys, key, ttl) == (1, "test_cache:j1:v:lease", "120")
    assert token == driver._codec.encode({"owner": "a"})

    driver._client.eval.return_value = 0
    assert driver.touch_if("lease", {"owner": "b"}, 120) is False


@pytest.mark.parametrize("ttl", [0, -1, True, 1.5, "60"])
def test_every_surface_refuses_an_ambiguous_ttl(tmp_path, ttl) -> None:
    redis_driver = object.__new__(RedisCacheDriver)
    for surface in (
        FileCacheDriver(str(tmp_path), signing_key=_KEY),
        redis_driver,
        CacheFake(),
    ):
        with pytest.raises(CacheConfigurationException, match="positive integer"):
            surface.touch_if("lease", "owner", ttl)


def test_fake_matches_the_driver_contract() -> None:
    cache = CacheFake()
    cache.add("lease", "owner", 10)

    assert cache.touch_if("lease", "owner", 120) is True
    assert cache.ttl_of("lease") == 120
    assert cache.touch_if("lease", "someone-else", 600) is False
    assert cache.ttl_of("lease") == 120
    assert cache.touch_if("absent", "owner", 120) is False
    assert cache.has("absent") is False
