"""The file driver keeps counters apart from values, as Redis does.

``RedisCacheDriver.increment`` writes under the ``c:`` namespace and ``get``
reads ``v:``, so after ``increment(key)`` a ``get(key)`` misses. The file
driver stored both in one file: ``get`` answered the counter's int, and
``increment`` on a key holding a ``put`` value raised instead of counting.
"""

from __future__ import annotations

from cara.cache.drivers import FileCacheDriver

_KEY = b"file-cache-counter-namespace-signing-key"


def _driver(tmp_path) -> FileCacheDriver:
    return FileCacheDriver(str(tmp_path), signing_key=_KEY)


def test_get_after_increment_misses_like_redis(tmp_path) -> None:
    driver = _driver(tmp_path)

    assert driver.increment("hits", 1, ttl=60) == 1
    assert driver.increment("hits", 2, ttl=60) == 3

    assert driver.get("hits") is None
    assert driver.get("hits", "miss") == "miss"
    assert driver.many(["hits"]) == {"hits": None}


def test_values_and_counters_do_not_see_each_other(tmp_path) -> None:
    driver = _driver(tmp_path)
    driver.put("k", "value", ttl=60)

    assert driver.increment("k", 1, ttl=60) == 1
    assert driver.get("k") == "value"


def test_has_ttl_forget_and_pattern_cover_both_namespaces(tmp_path) -> None:
    driver = _driver(tmp_path)
    driver.increment("rate:user:7", 1, ttl=60)

    assert driver.has("rate:user:7") is True
    assert 0 < driver.ttl("rate:user:7") <= 60
    assert driver.forget("rate:user:7") is True
    assert driver.has("rate:user:7") is False
    assert driver.increment("rate:user:7", 1, ttl=60) == 1  # restarted

    driver.increment("feed:home", 1, ttl=60)
    driver.put("feed:home", "cached", ttl=60)
    assert driver.forget_pattern("feed:home") == 2
    assert driver.has("feed:home") is False


def test_flush_drops_counters(tmp_path) -> None:
    driver = _driver(tmp_path)
    driver.increment("n", 5, ttl=60)

    driver.flush()

    assert driver.increment("n", 1, ttl=60) == 1


def test_a_value_key_spelled_like_a_counter_file_is_hashed_apart(tmp_path) -> None:
    # '=' is outside the filename whitelist, so no value key can land on a
    # counter's file.
    driver = _driver(tmp_path)
    driver.increment("x", 1, ttl=60)
    driver.put("x=c", "value", ttl=60)

    assert driver.increment("x", 1, ttl=60) == 2
    assert driver.get("x=c") == "value"
