"""``CacheFake`` keeps counters apart from values, as Redis does.

``RedisCacheDriver.increment`` writes under the ``c:`` namespace and ``get``
reads ``v:``, so after ``increment(key)`` a ``get(key)`` misses. The fake kept
both in one dict: ``get`` answered the counter's int — a value no Redis
deployment ever returns — so a test could pass on the fake and fail in
production. ``has`` / ``ttl`` / ``forget`` / ``forget_pattern`` / ``flush``
cover both namespaces, as the drivers do.
"""

from __future__ import annotations

from cara.testing.fakes import CacheFake


def test_get_after_increment_returns_the_default():
    cache = CacheFake()

    assert cache.increment("hits", 1, ttl=60) == 1
    assert cache.increment("hits", 2, ttl=60) == 3

    assert cache.get("hits") is None
    assert cache.get("hits", "miss") == "miss"
    assert cache.many(["hits"]) == {"hits": None}


def test_a_value_and_a_counter_under_one_key_do_not_see_each_other():
    cache = CacheFake()
    cache.put("k", "value", 60)

    assert cache.increment("k", 1, ttl=60) == 1
    assert cache.get("k") == "value"


def test_has_and_ttl_see_a_counter():
    cache = CacheFake()
    cache.increment("rate:user:7", 1, ttl=30)

    assert cache.has("rate:user:7") is True
    assert cache.ttl("rate:user:7") == 30


def test_forget_removes_both_namespaces():
    cache = CacheFake()
    cache.put("k", "value", 60)
    cache.increment("k", 5, ttl=60)

    assert cache.forget("k") is True

    assert cache.has("k") is False
    assert cache.increment("k", 1, ttl=60) == 1  # restarted


def test_forget_pattern_counts_each_namespace():
    cache = CacheFake()
    cache.increment("feed:home", 1, ttl=60)
    cache.put("feed:home", "cached", 60)
    cache.put("other", "kept", 60)

    assert cache.forget_pattern("feed:*") == 2

    assert cache.has("feed:home") is False
    assert cache.get("other") == "kept"


def test_flush_drops_counters():
    cache = CacheFake()
    cache.increment("n", 5, ttl=60)

    cache.flush()

    assert cache.has("n") is False
    assert cache.increment("n", 1, ttl=60) == 1
