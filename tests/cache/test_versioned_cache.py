"""Tests for the VersionedCache version-key stamp primitive.

Pins the two invariants the version-key pattern depends on:

* ``read()`` goes through ``Cache.increment(key, 0, ttl)`` — never
  ``Cache.get`` (which can't decode an INCRBY-written counter under the
  Redis driver) — and materialises a missing key as 0,
* ``bump()`` is ``Cache.increment(key, 1, ttl)`` (atomic INCRBY, no
  read-modify-write TOCTOU),
* a callable TTL is resolved at call time, not at construction.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from cara.cache import VersionedCache


def test_read_uses_increment_zero_and_defaults_to_zero():
    cache = MagicMock()
    cache.increment.return_value = 0
    with patch("cara.cache.VersionedCache.Cache", cache):
        stamp = VersionedCache("brand:version", 3600).read()
    assert stamp == 0
    cache.increment.assert_called_once_with("brand:version", 0, 3600)
    cache.get.assert_not_called()


def test_bump_uses_increment_one():
    cache = MagicMock()
    cache.increment.return_value = 7
    with patch("cara.cache.VersionedCache.Cache", cache):
        stamp = VersionedCache("brand:version", 3600).bump()
    assert stamp == 7
    cache.increment.assert_called_once_with("brand:version", 1, 3600)


def test_callable_ttl_resolved_at_call_time():
    cache = MagicMock()
    cache.increment.return_value = 1
    ttl_values = iter([100, 200])
    vc = VersionedCache("k", lambda: next(ttl_values))
    with patch("cara.cache.VersionedCache.Cache", cache):
        vc.read()
        vc.bump()
    assert cache.increment.call_args_list[0].args == ("k", 0, 100)
    assert cache.increment.call_args_list[1].args == ("k", 1, 200)


def test_read_or_none_answers_none_on_cache_fault_not_zero():
    cache = MagicMock()
    cache.increment.side_effect = ConnectionError("redis down")
    with patch("cara.cache.VersionedCache.Cache", cache):
        assert VersionedCache("k", 60).read_or_none() is None


def test_try_bump_swallows_cache_fault():
    cache = MagicMock()
    cache.increment.side_effect = ConnectionError("redis down")
    with patch("cara.cache.VersionedCache.Cache", cache):
        assert VersionedCache("k", 60).try_bump() is None


def test_remember_bypasses_cache_when_stamp_unreadable():
    cache = MagicMock()
    cache.increment.side_effect = ConnectionError("redis down")
    with patch("cara.cache.VersionedCache.Cache", cache):
        value = VersionedCache("k", 60).remember(
            lambda v: f"page:v{v}", 30, lambda: "fresh"
        )
    assert value == "fresh"
    cache.remember.assert_not_called()


def test_remember_keys_by_stamp_and_is_not_strict():
    cache = MagicMock()
    cache.increment.return_value = 4
    cache.remember.return_value = "cached"
    with patch("cara.cache.VersionedCache.Cache", cache):
        value = VersionedCache("k", 60).remember(lambda v: f"page:v{v}", 30, lambda: "x")
    assert value == "cached"
    args, kwargs = cache.remember.call_args
    assert args[0] == "page:v4" and args[1] == 30
    assert kwargs == {"strict": False}
