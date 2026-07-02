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
