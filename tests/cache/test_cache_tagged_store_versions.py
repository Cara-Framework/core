"""Flushing one tag invalidates every entry carrying it, and nothing else.

A tag used to be only a key prefix: ``tags("posts", "featured")`` wrote
``posts:featured:<key>``. So ``tags("featured").flush()`` (pattern
``featured:*``) left that entry alive, ``tags("posts").flush()`` also deleted
an UNTAGGED key that merely began with ``posts:``, and every flush walked the
keyspace. Each tag now owns a version id folded into its entries' keys;
flushing a tag replaces the id — no scan — and returns how many tags it reset.
"""

from __future__ import annotations

import pytest

from cara.cache.CacheTaggedStore import CacheTaggedStore
from cara.cache.drivers.FileCacheDriver import FileCacheDriver
from cara.testing.fakes import CacheFake

_KEY = b"cache-tagged-store-signing-key-32b"


@pytest.fixture(params=["cache-fake", "file-driver"])
def cache(request, tmp_path, monkeypatch):
    backend = (
        CacheFake()
        if request.param == "cache-fake"
        else FileCacheDriver(str(tmp_path), signing_key=_KEY)
    )

    def no_scan(*_args, **_kwargs):
        raise AssertionError("a tag flush must not scan the keyspace")

    monkeypatch.setattr(backend, "forget_pattern", no_scan, raising=False)
    monkeypatch.setattr(backend, "forget_by_prefix", no_scan, raising=False)
    return backend


def _tags(cache, *tags: str) -> CacheTaggedStore:
    return CacheTaggedStore(cache, list(tags))


def test_flushing_either_tag_invalidates_an_entry_stored_under_both(cache):
    _tags(cache, "posts", "featured").put("post:1", "hero", 60)
    assert _tags(cache, "posts", "featured").get("post:1") == "hero"

    assert _tags(cache, "featured").flush() == 1

    assert _tags(cache, "posts", "featured").get("post:1") is None


def test_flushing_one_tag_spares_entries_of_other_tags(cache):
    _tags(cache, "posts").put("post:1", "kept", 60)
    _tags(cache, "users").put("user:1", "kept too", 60)

    _tags(cache, "featured").flush()
    _tags(cache, "users").flush()

    assert _tags(cache, "posts").get("post:1") == "kept"
    assert _tags(cache, "users").get("user:1") is None


def test_an_untagged_key_with_the_tag_as_prefix_is_untouched(cache):
    cache.put("posts:1", "untagged", 60)
    _tags(cache, "posts").put("1", "tagged", 60)

    _tags(cache, "posts").flush()

    assert cache.get("posts:1") == "untagged"
    assert _tags(cache, "posts").get("1") is None


def test_flush_returns_the_number_of_tags_reset_and_new_writes_land(cache):
    store = _tags(cache, "posts", "featured", "home")

    assert store.flush() == 3

    store.put("post:2", "fresh", 60)
    assert store.get("post:2") == "fresh"
