from __future__ import annotations

import pytest

from cara.cache.Cache import Cache

_EXACT_KEY = "refresh:inflight:7"


class _LockStore:
    """The atomic driver primitives used by ``CacheLock``."""

    def __init__(self) -> None:
        self.entries: dict[str, str] = {}
        self.ttls: dict[str, int | None] = {}

    def add(self, key: str, value: str, ttl: int | None = None) -> bool:
        if key in self.entries:
            return False
        self.entries[key] = value
        self.ttls[key] = ttl
        return True

    def forget_if(self, key: str, expected_value: str) -> bool:
        if self.entries.get(key) != expected_value:
            return False
        self.entries.pop(key)
        self.ttls.pop(key, None)
        return True


def _cache() -> tuple[Cache, _LockStore]:
    store = _LockStore()
    cache = Cache(application=None, default_driver="test")
    cache.add_driver("test", store)
    return cache, store


def test_exact_lock_preserves_key_while_named_lock_keeps_its_prefix() -> None:
    cache, _store = _cache()

    assert cache.exact_lock(_EXACT_KEY).key == _EXACT_KEY
    assert cache.lock(_EXACT_KEY).key == f"lock:{_EXACT_KEY}"


def test_exact_lock_claim_uses_owner_token_and_requested_ttl() -> None:
    cache, store = _cache()
    lock = cache.exact_lock(_EXACT_KEY, timeout=300, owner="worker-a")

    assert lock.acquire() is True
    assert store.entries == {_EXACT_KEY: "worker-a"}
    assert store.ttls == {_EXACT_KEY: 300}
    assert lock.release() is True
    assert store.entries == {}


def test_exact_lock_release_is_owner_fenced_after_lease_takeover() -> None:
    cache, store = _cache()
    first = cache.exact_lock(_EXACT_KEY, timeout=300, owner="worker-a")
    successor = cache.exact_lock(_EXACT_KEY, timeout=300, owner="worker-b")
    assert first.acquire() is True

    # Worker A's lease expires; worker B acquires the same canonical key.
    store.entries.pop(_EXACT_KEY)
    store.ttls.pop(_EXACT_KEY)
    assert successor.acquire() is True

    assert first.release() is False
    assert store.entries == {_EXACT_KEY: "worker-b"}
    assert successor.release() is True


@pytest.mark.parametrize("key", ["", None, b""])
def test_exact_lock_rejects_an_empty_or_non_string_key(key: object) -> None:
    cache, _store = _cache()

    with pytest.raises(ValueError, match="non-empty string"):
        cache.exact_lock(key)  # type: ignore[arg-type]
