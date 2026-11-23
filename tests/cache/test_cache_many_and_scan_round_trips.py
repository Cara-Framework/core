"""Round trips the cache used to spend, and one set that never stopped growing.

* There was no multi-key read: N keys were N sequential ``GET`` round trips
  (three per authenticated request for a token's revocation entries).
* ``forget_pattern`` walked the whole keyspace twice (values, then counters)
  at 100 keys a step.
* The "already warned about this large key" set had no bound.
"""

from __future__ import annotations

import fnmatch
import importlib

import pytest

from cara.cache.Cache import Cache
from cara.cache.codecs import JsonCacheCodec
from cara.cache.contracts import CacheContract
from cara.cache.drivers import FileCacheDriver, RedisCacheDriver
from cara.exceptions import CacheConfigurationException

_KEY = b"redis-cache-driver-test-key-32-bytes"


class _FakeRedis:
    """A dict with the few commands these paths use, counting each call."""

    def __init__(self) -> None:
        self.data: dict[str, bytes] = {}
        self.calls: dict[str, int] = {}
        self.scan_matches: list[str] = []
        self.fail: Exception | None = None

    def _count(self, command: str) -> None:
        self.calls[command] = self.calls.get(command, 0) + 1
        if self.fail is not None:
            raise self.fail

    def get(self, key):
        self._count("get")
        return self.data.get(key)

    def mget(self, keys):
        self._count("mget")
        return [self.data.get(key) for key in keys]

    def set(self, key, value, ex=None, nx=False):
        self._count("set")
        self.data[key] = value
        return True

    def delete(self, *keys):
        self._count("delete")
        return sum(self.data.pop(key, None) is not None for key in keys)

    def scan(self, cursor=0, match="*", count=10):
        """Like the server: examine ``count`` keys per step, matching or not."""
        self._count("scan")
        self.scan_matches.append(match)
        if cursor == 0:
            self._walk = sorted(self.data)
        names = self._walk
        window = names[cursor : cursor + count]
        following = cursor + count
        return (
            0 if following >= len(names) else following,
            [name for name in window if fnmatch.fnmatchcase(name, match)],
        )


def _driver(client: _FakeRedis | None = None) -> RedisCacheDriver:
    driver = object.__new__(RedisCacheDriver)
    driver._prefix = "app:j1:"
    driver._value_prefix = "app:j1:v:"
    driver._counter_prefix = "app:j1:c:"
    driver._codec = JsonCacheCodec(_KEY)
    driver._default_ttl = 60
    driver._large_value_bytes = 64
    driver._large_value_warned = set()
    driver._client = client or _FakeRedis()
    return driver


def test_many_reads_every_key_in_one_round_trip() -> None:
    driver = _driver()
    driver.put("jwt_blacklist:abc", True, 60)
    driver.put("jwt_user_revoke:7", 1_700_000_000.5, 60)
    driver._client.calls.clear()

    values = driver.many(
        ["jwt_blacklist:abc", "jwt_family_revoked:f1", "jwt_user_revoke:7"], 0
    )

    assert values == {
        "jwt_blacklist:abc": True,
        "jwt_family_revoked:f1": 0,
        "jwt_user_revoke:7": 1_700_000_000.5,
    }
    assert list(values) == [
        "jwt_blacklist:abc",
        "jwt_family_revoked:f1",
        "jwt_user_revoke:7",
    ]
    assert driver._client.calls == {"mget": 1}


def test_many_with_no_keys_costs_no_round_trip() -> None:
    driver = _driver()

    assert driver.many([]) == {}
    assert driver._client.calls == {}


def test_many_fails_closed_like_get() -> None:
    driver = _driver()
    driver._client.fail = ConnectionError("redis is down")

    with pytest.raises(ConnectionError):
        driver.many(["a", "b"])
    # A disposable read degrades to the default for every key instead.
    assert driver.many(["a", "b"], "fallback", strict=False) == {
        "a": "fallback",
        "b": "fallback",
    }


def test_many_deletes_and_refuses_a_corrupt_entry_like_get() -> None:
    driver = _driver()
    driver.put("good", "value", 60)
    driver._client.data["app:j1:v:tampered"] = b"not an authenticated envelope"

    with pytest.raises(CacheConfigurationException, match="tampered"):
        driver.many(["good", "tampered"])
    assert "app:j1:v:tampered" not in driver._client.data

    driver._client.data["app:j1:v:tampered"] = b"still not an envelope"
    assert driver.many(["good", "tampered"], None, strict=False) == {
        "good": "value",
        "tampered": None,
    }


def test_get_still_reads_one_key_with_one_get() -> None:
    driver = _driver()
    driver.put("k", {"a": 1}, 60)
    driver._client.calls.clear()

    assert driver.get("k") == {"a": 1}
    assert driver.get("absent", "d") == "d"
    assert driver._client.calls == {"get": 2}


def test_the_manager_exposes_many_with_the_driver_argument_last() -> None:
    manager = Cache(None, "redis")
    manager.add_driver("redis", _driver())
    manager.put("a", 1, 60)

    assert manager.many(["a", "b"]) == {"a": 1, "b": None}
    assert manager.many(["a", "b"], "d", "redis", strict=False) == {"a": 1, "b": "d"}


def test_a_driver_without_its_own_many_reads_the_keys_in_turn() -> None:
    class _Dict(CacheContract):
        def __init__(self) -> None:
            self.stored = {"a": 1}
            self.strict_seen: list[bool] = []

        def get(self, key, default=None, *, strict=True):
            self.strict_seen.append(strict)
            return self.stored.get(key, default)

    driver = _Dict()

    assert driver.many(["a", "b"], "d", strict=False) == {"a": 1, "b": "d"}
    assert driver.strict_seen == [False, False]


def test_the_file_driver_reads_many_keys_under_one_lock(tmp_path, monkeypatch) -> None:
    driver = FileCacheDriver(str(tmp_path), signing_key=_KEY)
    driver.put("a", 1, 60)
    driver.put("b", [2], 60)
    acquisitions = []
    exclusive = driver._exclusive
    monkeypatch.setattr(
        driver, "_exclusive", lambda: acquisitions.append(1) or exclusive()
    )

    assert driver.many(["a", "b", "c"], "d") == {"a": 1, "b": [2], "c": "d"}
    assert len(acquisitions) == 1


def test_forget_pattern_walks_the_keyspace_once() -> None:
    client = _FakeRedis()
    driver = _driver(client)
    for index in range(40):
        driver.put(f"home:{index}", index, 60)
    client.data["app:j1:c:home:hits"] = b"3"  # a counter in the same family
    client.data["app:j1:v:homer"] = b"other family"
    client.data["app:raw:home:slot"] = b"a raw structure outside the codec"
    for index in range(5000):
        client.data[f"queue:job:{index}"] = b"co-tenant"
    client.calls.clear()

    deleted = driver.forget_pattern("home:*")

    assert deleted == 41
    assert "app:j1:v:homer" in client.data and "app:raw:home:slot" in client.data
    assert len(client.data) == 5002
    # One pattern covering both namespaces, 1000 keys a step: 6 round trips
    # over 5,043 keys. Two walks at 100 a step made 102.
    assert set(client.scan_matches) == {"app:j1:[vc]:home:*"}
    assert client.calls["scan"] == 6


def test_flush_steps_through_the_keyspace_a_thousand_keys_at_a_time() -> None:
    client = _FakeRedis()
    driver = _driver(client)
    driver.put("a", 1, 60)
    for index in range(3000):
        client.data[f"queue:job:{index}"] = b"co-tenant"
    client.calls.clear()

    driver.flush()

    assert client.calls["scan"] == 4  # 16 at the old step of 200
    assert len(client.data) == 3000  # nothing outside the prefix is touched


def test_the_large_value_warning_memory_is_bounded(monkeypatch) -> None:
    driver = _driver()
    monkeypatch.setattr(RedisCacheDriver, "_LARGE_VALUE_WARNED_LIMIT", 8)
    monkeypatch.setattr(
        importlib.import_module("cara.cache.drivers.RedisCacheDriver"),
        "Log",
        type("_Quiet", (), {"warning": staticmethod(lambda *a, **k: None)}),
    )

    for stamp in range(100):  # version-stamped keys: a new name every bump
        driver.put(f"catalog:v{stamp}:page", "x" * 200, 60)

    assert len(driver._large_value_warned) <= 8
