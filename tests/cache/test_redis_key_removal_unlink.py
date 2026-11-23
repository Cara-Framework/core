"""Redis ``flush`` / ``forget_pattern`` remove keys with UNLINK, not DEL.

``DEL`` frees every value inline on the single Redis thread, so flushing a
large prefix (or one big value) stalled every other client of the shared
server — queues, sessions, rate limits — for the duration. Removal now uses
``UNLINK`` (memory reclaimed on a background thread); a server without it
(Redis < 4.0, "unknown command") falls back to ``DEL``, once per driver.
"""

from __future__ import annotations

import fnmatch
import importlib
from unittest.mock import Mock

import pytest
from redis.exceptions import ResponseError

from cara.cache.codecs import JsonCacheCodec
from cara.cache.drivers import RedisCacheDriver

_removal_module = importlib.import_module("cara.cache.drivers._RedisKeyRemoval")
_KEY = b"redis-key-removal-signing-key-32b"


class _FakeRedis:
    def __init__(self, unlink_error: Exception | None = None) -> None:
        self.data: dict[str, bytes] = {}
        self.commands: list[str] = []
        self.unlink_error = unlink_error

    def unlink(self, *keys):
        self.commands.append("unlink")
        if self.unlink_error is not None:
            raise self.unlink_error
        return sum(self.data.pop(key, None) is not None for key in keys)

    def delete(self, *keys):
        self.commands.append("delete")
        return sum(self.data.pop(key, None) is not None for key in keys)

    def scan(self, cursor=0, match="*", count=10):
        return 0, [name for name in sorted(self.data) if fnmatch.fnmatchcase(name, match)]


def _driver(client: _FakeRedis) -> RedisCacheDriver:
    driver = object.__new__(RedisCacheDriver)
    driver._prefix = "app:j1:"
    driver._value_prefix = "app:j1:v:"
    driver._counter_prefix = "app:j1:c:"
    driver._codec = JsonCacheCodec(_KEY)
    driver._client = client
    client.data.update(
        {
            "app:j1:v:feed:home": b"1",
            "app:j1:c:feed:home": b"2",
            "app:j1:v:other": b"3",
            "queue:jobs": b"co-tenant",
        }
    )
    return driver


@pytest.fixture
def log(monkeypatch) -> Mock:
    log = Mock()
    monkeypatch.setattr(_removal_module, "Log", log)
    return log


def test_forget_pattern_unlinks_both_namespaces(log):
    client = _FakeRedis()
    driver = _driver(client)

    assert driver.forget_pattern("feed:*") == 2

    assert client.commands == ["unlink"]
    assert sorted(client.data) == ["app:j1:v:other", "queue:jobs"]


def test_flush_unlinks_only_our_prefix(log):
    client = _FakeRedis()
    driver = _driver(client)

    driver.flush()

    assert client.commands == ["unlink"]
    assert client.data == {"queue:jobs": b"co-tenant"}


def test_a_server_without_unlink_falls_back_to_del_once(log):
    client = _FakeRedis(ResponseError("unknown command 'UNLINK'"))
    driver = _driver(client)

    assert driver.forget_pattern("feed:*") == 2
    driver.flush()

    # One failed UNLINK, then DEL — the driver does not retry UNLINK.
    assert client.commands == ["unlink", "delete", "delete"]
    assert client.data == {"queue:jobs": b"co-tenant"}
    assert log.warning.call_count == 1
    assert "falls back to DEL" in log.warning.call_args.args[0]


def test_any_other_unlink_failure_is_not_retried_as_del(log):
    client = _FakeRedis(ConnectionError("redis unreachable"))
    driver = _driver(client)

    with pytest.raises(ConnectionError):
        driver.forget_pattern("feed:*")

    assert client.commands == ["unlink"]
    assert "app:j1:v:feed:home" in client.data
