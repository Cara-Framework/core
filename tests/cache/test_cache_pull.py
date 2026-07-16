from __future__ import annotations

import pickle
from unittest.mock import MagicMock

from cara.cache import Cache
from cara.cache.codecs import JsonCacheCodec
from cara.cache.drivers import FileCacheDriver, RedisCacheDriver
from cara.testing.fakes import CacheFake


def test_cache_manager_delegates_pull() -> None:
    driver = CacheFake()
    driver.put("once", {"user_id": 7})
    cache = Cache(application=None, default_driver="fake")
    cache.add_driver("fake", driver)

    assert cache.pull("once") == {"user_id": 7}
    assert cache.pull("once", "missing") == "missing"


def test_file_driver_pull_removes_value(tmp_path) -> None:
    driver = FileCacheDriver(
        str(tmp_path),
        default_ttl=60,
        signing_key=b"file-pull-test-signing-key-32-bytes",
    )
    driver.put("once", "value")

    assert driver.pull("once") == "value"
    assert driver.pull("once") is None


def test_file_driver_deletes_legacy_pickle_without_decoding(tmp_path) -> None:
    driver = FileCacheDriver(
        str(tmp_path),
        default_ttl=60,
        signing_key=b"file-pull-test-signing-key-32-bytes",
    )
    path = driver._file_path("legacy")
    with open(path, "wb") as legacy:
        legacy.write(pickle.dumps((None, {"secret": "legacy"})))

    assert driver.get("legacy", "miss") == "miss"
    assert not tmp_path.joinpath("legacy.cache").exists()


def test_redis_driver_pull_decodes_authenticated_value() -> None:
    driver = object.__new__(RedisCacheDriver)
    driver._prefix = "test:j1:"
    driver._value_prefix = "test:j1:v:"
    driver._counter_prefix = "test:j1:c:"
    driver._codec = JsonCacheCodec(b"redis-pull-test-signing-key-32-bytes")
    driver._client = MagicMock()
    driver._client.getdel.return_value = driver._codec.encode({"user_id": 7})

    assert driver.pull("once") == {"user_id": 7}
    driver._client.getdel.assert_called_once_with("test:j1:v:once")
