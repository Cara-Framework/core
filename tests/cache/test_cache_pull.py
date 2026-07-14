from __future__ import annotations

import pickle
from unittest.mock import MagicMock

from cara.cache import Cache
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
    driver = FileCacheDriver(str(tmp_path), default_ttl=60)
    driver.put("once", "value")

    assert driver.pull("once") == "value"
    assert driver.pull("once") is None


def test_redis_driver_pull_decodes_pickled_value() -> None:
    driver = object.__new__(RedisCacheDriver)
    driver._prefix = "test:"
    driver._client = MagicMock()
    driver._client.getdel.return_value = pickle.dumps({"user_id": 7})

    assert driver.pull("once") == {"user_id": 7}
    driver._client.getdel.assert_called_once_with("test:once")
