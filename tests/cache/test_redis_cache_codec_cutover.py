from __future__ import annotations

import pickle
from unittest.mock import MagicMock

import pytest

from cara.cache.codecs import JsonCacheCodec
from cara.cache.drivers import RedisCacheDriver
from cara.exceptions import CacheConfigurationException

_KEY = b"redis-cache-driver-test-key-32-bytes"
_PICKLE_EXECUTED = False


def _execute_pickle_gadget() -> None:
    global _PICKLE_EXECUTED
    _PICKLE_EXECUTED = True


class _PickleGadget:
    def __reduce__(self):
        return (_execute_pickle_gadget, ())


def _driver() -> RedisCacheDriver:
    driver = object.__new__(RedisCacheDriver)
    driver._prefix = "test_cache:j1:"
    driver._value_prefix = "test_cache:j1:v:"
    driver._counter_prefix = "test_cache:j1:c:"
    driver._codec = JsonCacheCodec(_KEY)
    driver._default_ttl = 60
    driver._large_value_bytes = 262144
    driver._large_value_warned = set()
    driver._client = MagicMock()
    return driver


def test_constructor_uses_clean_versioned_type_namespaces() -> None:
    driver = RedisCacheDriver(
        host="127.0.0.1",
        port=6379,
        db=0,
        password=None,
        prefix="test_cache:",
        signing_key=_KEY,
    )

    assert driver._prefix == "test_cache:j1:"
    assert driver._value_key("user:1") == "test_cache:j1:v:user:1"
    assert driver._counter_key("rate:1") == "test_cache:j1:c:rate:1"


def test_constructor_never_falls_back_to_app_key(monkeypatch) -> None:
    monkeypatch.setenv("APP_KEY", "app-key-must-not-sign-cache-values-" * 2)

    with pytest.raises(CacheConfigurationException, match="CACHE_SIGNING_KEY"):
        RedisCacheDriver(
            host="127.0.0.1",
            port=6379,
            db=0,
            password=None,
            signing_key=None,
        )


@pytest.mark.parametrize("threshold", [True, False, 0, -1, 1.5, "1024"])
def test_constructor_rejects_ambiguous_large_value_threshold(threshold) -> None:
    with pytest.raises(CacheConfigurationException, match="positive integer"):
        RedisCacheDriver(
            host="127.0.0.1",
            port=6379,
            db=0,
            password=None,
            signing_key=_KEY,
            large_value_bytes=threshold,
        )


def test_noncanonical_pickle_is_deleted_and_rejected_without_execution() -> None:
    global _PICKLE_EXECUTED
    _PICKLE_EXECUTED = False
    driver = _driver()
    driver._client.get.return_value = pickle.dumps(_PickleGadget())

    with pytest.raises(CacheConfigurationException, match="Corrupt cache value"):
        driver.get("auth", "missing", strict=True)

    driver._client.get.assert_called_once_with("test_cache:j1:v:auth")
    driver._client.delete.assert_called_once_with("test_cache:j1:v:auth")
    assert _PICKLE_EXECUTED is False


def test_disposable_corruption_is_deleted_and_read_as_a_miss() -> None:
    driver = _driver()
    driver._client.get.return_value = b"tampered"

    assert driver.get("acceleration", "missing", strict=False) == "missing"

    driver._client.delete.assert_called_once_with("test_cache:j1:v:acceleration")


def test_strict_get_deletes_tampered_value_before_raising() -> None:
    driver = _driver()
    payload = bytearray(driver._codec.encode({"user_id": 7}))
    payload[-1] ^= 1
    driver._client.get.return_value = bytes(payload)

    with pytest.raises(CacheConfigurationException, match="Corrupt cache value"):
        driver.get("oauth-state", strict=True)

    driver._client.delete.assert_called_once_with("test_cache:j1:v:oauth-state")


def test_put_writes_authenticated_envelope_only() -> None:
    driver = _driver()

    driver.put("user:7", {"role": "viewer"}, ttl=30)

    redis_key, payload = driver._client.set.call_args.args
    assert redis_key == "test_cache:j1:v:user:7"
    assert payload.startswith(JsonCacheCodec.MAGIC)
    assert driver._codec.decode(payload) == {"role": "viewer"}
    assert driver._client.set.call_args.kwargs == {"ex": 30}


def test_backend_read_failure_raises_by_default() -> None:
    driver = _driver()
    driver._client.get.side_effect = ConnectionError("redis unavailable")

    with pytest.raises(ConnectionError, match="redis unavailable"):
        driver.get("authority")


def test_disposable_read_must_explicitly_opt_into_backend_miss() -> None:
    driver = _driver()
    driver._client.get.side_effect = ConnectionError("redis unavailable")

    assert driver.get("acceleration", "miss", strict=False) == "miss"


def test_disposable_write_must_explicitly_opt_into_backend_drop() -> None:
    driver = _driver()
    driver._client.set.side_effect = ConnectionError("redis unavailable")

    driver.put("acceleration", {"value": 1}, ttl=30, strict=False)

    with pytest.raises(ConnectionError, match="redis unavailable"):
        driver.put("authority", {"value": 1}, ttl=30)


@pytest.mark.parametrize("ttl", [-1, True, 2.5, "30"])
def test_ttl_rejects_coercive_values(ttl) -> None:
    driver = _driver()

    with pytest.raises(CacheConfigurationException, match="TTL"):
        driver.put("authority", "value", ttl=ttl)


def test_counter_requires_expiry_and_exact_integer_amount() -> None:
    driver = _driver()

    with pytest.raises(CacheConfigurationException, match="expiration"):
        driver.increment("rate", 1)
    with pytest.raises(TypeError, match="amount"):
        driver.increment("rate", True, ttl=60)


def test_corrupt_counter_is_never_reset() -> None:
    driver = _driver()
    driver._client.incrby.side_effect = RuntimeError("WRONGTYPE")

    with pytest.raises(RuntimeError, match="WRONGTYPE"):
        driver.increment("rate", 1, ttl=60)

    driver._client.pipeline.assert_not_called()


def test_counter_namespace_never_overlaps_authenticated_values() -> None:
    driver = _driver()
    driver._client.incrby.return_value = 1

    assert driver.increment("rate:user:7", 1, ttl=60) == 1

    driver._client.incrby.assert_called_once_with(
        "test_cache:j1:c:rate:user:7",
        1,
    )
    driver._client.expire.assert_called_once_with(
        "test_cache:j1:c:rate:user:7",
        60,
    )


def test_forget_and_has_cover_both_type_namespaces() -> None:
    driver = _driver()
    driver._client.delete.return_value = 2
    driver._client.exists.return_value = 1

    assert driver.forget("shared-key") is True
    assert driver.has("shared-key") is True

    keys = (
        "test_cache:j1:v:shared-key",
        "test_cache:j1:c:shared-key",
    )
    driver._client.delete.assert_called_once_with(*keys)
    driver._client.exists.assert_called_once_with(*keys)


def test_lock_add_and_compare_delete_use_identical_canonical_bytes() -> None:
    driver = _driver()
    driver._client.set.return_value = True
    driver._client.eval.return_value = 1
    owner = {"token": "owner-1", "generation": 2}

    assert driver.add("lock", owner, ttl=30) is True
    assert driver.forget_if("lock", owner) is True

    stored_payload = driver._client.set.call_args.args[1]
    released_payload = driver._client.eval.call_args.args[3]
    assert stored_payload == released_payload
    assert driver._codec.decode(stored_payload) == owner


def test_ttl_prefers_counter_then_falls_back_to_value() -> None:
    driver = _driver()
    driver._client.ttl.side_effect = [-2, 42]

    assert driver.ttl("key") == 42
    assert driver._client.ttl.call_args_list == [
        (("test_cache:j1:c:key",),),
        (("test_cache:j1:v:key",),),
    ]
