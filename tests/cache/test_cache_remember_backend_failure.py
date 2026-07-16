from __future__ import annotations

import time
from typing import Any

from cara.cache import Cache


class _Driver:
    def __init__(
        self,
        *,
        add_results: list[bool | Exception],
        winner_value: Any = None,
    ) -> None:
        self.add_results = list(add_results)
        self.winner_value = winner_value
        self.add_calls = 0
        self.put_calls = 0
        self.remember_calls = 0
        self.get_calls = 0

    def get(self, _key: str, default: Any = None) -> Any:
        self.get_calls += 1
        if self.winner_value is not None and self.get_calls > 1:
            return self.winner_value
        return default

    def add(self, _key: str, _value: Any, _ttl: int | None = None) -> bool:
        self.add_calls += 1
        result = self.add_results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    def put(self, _key: str, _value: Any, _ttl: int | None = None) -> None:
        self.put_calls += 1
        raise ConnectionError("cache write unavailable")

    def remember(self, _key: str, _ttl: int, callback):
        self.remember_calls += 1
        return callback()


def _cache(driver: _Driver) -> Cache:
    cache = Cache(application=None, default_driver="fake")
    cache.add_driver("fake", driver)
    return cache


def test_initial_lock_backend_error_computes_without_polling() -> None:
    driver = _Driver(add_results=[ConnectionError("redis unavailable")])
    callbacks = 0

    def callback() -> dict[str, bool]:
        nonlocal callbacks
        callbacks += 1
        return {"ok": True}

    started = time.monotonic()
    result = _cache(driver).remember(
        "catalog:facets",
        60,
        callback,
        stampede_lock_seconds=5,
    )

    assert result == {"ok": True}
    assert callbacks == 1
    assert driver.add_calls == 1
    assert driver.put_calls == 1
    assert time.monotonic() - started < 0.5


def test_secondary_lock_backend_error_stops_waiting_immediately() -> None:
    driver = _Driver(
        add_results=[
            False,
            ConnectionError("redis became unavailable"),
        ],
    )
    callbacks = 0

    def callback() -> str:
        nonlocal callbacks
        callbacks += 1
        return "computed"

    started = time.monotonic()
    result = _cache(driver).remember(
        "home:aggregate",
        60,
        callback,
        stampede_lock_seconds=5,
    )

    assert result == "computed"
    assert callbacks == 1
    assert driver.add_calls == 2
    assert time.monotonic() - started < 0.5


def test_false_lock_result_remains_legitimate_contention() -> None:
    driver = _Driver(
        add_results=[False],
        winner_value={"from": "winner"},
    )
    callbacks = 0

    def callback() -> dict[str, str]:
        nonlocal callbacks
        callbacks += 1
        return {"from": "fallback"}

    result = _cache(driver).remember(
        "home:aggregate",
        60,
        callback,
        stampede_lock_seconds=5,
    )

    assert result == {"from": "winner"}
    assert callbacks == 0
    assert driver.add_calls == 1
