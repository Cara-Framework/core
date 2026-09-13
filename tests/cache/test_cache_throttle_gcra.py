"""The GCRA cell: burst, refusal without a write, refill, expiry.

The Redis driver evaluates the same algorithm server-side in Lua; this pins the
arithmetic against a clock the test controls, for the file driver AND the cache
fake — a fake that computes a different bucket is a test suite proving the wrong
thing.
"""

from __future__ import annotations

import importlib
import time as real_time

import pytest

from cara.cache.drivers.FileCacheDriver import FileCacheDriver
from cara.exceptions import CacheConfigurationException
from cara.testing.fakes import CacheFake

_driver_module = importlib.import_module("cara.cache.drivers.FileCacheDriver")
_fake_module = importlib.import_module("cara.testing.fakes.CacheFake")
_KEY = "throttle-cell-signing-key-0123456789abcdef"
_START_MS = 1_700_000_000_000


class _Clock:
    """Stands in for the driver module's ``time``; only ``time()`` is frozen."""

    def __init__(self, now_ms: int) -> None:
        self.now_ms = now_ms

    def time(self) -> float:
        return self.now_ms / 1000

    def __getattr__(self, name: str):
        return getattr(real_time, name)


@pytest.fixture
def clock(monkeypatch) -> _Clock:
    frozen = _Clock(_START_MS)
    monkeypatch.setattr(_driver_module, "time", frozen)
    monkeypatch.setattr(_fake_module, "time", frozen)
    return frozen


@pytest.fixture(params=["file-driver", "cache-fake"])
def driver(request, tmp_path, clock) -> FileCacheDriver | CacheFake:
    if request.param == "file-driver":
        return FileCacheDriver(str(tmp_path), signing_key=_KEY)
    return CacheFake()


def _spend(
    driver: FileCacheDriver | CacheFake, *, cost: int = 1
) -> tuple[bool, int, int, int]:
    return driver.throttle("rate:user:7", emission_interval_ms=1000, burst=3, cost=cost)


def test_a_full_bucket_absorbs_its_burst_then_refuses(driver) -> None:
    assert _spend(driver) == (True, 2, 0, 1000)
    assert _spend(driver) == (True, 1, 0, 2000)
    assert _spend(driver) == (True, 0, 0, 3000)
    assert _spend(driver) == (False, 0, 1000, 3000)


def test_a_refusal_writes_nothing_so_hammering_does_not_delay_the_reopening(
    driver, clock
) -> None:
    for _ in range(3):
        _spend(driver)
    for _ in range(5):
        assert _spend(driver) == (False, 0, 1000, 3000)

    clock.now_ms += 1000

    assert _spend(driver) == (True, 0, 0, 3000)


def test_the_bucket_refills_at_its_pace_not_at_a_window_boundary(driver, clock) -> None:
    for _ in range(3):
        _spend(driver)

    clock.now_ms += 2500

    assert _spend(driver) == (True, 1, 0, 1500)
    assert _spend(driver) == (True, 0, 0, 2500)
    assert _spend(driver)[0] is False


def test_an_idle_cell_expires_back_to_a_full_bucket(driver, clock) -> None:
    _spend(driver)

    clock.now_ms += 5000

    assert _spend(driver) == (True, 2, 0, 1000)


def test_a_cost_that_does_not_fit_is_refused_while_cells_remain(driver) -> None:
    assert _spend(driver, cost=2) == (True, 1, 0, 2000)
    assert _spend(driver, cost=2) == (False, 1, 1000, 2000)


@pytest.mark.parametrize(
    ("interval", "burst", "cost"),
    [(0, 3, 1), (1000, 0, 1), (1000, 3, 0), (True, 3, 1), (1000, 3, 1.5)],
    ids=["zero-interval", "zero-burst", "zero-cost", "bool-interval", "float-cost"],
)
def test_parameters_that_describe_no_bucket_are_configuration_errors(
    driver, interval, burst, cost
) -> None:
    with pytest.raises(CacheConfigurationException):
        driver.throttle(
            "rate:user:7", emission_interval_ms=interval, burst=burst, cost=cost
        )


def test_a_cell_holding_something_else_refuses_rather_than_resetting(driver) -> None:
    driver.put("rate:user:7", "not a timestamp", ttl=60)

    with pytest.raises(CacheConfigurationException):
        _spend(driver)
