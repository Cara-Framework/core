"""File cache preserves authority semantics across corruption and processes."""

from __future__ import annotations

import multiprocessing
import os
from pathlib import Path

import pytest

from cara.cache.drivers import FileCacheDriver
from cara.exceptions import CacheConfigurationException

_KEY = b"file-cache-authority-signing-key-32-bytes"


def _claim(directory: str, start_fd: int, result_fd: int) -> None:
    driver = FileCacheDriver(directory, signing_key=_KEY)
    try:
        if os.read(start_fd, 1) != b"1":
            os._exit(2)
        os.write(result_fd, b"1" if driver.add("flight", "owner", ttl=30) else b"0")
    finally:
        os.close(start_fd)
        os.close(result_fd)


def test_default_read_rejects_corruption_and_disposable_read_opts_out(tmp_path) -> None:
    driver = FileCacheDriver(str(tmp_path), signing_key=_KEY)
    path = Path(driver._file_path("authority"))
    path.write_bytes(b"tampered")

    with pytest.raises(CacheConfigurationException, match="Unreadable"):
        driver.get("authority")

    path.write_bytes(b"tampered")
    assert driver.get("acceleration", "miss", strict=False) == "miss"


def test_counter_rejects_corrupt_state(tmp_path) -> None:
    driver = FileCacheDriver(str(tmp_path), signing_key=_KEY)
    # The counter's own file: a ``put`` value is another namespace (as on Redis).
    driver._write_file(driver._counter_path("rate"), None, "not-an-integer")

    with pytest.raises(CacheConfigurationException, match="non-integer"):
        driver.increment("rate", 1, ttl=60)


def test_add_has_one_cross_process_winner(tmp_path) -> None:
    context = multiprocessing.get_context("fork")
    start_read, start_write = os.pipe()
    result_read, result_write = os.pipe()
    workers = [
        context.Process(target=_claim, args=(str(tmp_path), start_read, result_write))
        for _ in range(4)
    ]
    for worker in workers:
        worker.start()
    os.close(start_read)
    os.close(result_write)
    os.write(start_write, b"1" * len(workers))
    os.close(start_write)
    outcomes = b""
    while len(outcomes) < len(workers):
        chunk = os.read(result_read, len(workers) - len(outcomes))
        if not chunk:
            break
        outcomes += chunk
    os.close(result_read)
    for worker in workers:
        worker.join(timeout=5)
        assert worker.exitcode == 0

    assert outcomes.count(b"1") == 1
    assert outcomes.count(b"0") == 3


@pytest.mark.parametrize("ttl", [-1, True, 2.5, "30"])
def test_file_ttl_rejects_coercive_values(tmp_path, ttl) -> None:
    driver = FileCacheDriver(str(tmp_path), signing_key=_KEY)

    with pytest.raises(CacheConfigurationException, match="TTL"):
        driver.put("authority", "value", ttl=ttl)


@pytest.mark.parametrize(
    ("first", "second"),
    [
        ("search:q=red shoes", "search:q=red_shoes"),
        ("page:/a/b", "page:_a_b"),
        ("user:1|orders", "user:1_orders"),
    ],
)
def test_keys_that_differ_only_in_sanitized_characters_do_not_alias(
    tmp_path, first: str, second: str
) -> None:
    # Every unsafe character became "_" in the filename, so both keys shared
    # one file: a read answered the other key's value, and ``add`` (the lock
    # primitive) on one refused the other.
    driver = FileCacheDriver(str(tmp_path), signing_key=_KEY)

    driver.put(first, "first value", 60)

    assert driver.get(second) is None
    assert driver.add(second, "second value", 60) is True
    assert driver.get(first) == "first value"
    assert driver.get(second) == "second value"
    assert driver.forget(first) is True
    assert driver.get(second) == "second value"


def test_pattern_invalidation_still_reaches_sanitized_keys(tmp_path) -> None:
    driver = FileCacheDriver(str(tmp_path), signing_key=_KEY)
    driver.put("page:/a/b", 1, 60)
    driver.put("page:/a/c", 2, 60)
    driver.put("page:plain", 3, 60)
    driver.put("other:/a/b", 4, 60)

    assert driver.forget_pattern("page:/a/b") == 1  # no trailing wildcard
    assert driver.get("page:/a/b") is None
    assert driver.forget_pattern("page:*") == 2
    assert driver.get("other:/a/b") == 4
