"""``CacheLock`` never hides the body's error, and never locks forever
(framework seam sweep, 2026-11-19).

* ``__exit__`` / ``__aexit__`` called ``release()`` bare: a cache fault there
  replaced the body's own exception (a Paddle failure surfaced as a Redis
  ``forget_if`` error);
* a lock whose TTL lapsed mid-body released nothing and said nothing;
* ``timeout=0`` reached the driver as "no expiry": a holder that crashed held
  the key forever.
"""

from __future__ import annotations

import asyncio
import logging

import pytest

from cara.cache.CacheLock import CacheLock


class _Store:
    def __init__(self, *, release_fails: bool = False) -> None:
        self.entries: dict[str, str] = {}
        self.release_fails = release_fails

    def add(self, key, value, ttl=None):
        if key in self.entries:
            return False
        self.entries[key] = value
        return True

    def forget_if(self, key, expected):
        if self.release_fails:
            raise ConnectionError("redis went away")
        if self.entries.get(key) != expected:
            return False
        del self.entries[key]
        return True


class _BodyFailed(Exception):
    pass


def test_the_body_error_survives_a_failed_release():
    with pytest.raises(_BodyFailed), CacheLock(_Store(release_fails=True), "k", 30):
        raise _BodyFailed()


def test_the_async_body_error_survives_a_failed_release():
    async def run():
        async with CacheLock(_Store(release_fails=True), "k", 30):
            raise _BodyFailed()

    with pytest.raises(_BodyFailed):
        asyncio.run(run())


def test_a_failed_release_after_a_clean_body_still_raises():
    with pytest.raises(ConnectionError), CacheLock(_Store(release_fails=True), "k", 30):
        pass


def test_a_lapsed_lock_is_reported(caplog):
    store = _Store()
    with (
        caplog.at_level(logging.WARNING, logger="cara.cache"),
        CacheLock(store, "k", 30),
    ):
        store.entries.clear()  # the TTL ran out mid-body

    assert "lapsed" in caplog.text


@pytest.mark.parametrize("timeout", [0, -1, True, 1.5])
def test_a_lock_without_a_real_expiry_is_refused(timeout):
    with pytest.raises(ValueError):
        CacheLock(_Store(), "k", timeout)
