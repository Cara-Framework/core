"""``KeyCooldown`` releases a cooled-down key's job before it takes a slot."""

from __future__ import annotations

import asyncio

import pytest

from cara.queues.contracts import JobThrottledException
from cara.queues.middleware import KeyCooldown, cooldown_remaining, mark_cooldown
from cara.testing.fakes.CacheFake import CacheFake


def _module():
    # The barrel's lazy export binds the CLASS at the package attribute, so
    # ``import …KeyCooldown as m`` would hand back the class, not the module.
    import importlib

    return importlib.import_module("cara.queues.middleware.KeyCooldown")


@pytest.fixture
def cache(monkeypatch):
    fake = CacheFake()
    module = _module()

    monkeypatch.setattr(module, "_cache", lambda: fake)
    return fake


def _run(mw, ran):
    async def handler(job):
        ran.append(job)
        return "done"

    return asyncio.run(mw.handle("job", handler))


def test_no_cooldown_runs_the_job(cache):
    ran = []
    assert _run(KeyCooldown("retailer:a"), ran) == "done"
    assert ran == ["job"]


def test_cooled_down_key_raises_a_throttle_and_never_runs(cache):
    mark_cooldown("retailer:a", headers={"Retry-After": "40"})
    ran = []
    with pytest.raises(JobThrottledException) as info:
        _run(KeyCooldown("retailer:a"), ran)
    assert ran == []
    assert 39 <= info.value.retry_after <= 41
    # Another key is untouched.
    assert _run(KeyCooldown("retailer:b"), ran) == "done"


def test_cooldown_is_clamped_and_defaulted(cache):
    assert mark_cooldown("k1", seconds=1) == 5.0
    assert mark_cooldown("k2", seconds=10_000) == 300.0
    assert mark_cooldown("k3") == 30.0
    assert mark_cooldown("k4", headers={"Retry-After": "garbage"}) == 30.0


def test_a_shorter_cooldown_never_shortens_an_open_one(cache):
    mark_cooldown("k", seconds=100)
    mark_cooldown("k", seconds=5)
    assert cooldown_remaining("k") > 90


def test_cache_outage_fails_open(monkeypatch):
    module = _module()

    class _Down:
        def get(self, *a, **k):
            raise OSError("redis down")

        put = get

    monkeypatch.setattr(module, "_cache", lambda: _Down())
    assert mark_cooldown("k", seconds=50) == 0.0
    ran = []
    assert _run(KeyCooldown("k"), ran) == "done"


def test_none_key_is_a_pass_through(cache):
    ran = []
    assert _run(KeyCooldown(None), ran) == "done"
