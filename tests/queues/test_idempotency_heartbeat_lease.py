"""The job lease is a heartbeat lease, and only a run that RAN spends a cooldown.

Live, 2026-10-30: a worker restart mid-pull left the order poll's
``job_lock:`` lease behind for its full ``timeout + 300`` (3900 s). Every
later poll of that channel claimed the 3-minute cooldown, met the orphaned
lease, waited 300 s on it and settled "completed" having done nothing — and
kept the cooldown, so the next queued polls skipped too. These pin the fix:

* the lease is short (``JOB_LOCK_LEASE_SECONDS``) whatever the job's timeout,
  and the running callback keeps it alive;
* a renewal is a compare-and-set — a lapsed lease another run took is never
  extended or recreated;
* a run that only waited on another owner hands its cooldown back;
* a forced worker exit hands back the lease and the unspent cooldown.
"""

from __future__ import annotations

import asyncio
import sys
import time

import pytest

from cara.queues.idempotency import MakesIdempotentBase
from cara.queues.LeaseHeartbeat import LeaseHeartbeat
from cara.testing.fakes.CacheFake import CacheFake

cooldown_module = sys.modules["cara.queues.idempotency.ClaimsSourceCooldown"]
orchestrator = sys.modules["cara.queues.idempotency.MakesIdempotentBase"]

COOLDOWN_KEY = "collection_cooldown:orders:31"


class _RecordingCache(CacheFake):
    def __init__(self) -> None:
        super().__init__()
        self.touches: list[tuple[str, int]] = []

    def touch_if(self, key, expected_value, ttl):
        renewed = super().touch_if(key, expected_value, ttl)
        if renewed:
            self.touches.append((key, ttl))
        return renewed


@pytest.fixture
def cache(monkeypatch):
    fake = _RecordingCache()
    monkeypatch.setattr(cooldown_module, "Cache", fake)
    monkeypatch.setattr(orchestrator, "Cache", fake)
    monkeypatch.setattr(cooldown_module, "config", lambda key, default=None: default)
    yield fake
    for heartbeat in LeaseHeartbeat.live():
        heartbeat.stop()


@pytest.fixture
def fast_heartbeat(monkeypatch):
    # A 1 s lease renewed every 20 ms keeps the test fast; production renews
    # its 120 s lease every 40 s.
    monkeypatch.setattr(_PullOrders, "JOB_LOCK_LEASE_SECONDS", 1)
    monkeypatch.setattr(LeaseHeartbeat, "RENEWALS_PER_LEASE", 50)


class _PullOrders(MakesIdempotentBase):
    source_cooldown_minutes = {"orders": 3}
    cooldown_grain_attrs = ("channel_id",)
    cooldown_requires_grains = True
    idempotency_cache_results = False
    timeout = 3600

    def __init__(self, channel_id: int = 31) -> None:
        super().__init__()
        self.source = "orders"
        self.channel_id = channel_id

    def should_execute_based_on_lifecycle(self) -> bool:
        return self._claim_source_cooldown()


def _lock_key(job: MakesIdempotentBase) -> str:
    job._idempotency_key = job.generate_idempotency_key()
    return job._lock_key()


def test_the_lease_is_short_whatever_the_jobs_timeout(cache):
    job = _PullOrders()
    key = _lock_key(job)

    assert job.acquire_job_lock() is True

    assert cache.ttl_of(key) == MakesIdempotentBase.JOB_LOCK_LEASE_SECONDS == 120
    job.release_job_lock()


def test_a_running_callback_keeps_its_lease_alive(cache, fast_heartbeat):
    job = _PullOrders()
    key = _lock_key(_PullOrders())

    async def long_pull():
        # A synchronous pull blocks the event loop; the heartbeat must not
        # care (it renews from its own thread).
        time.sleep(0.15)
        assert cache.has(key)
        return "pulled"

    assert asyncio.run(job.wrap_with_idempotency(long_pull)) == "pulled"

    assert len([touch for touch in cache.touches if touch[0] == key]) >= 3
    assert not cache.has(key), "the lease outlived its run"
    assert LeaseHeartbeat.live() == []


def test_a_lease_another_run_took_is_never_renewed_or_recreated(cache, fast_heartbeat):
    job = _PullOrders()
    key = _lock_key(_PullOrders())

    async def pull_while_our_lease_lapses():
        # Our lease lapsed (the fake has no clock — drop it the way expiry
        # would) and another run took the key.
        cache.forget(key)
        cache.add(key, {"owner": "the-next-run"}, 1)
        time.sleep(0.1)
        return "pulled"

    asyncio.run(job.wrap_with_idempotency(pull_while_our_lease_lapses))

    assert cache.get(key) == {"owner": "the-next-run"}
    assert cache.touches == [], "a renewal extended a lease this run no longer held"


def test_a_run_that_only_waited_on_another_owner_keeps_no_cooldown(cache, monkeypatch):
    owner = _PullOrders()
    _lock_key(owner)
    assert owner.acquire_job_lock() is True

    async def _waited():
        return None  # the owner's lease ran out; nothing was polled

    waiter = _PullOrders()
    monkeypatch.setattr(waiter, "wait_for_completion", _waited)

    async def never():
        raise AssertionError("the waiter must not run the callback")

    assert asyncio.run(waiter.wrap_with_idempotency(never)) is None
    assert not cache.has(COOLDOWN_KEY), "a run that never polled kept the cooldown"

    # The lease is gone now: the next tick of the poll runs at once instead
    # of skipping as "cooling down".
    owner.release_job_lock()
    polled: list[str] = []

    async def poll():
        polled.append("poll")

    asyncio.run(_PullOrders().wrap_with_idempotency(poll))
    assert polled == ["poll"]


def test_a_waiter_that_lost_the_acquire_race_keeps_no_cooldown(cache, monkeypatch):
    waiter = _PullOrders()
    monkeypatch.setattr(waiter, "is_job_locked", lambda: False)
    monkeypatch.setattr(waiter, "acquire_job_lock", lambda: False)

    async def _waited():
        return None

    monkeypatch.setattr(waiter, "wait_for_completion", _waited)

    async def never():
        raise AssertionError("the race loser must not run the callback")

    assert asyncio.run(waiter.wrap_with_idempotency(never)) is None
    assert not cache.has(COOLDOWN_KEY)


def test_a_forced_exit_hands_back_the_lease_and_the_unspent_cooldown(cache):
    job = _PullOrders()
    key = _lock_key(_PullOrders())
    observed: dict[str, bool] = {}

    async def pull_that_ignores_cancellation():
        # The worker's forced-exit step runs while this run is still stuck.
        assert LeaseHeartbeat.release_all(timeout_seconds=2.0) == 1
        observed["lease"] = cache.has(key)
        observed["cooldown"] = cache.has(COOLDOWN_KEY)
        return "pulled"

    asyncio.run(job.wrap_with_idempotency(pull_that_ignores_cancellation))

    assert observed == {"lease": False, "cooldown": False}
