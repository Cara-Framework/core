"""A concurrency slot is a heartbeat lease, not a timeout-sized reservation.

Synkronus sized its provider and per-tenant slots to ``timeout + 30``; a
worker restart mid-pull then kept the dead run's slot — and the tenant's
one-slot share of the lane — held for an hour (live, 2026-10-30). The slot
now keeps the limiter's short TTL and the running job renews its own member;
these pin that the renewal targets exactly that member, that it never
revives an expired one, and that the heartbeat ends with the job.
"""

from __future__ import annotations

import asyncio
import time

import pytest

from cara.queues.LeaseHeartbeat import LeaseHeartbeat
from cara.queues.middleware.ConcurrencyLimited import ConcurrencyLimited


class _Redis:
    def __init__(self):
        self.renewals: list[tuple] = []
        self.zrems: list[tuple[str, str]] = []

    def eval(self, script, numkeys, *args):
        if script == ConcurrencyLimited._RENEW_LUA:
            self.renewals.append(args)
        return 1

    def zrem(self, key, member):
        self.zrems.append((key, member))
        return 1


class _Driver:
    driver_name = "redis"

    def __init__(self, redis):
        self._redis = redis

    def connection(self):
        return self._redis


class _Cache:
    def __init__(self, redis):
        self._driver = _Driver(redis)

    def driver(self, name=None):
        return self._driver


@pytest.fixture(autouse=True)
def _no_leftover_heartbeats():
    yield
    for heartbeat in LeaseHeartbeat.live():
        heartbeat.stop()


def test_a_running_job_renews_its_own_slot_and_releases_it_after(monkeypatch):
    monkeypatch.setattr(LeaseHeartbeat, "RENEWALS_PER_LEASE", 6000)  # every 20 ms
    redis = _Redis()
    monkeypatch.setattr(
        ConcurrencyLimited, "_resolve_cache", staticmethod(lambda: _Cache(redis))
    )
    middleware = ConcurrencyLimited(max_concurrent=1, key="provider:amazon")

    async def _blocking_pull(_job):
        time.sleep(0.12)  # a synchronous call blocking the event loop
        return "pulled"

    result = asyncio.run(middleware.handle(object(), _blocking_pull))

    assert result == "pulled"
    assert len(redis.renewals) >= 2, "the slot was not renewed while the job ran"
    key, slot_id, now, expiry, key_ttl = redis.renewals[0]
    assert key == "cara:concurrency:provider:amazon"
    assert float(expiry) - float(now) == pytest.approx(middleware.slot_ttl)
    assert key_ttl == str(middleware.slot_ttl + 60)
    assert redis.zrems == [(key, slot_id)]
    assert LeaseHeartbeat.live() == []


def test_renewal_never_revives_an_expired_or_pruned_slot():
    """The script refuses a member whose score has passed: an acquirer may
    have pruned it and taken its place, and reviving it would overfill."""
    script = ConcurrencyLimited._RENEW_LUA

    assert "ZSCORE" in script
    assert "tonumber(score) <= tonumber(ARGV[2])" in script
    assert "'XX'" in script, "a renewal must never ADD a member"


def test_the_default_slot_ttl_is_the_dead_worker_horizon():
    assert ConcurrencyLimited().slot_ttl == ConcurrencyLimited.DEFAULT_SLOT_TTL == 120
