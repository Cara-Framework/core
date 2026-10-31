"""A redelivery of a job whose worker DIED re-owns that run's cooldown.

A raising run hands its cooldown claim back; a SIGKILLed one cannot. Its
window outlived it and met the very redelivery that retries it, which the
gate settled as done without running (live test, 2026-10-31: a Shopify
listing pull killed mid-walk was "completed in 0.002s" at its lease lapse,
the channel unpulled and its run orphaned RUNNING). The same durable job
(``_db_job_id``) may take the window over; every other dispatch still meets
the live cooldown.
"""

from __future__ import annotations

import asyncio
import sys

import pytest

from cara.queues.idempotency import MakesIdempotentBase
from cara.testing.fakes.CacheFake import CacheFake

module = sys.modules["cara.queues.idempotency.ClaimsSourceCooldown"]
orchestrator = sys.modules["cara.queues.idempotency.MakesIdempotentBase"]

COOLDOWN_KEY = "collection_cooldown:inventory:1922"


@pytest.fixture
def cache(monkeypatch):
    fake = CacheFake()
    monkeypatch.setattr(module, "Cache", fake)
    monkeypatch.setattr(orchestrator, "Cache", fake)
    monkeypatch.setattr(module, "config", lambda key, default=None: default)
    return fake


class _PullListings(MakesIdempotentBase):
    source_cooldown_minutes = {"inventory": 10}
    cooldown_grain_attrs = ("channel_id",)
    cooldown_requires_grains = True
    idempotency_cache_results = False

    def __init__(self, db_job_id: int | None = None) -> None:
        super().__init__()
        self.source = "inventory"
        self.channel_id = 1922
        if db_job_id is not None:
            # What the worker stamps on every delivery of one durable job.
            self._db_job_id = db_job_id

    def should_execute_based_on_lifecycle(self) -> bool:
        return self._claim_source_cooldown()


def _run(job, callback):
    return asyncio.run(job.wrap_with_idempotency(callback))


def _killed_run(cache, db_job_id: int) -> None:
    """A run that claimed the window and then died: no release ran."""
    job = _PullListings(db_job_id)
    assert job._claim_source_cooldown() is True
    # SIGKILL: neither the success path nor the exception path executes; the
    # job lock is a heartbeat lease that lapses on its own (not modelled).


def test_the_redelivery_of_a_killed_run_runs(cache):
    _killed_run(cache, db_job_id=77)
    ran: list[str] = []

    async def pull():
        ran.append("pulled")
        return "pulled"

    assert _run(_PullListings(77), pull) == "pulled"
    assert ran == ["pulled"]
    # It now owns a full window again, under its own name.
    assert str(cache.get(COOLDOWN_KEY)).endswith("|77")
    assert cache.ttl_of(COOLDOWN_KEY) == 10 * 60


def test_another_job_still_meets_the_dead_runs_window(cache):
    _killed_run(cache, db_job_id=77)

    async def never():
        raise AssertionError("a different dispatch must stay gated")

    assert _run(_PullListings(78), never) is None
    assert _run(_PullListings(), never) is None
    assert str(cache.get(COOLDOWN_KEY)).endswith("|77")


def test_a_completed_run_still_gates_its_next_tick(cache):
    async def pull():
        return "pulled"

    assert _run(_PullListings(90), pull) == "pulled"

    async def never():
        raise AssertionError("the next tick is a new job inside the window")

    assert _run(_PullListings(91), never) is None


def test_an_anonymous_window_is_never_taken_over(cache):
    """A sync dispatch (no durable job) claims anonymously, as before."""
    cache.add(COOLDOWN_KEY, "2026-10-31T00:00:00+00:00", 600)

    async def never():
        raise AssertionError("no owner to match")

    assert _run(_PullListings(77), never) is None
    assert cache.get(COOLDOWN_KEY) == "2026-10-31T00:00:00+00:00"


def test_a_failed_redelivery_still_hands_the_window_back(cache):
    _killed_run(cache, db_job_id=77)

    async def failing():
        raise ConnectionError("marketplace 503")

    with pytest.raises(ConnectionError):
        _run(_PullListings(77), failing)
    assert not cache.has(COOLDOWN_KEY)
