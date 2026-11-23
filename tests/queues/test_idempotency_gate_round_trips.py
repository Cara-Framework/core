"""A fresh idempotent run asks the cache five times, and leaves no counter behind.

Measured 2026-11-13 (1,000,000-job burst, scratch stack): every worker job
paid 11 Redis round trips, six of them the idempotency gate — result
EXISTS, lock EXISTS, fence INCR (an EVAL), lock SET NX, result SET, lock
release (an EVAL). Two of those bought nothing:

* the lock EXISTS before the acquire: ``Cache.add`` (SET NX) answers the
  same question atomically, and a lost race already takes the same branch;
* the ``job_fence:`` counter: its number was written into the lock value
  and never read by anything. The release and the renewal compare the
  whole owner payload, whose random ``owner`` already makes it unique, so
  a lapsed owner can never delete or extend the next one's lease without
  it. It cost one round trip per job and one key per job identity held
  for seven days — a million distinct jobs left a million counters in a
  ``noeviction`` Redis.

The count is five, not the four that measurement left, since 2026-11-23: the
lease winner reads the result once more. Without that read a duplicate whose
first read missed, and whose ``SET NX`` landed just after the owner cached its
result and released, ran the body a second time (see
``test_idempotency_lease_winner_rereads_result``). The first read is one
``GET`` (it was ``EXISTS``; a hit used to cost ``EXISTS`` + ``GET``).
"""

from __future__ import annotations

import asyncio
import sys

import pytest

from cara.queues.idempotency import MakesIdempotentBase
from cara.queues.LeaseHeartbeat import LeaseHeartbeat
from cara.testing.fakes.CacheFake import CacheFake

orchestrator = sys.modules["cara.queues.idempotency.MakesIdempotentBase"]


class _CountingCache(CacheFake):
    def __init__(self) -> None:
        super().__init__()
        self.calls: list[tuple[str, str]] = []

    def has(self, key):
        self.calls.append(("has", key.split(":")[0]))
        return super().has(key)

    def get(self, key, default=None, *, strict=True):
        self.calls.append(("get", key.split(":")[0]))
        return super().get(key, default, strict=strict)

    def add(self, key, value, ttl=None):
        self.calls.append(("add", key.split(":")[0]))
        return super().add(key, value, ttl)

    def put(self, key, value, ttl=None, **kwargs):
        self.calls.append(("put", key.split(":")[0]))
        return super().put(key, value, ttl, **kwargs)

    def increment(self, key, amount=1, ttl=None):
        self.calls.append(("increment", key.split(":")[0]))
        return super().increment(key, amount, ttl)

    def forget_if(self, key, expected_value):
        self.calls.append(("forget_if", key.split(":")[0]))
        return super().forget_if(key, expected_value)


@pytest.fixture
def cache(monkeypatch):
    fake = _CountingCache()
    monkeypatch.setattr(orchestrator, "Cache", fake)
    yield fake
    for heartbeat in LeaseHeartbeat.live():
        heartbeat.stop()


class _Push(MakesIdempotentBase):
    idempotency_params = ("task_id",)

    def __init__(self, task_id: int = 7) -> None:
        super().__init__()
        self.task_id = task_id


def test_a_fresh_run_asks_the_cache_five_times(cache):
    async def body():
        return "pushed"

    assert asyncio.run(_Push().wrap_with_idempotency(body)) == "pushed"

    # Five, deliberately: the second ``get`` is the lease winner's re-read,
    # the one round trip that stops a job that finished between the first
    # read and the acquire from running twice. Do not "optimise" it away.
    assert cache.calls == [
        ("get", "job_result"),
        ("add", "job_lock"),
        ("get", "job_result"),
        ("put", "job_result"),
        ("forget_if", "job_lock"),
    ]


def test_a_duplicate_of_a_finished_run_asks_the_cache_once(cache):
    async def body():
        return "pushed"

    asyncio.run(_Push().wrap_with_idempotency(body))
    cache.calls.clear()

    async def never():
        raise AssertionError("a completed result let the duplicate run")

    assert asyncio.run(_Push().wrap_with_idempotency(never)) == "pushed"

    # One GET answers "is it done" and carries the result: it was EXISTS
    # followed by GET, two round trips for every duplicate dispatch.
    assert cache.calls == [("get", "job_result")]


def test_a_waiter_reads_the_result_with_one_get_per_poll(cache, monkeypatch):
    owner = _Push()
    owner._idempotency_key = owner.generate_idempotency_key()
    assert owner.acquire_job_lock() is True

    async def _owner_finishes(_seconds):
        owner.cache_result(None)
        owner.release_job_lock()
        cache.calls.clear()

    monkeypatch.setattr(orchestrator.asyncio, "sleep", _owner_finishes)

    async def never():
        raise AssertionError("a held lease let a second run in")

    # The owner returned None: the waiter must see "completed", not "absent".
    assert asyncio.run(_Push().wrap_with_idempotency(never)) is None
    assert cache.calls == [("get", "job_result")]


def test_a_run_leaves_no_counter_behind(cache):
    async def body():
        return "pushed"

    asyncio.run(_Push().wrap_with_idempotency(body))

    assert not any(key.startswith("job_fence:") for key in cache.all()), cache.all()


def test_a_held_lease_still_turns_the_second_run_away(cache, monkeypatch):
    owner = _Push()
    owner._idempotency_key = owner.generate_idempotency_key()
    assert owner.acquire_job_lock() is True

    second = _Push()

    async def _waited():
        return None

    monkeypatch.setattr(second, "wait_for_completion", _waited)

    async def never():
        raise AssertionError("a held lease let a second run in")

    assert asyncio.run(second.wrap_with_idempotency(never)) is None
    owner.release_job_lock()


def test_the_lease_release_is_still_owner_fenced(cache):
    first = _Push()
    first._idempotency_key = first.generate_idempotency_key()
    assert first.acquire_job_lock() is True
    key = first._lock_key()
    # The first lease lapsed and the next run took the key.
    cache.forget(key)
    second = _Push()
    second._idempotency_key = second.generate_idempotency_key()
    assert second.acquire_job_lock() is True

    first.release_job_lock()

    assert cache.has(key), "a lapsed owner deleted the next owner's lease"
    second.release_job_lock()
