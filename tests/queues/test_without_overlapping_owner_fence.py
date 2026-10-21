"""``WithoutOverlapping`` must be OWNER-FENCED and TTL-sized above the job.

Pre-fix the middleware acquired with ``add(key, "1", expire_after)`` and released
with a bare ``forget(key)`` — no owner token. Two failure modes followed:

  * **Wrong-lock delete.** If a job overran its lock TTL (the key lapsed and a
    peer/pod re-acquired), the original holder's ``finally`` deleted the PEER's
    freshly-acquired lock, so a *third* copy could start while two were running.
  * **Mid-run lapse.** ``expire_after`` was independent of the job's runtime, so
    a sweep that ran longer than ``expire_after`` had its lock TTL-expire
    mid-run and a second copy fired — the user-facing duplicate (e.g. a price
    alert delivered twice).

The fix stores a unique ``{pid}:{uuid}`` owner and releases via
``forget_if(key, owner)`` (compare-and-delete), and sizes the TTL as
``max(expire_after, job.timeout + buffer)``.
"""

from __future__ import annotations

import asyncio

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from cara.queues.contracts.JobThrottledException import JobThrottledException
from cara.queues.middleware.WithoutOverlapping import WithoutOverlapping
from cara.testing.fakes.CacheFake import CacheFake


class _Job:
    def __init__(self, timeout: int = 60) -> None:
        self.timeout = timeout


def test_effective_ttl_outlasts_job_timeout() -> None:
    mw = WithoutOverlapping(key="k", expire_after=100)
    # Long job: TTL must be timeout + buffer (well above expire_after).
    assert mw._effective_ttl(_Job(timeout=1800)) == 1800 + 300
    # Short job: expire_after floor vs timeout+buffer — the larger wins.
    assert mw._effective_ttl(_Job(timeout=10)) == max(100, 10 + 300)
    # Missing/zero timeout falls back to the expire_after floor.
    assert mw._effective_ttl(_Job(timeout=0)) == max(100, 300)


def test_release_is_owner_fenced_and_never_deletes_a_peers_lock(monkeypatch) -> None:
    cache = CacheFake()
    mw = WithoutOverlapping(key="sweep", expire_after=100)
    monkeypatch.setattr(mw, "_resolve_cache", lambda: cache)
    redis_key = f"{mw.REDIS_KEY_PREFIX}sweep"

    async def _next(job):
        # Simulate: our lock TTL lapsed mid-run and a PEER re-acquired it with
        # its OWN owner token.
        cache.forget(redis_key)
        assert cache.add(redis_key, "peer-owner", 100) is True
        return "did-work"

    result = asyncio.run(mw.handle(_Job(timeout=60), _next))

    assert result == "did-work"
    # The original holder's ``finally`` ran forget_if(key, its-own-owner); with
    # the owner fence it must NOT have deleted the peer's freshly-acquired lock.
    assert cache.get(redis_key) == "peer-owner", (
        "owner fence failed — the finished job deleted a peer's lock, so a "
        "third overlapping copy could now acquire and run"
    )


def test_held_lock_skips_the_second_run(monkeypatch) -> None:
    cache = CacheFake()
    mw = WithoutOverlapping(key="dup", expire_after=100)
    monkeypatch.setattr(mw, "_resolve_cache", lambda: cache)
    redis_key = f"{mw.REDIS_KEY_PREFIX}dup"

    # A already holds the lock (owner token present).
    assert mw._try_acquire(cache, redis_key, "owner-A", 100) is True

    ran: list[str] = []

    async def _b_body(job):
        ran.append("B")
        return "B-done"

    # B's middleware run must find the key held and REFUSE to run the body.
    #
    # It must refuse by RAISING, not by returning. This assertion used to read
    # ``result_b is None`` — but ``None`` is also the normal success return of
    # an async handler, so the worker could not tell "skipped" from "done": it
    # settled the delivery ``completed`` and ACKed, discarding B while the
    # ledger reported it finished. The throttle signal is unambiguous and
    # carries the delay after which the peer's lease could have expired.
    with pytest.raises(JobThrottledException) as raised:
        asyncio.run(mw.handle(_Job(timeout=60), _b_body))

    assert ran == [], "B's body must not run while the lock is held"
    assert raised.value.key == "dup"
    assert raised.value.retry_after >= 1
    # The starvation lane, not the failure budget: B is healthy, it just lost
    # the lock lottery.
    assert JobThrottledException.is_throttle is True


class _FaultingCache(CacheFake):
    """A cache whose backend raises redis-py's OWN ``ConnectionError``.

    That class descends from ``redis.exceptions.RedisError`` only — not from
    the builtin ``ConnectionError`` the middleware used to name — so every
    degradation handler was dead code and a Redis outage escaped as a job
    failure.
    """

    def __init__(self, *, fail: set[str]) -> None:
        super().__init__()
        self._fail = fail

    def _maybe_fail(self, op: str) -> None:
        if op in self._fail:
            raise RedisConnectionError(f"{op}: connection reset")

    def add(self, key, value, ttl=None):
        self._maybe_fail("add")
        return super().add(key, value, ttl)

    def has(self, key):
        self._maybe_fail("has")
        return super().has(key)

    def forget_if(self, key, expected_value):
        self._maybe_fail("forget_if")
        return super().forget_if(key, expected_value)


class _AddlessFaultingCache(_FaultingCache):
    """Same backend, minus ``add`` — drives the has + put fallback."""

    add = None


def test_release_fault_keeps_the_successful_result(monkeypatch) -> None:
    cache = _FaultingCache(fail={"forget_if"})
    mw = WithoutOverlapping(key="sweep", expire_after=100)
    monkeypatch.setattr(mw, "_resolve_cache", lambda: cache)

    async def _next(job):
        return "did-work"

    # A raise out of the release ``finally`` replaced this return value, so a
    # job that FINISHED settled as failed and ran its side effects again.
    assert asyncio.run(mw.handle(_Job(timeout=60), _next)) == "did-work"


def test_release_fault_does_not_mask_the_bodys_own_error(monkeypatch) -> None:
    cache = _FaultingCache(fail={"forget_if"})
    mw = WithoutOverlapping(key="sweep", expire_after=100)
    monkeypatch.setattr(mw, "_resolve_cache", lambda: cache)

    async def _next(job):
        raise ValueError("body failed")

    with pytest.raises(ValueError, match="body failed"):
        asyncio.run(mw.handle(_Job(timeout=60), _next))


@pytest.mark.parametrize(
    "cache",
    [_FaultingCache(fail={"add"}), _AddlessFaultingCache(fail={"has"})],
    ids=["add", "has-fallback"],
)
def test_acquire_fault_throttles_and_never_runs_the_body(monkeypatch, cache) -> None:
    mw = WithoutOverlapping(key="sweep", expire_after=100)
    monkeypatch.setattr(mw, "_resolve_cache", lambda: cache)
    ran: list[str] = []

    async def _next(job):
        ran.append("ran")

    # Fail CLOSED on the throttle lane: a Redis outage must neither spend the
    # failure budget (the error used to escape as a job failure) nor degrade
    # to the process-local dict (every pod would then run its own copy).
    with pytest.raises(JobThrottledException) as raised:
        asyncio.run(mw.handle(_Job(timeout=60), _next))

    assert ran == []
    assert raised.value.key == "sweep"
    assert raised.value.retry_after == mw._effective_ttl(_Job(timeout=60))


def test_absent_container_uses_the_process_local_lock(monkeypatch) -> None:
    import builtins

    monkeypatch.delattr(builtins, "app", raising=False)
    assert WithoutOverlapping._resolve_cache() is None
