"""The lease winner reads the result once more before it runs the body.

The gate asked "is this job done?" and then took the lease, two round trips
with the lifecycle hook between them. A duplicate dispatch whose first read
missed could meet a key the owner had JUST freed: the owner caches its result,
then releases its lease, so the duplicate's ``SET NX`` succeeded and the body
ran a second time although a completed result for the same identity existed
(audit, 2026-11-23: ``ChargeJob(order_id=7)`` executed by both racers).

An owner caches before it releases, so whoever can take the lease can also
read any finished owner's result. One read after the acquire closes the
window; these tests drive the interleave through the gate's own lifecycle
hook, which runs exactly between the first read and the acquire.
"""

from __future__ import annotations

import asyncio
import sys
import threading

import pytest

from cara.queues.idempotency import MakesIdempotentBase
from cara.queues.LeaseHeartbeat import LeaseHeartbeat
from cara.testing.fakes.CacheFake import CacheFake

_cooldown = sys.modules["cara.queues.idempotency.ClaimsSourceCooldown"]
_orchestrator = sys.modules["cara.queues.idempotency.MakesIdempotentBase"]


@pytest.fixture
def cache(monkeypatch):
    fake = CacheFake()
    monkeypatch.setattr(_cooldown, "Cache", fake)
    monkeypatch.setattr(_orchestrator, "Cache", fake)
    monkeypatch.setattr(_cooldown, "config", lambda key, default=None: default)
    yield fake
    for heartbeat in LeaseHeartbeat.live():
        heartbeat.stop()


class _Charge(MakesIdempotentBase):
    idempotency_params = ("order_id",)

    def __init__(self, name: str, executions: list[str], result="charged") -> None:
        super().__init__()
        self.order_id = 7
        self._name = name
        self._executions = executions
        self._result = result
        self._between_read_and_lease = None
        self._outcomes: list[str] = []

    def should_execute_based_on_lifecycle(self) -> bool:
        if self._between_read_and_lease is not None:
            self._between_read_and_lease()
        return True

    def _emit_idempotency_metric(self, outcome: str) -> None:
        self._outcomes.append(outcome)

    async def body(self):
        self._executions.append(self._name)
        if isinstance(self._result, Exception):
            raise self._result
        return self._result


def _runs_to_completion(job: _Charge, raised: list[BaseException] | None = None):
    """A whole run of ``job`` on another worker (its own thread and loop).

    The hook that calls this runs inside the duplicate's event loop, so the
    other worker needs a thread of its own; what its run raised is collected
    in ``raised`` (a thread's exception never reaches the test otherwise).
    """

    def _run() -> None:
        try:
            asyncio.run(job.wrap_with_idempotency(job.body))
        except Exception as exc:
            if raised is None:
                raise
            raised.append(exc)

    def _other_worker() -> None:
        thread = threading.Thread(target=_run)
        thread.start()
        thread.join()

    return _other_worker


def test_a_duplicate_that_wins_a_just_freed_lease_does_not_run_again(cache):
    executions: list[str] = []
    owner = _Charge("owner", executions, result={"charged": 7})
    duplicate = _Charge("duplicate", executions, result={"charged": "twice"})
    duplicate._between_read_and_lease = _runs_to_completion(owner)

    answered = asyncio.run(duplicate.wrap_with_idempotency(duplicate.body))

    assert executions == ["owner"], "the body ran for a job that had completed"
    assert answered == {"charged": 7}
    # It is a collision like any other duplicate, never a fresh run.
    assert duplicate._outcomes == ["collision"]
    # The lease it took to find that out is free again, and still the owner's
    # result is what the key answers.
    assert not cache.has(duplicate._lock_key())
    assert duplicate._idempotency_lock_value is None
    assert cache.get(duplicate._result_key()) == {"charged": 7}


def test_an_owner_that_returned_none_still_stops_the_duplicate(cache):
    """A bare None is stored as the sentinel: it is a completion too."""
    executions: list[str] = []
    owner = _Charge("owner", executions, result=None)
    duplicate = _Charge("duplicate", executions, result="ran anyway")
    duplicate._between_read_and_lease = _runs_to_completion(owner)

    assert asyncio.run(duplicate.wrap_with_idempotency(duplicate.body)) is None
    assert executions == ["owner"]


def test_a_free_lease_with_no_result_still_runs(cache):
    """The re-read only stands a run down on a completed result: an owner
    that failed (nothing cached) leaves the work for the next dispatch."""
    executions: list[str] = []
    refused = RuntimeError("upstream refused")
    owner = _Charge("owner", executions, result=refused)
    raised: list[BaseException] = []

    duplicate = _Charge("duplicate", executions)
    duplicate._between_read_and_lease = _runs_to_completion(owner, raised)

    assert asyncio.run(duplicate.wrap_with_idempotency(duplicate.body)) == "charged"
    assert raised == [refused]
    assert executions == ["owner", "duplicate"]
    assert duplicate._outcomes == ["fresh"]


def test_a_forced_run_is_not_answered_by_the_reread(cache):
    """``force`` bypasses completed-result evidence at the first read; the
    read after the lease must not quietly bring that evidence back."""
    executions: list[str] = []
    owner = _Charge("owner", executions)
    asyncio.run(owner.wrap_with_idempotency(owner.body))
    forced = _Charge("forced", executions, result="recovered")

    answered = asyncio.run(
        forced.wrap_with_idempotency(forced.body, force_execution=True)
    )

    assert answered == "recovered"
    assert executions == ["owner", "forced"]


def test_a_job_without_a_result_cache_is_not_answered_by_the_reread(cache):
    """A recurring job (``idempotency_cache_results = False``) never reads the
    result cache; an entry under its key must not stop its tick."""
    executions: list[str] = []

    class _Tick(_Charge):
        idempotency_cache_results = False

    tick = _Tick("tick", executions, result="ticked")
    tick._idempotency_key = tick.generate_idempotency_key()
    cache.put(tick._result_key(), "an earlier tick", 300)

    assert asyncio.run(tick.wrap_with_idempotency(tick.body)) == "ticked"
    assert executions == ["tick"]


def test_a_reread_hit_hands_back_the_cooldown_the_hook_claimed(cache):
    """A run answered from the cache asked nothing of the source, so it keeps
    no cooldown window — the same rule a waiter follows."""
    executions: list[str] = []

    class _Poll(_Charge):
        source_cooldown_minutes = {"orders": 5}

        def __init__(self, *args, **kwargs) -> None:
            super().__init__(*args, **kwargs)
            self.source = "orders"

        def should_execute_based_on_lifecycle(self) -> bool:
            claimed = self._claim_source_cooldown()
            super().should_execute_based_on_lifecycle()
            return claimed

    # The owner is an operator's forced run of the same identity: it skips
    # the cooldown, so it can finish while the poll already holds the window.
    owner = _Poll("owner", executions)
    owner.force = True
    poll = _Poll("poll", executions)
    poll._between_read_and_lease = _runs_to_completion(owner)

    assert asyncio.run(poll.wrap_with_idempotency(poll.body)) == "charged"

    assert executions == ["owner"]
    assert poll._idempotency_cooldown_claim is None
    assert not cache.has("collection_cooldown:orders"), "an unspent window was kept"
