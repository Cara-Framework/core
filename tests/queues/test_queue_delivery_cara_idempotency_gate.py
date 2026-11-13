"""What a cache outage means depends on which side of the callback it lands.

Before the callback the gate could not ask, so the job goes back to the queue
without spending an attempt; a failure here dead-lettered jobs that never ran.
After the callback the work is done, so a lost write must not undo that. The
callback's own cache errors are neither: they are its failures.
"""

from __future__ import annotations

import asyncio
import importlib
import sys
from types import SimpleNamespace

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError

from cara.context import Tenancy
from cara.exceptions import IdempotencyGateFaultException
from cara.queues.idempotency import MakesIdempotentBase
from cara.testing.fakes.CacheFake import CacheFake

_cooldown = sys.modules["cara.queues.idempotency.ClaimsSourceCooldown"]
_orchestrator = sys.modules["cara.queues.idempotency.MakesIdempotentBase"]
_bus = importlib.import_module("cara.queues.Bus")

_OUTAGE = RedisConnectionError("Error 61 connecting to redis. Connection refused.")
_OOM = ResponseError("OOM command not allowed when used memory > 'maxmemory'.")


class _BrokenCache(CacheFake):
    """A real fake that fails exactly the named operations."""

    def __init__(self, broken: dict[str, Exception]) -> None:
        super().__init__()
        self.broken = broken

    def _maybe_raise(self, operation: str) -> None:
        if operation in self.broken:
            raise self.broken[operation]

    def has(self, *args, **kwargs):
        self._maybe_raise("has")
        return super().has(*args, **kwargs)

    def get(self, *args, **kwargs):
        self._maybe_raise("get")
        return super().get(*args, **kwargs)

    def add(self, *args, **kwargs):
        self._maybe_raise("add")
        return super().add(*args, **kwargs)

    def increment(self, *args, **kwargs):
        self._maybe_raise("increment")
        return super().increment(*args, **kwargs)

    def put(self, *args, **kwargs):
        self._maybe_raise("put")
        return super().put(*args, **kwargs)


def _install(monkeypatch, cache: CacheFake) -> CacheFake:
    monkeypatch.setattr(_cooldown, "Cache", cache)
    monkeypatch.setattr(_orchestrator, "Cache", cache)
    monkeypatch.setattr(_cooldown, "config", lambda key, default=None: default)
    return cache


class _Job(MakesIdempotentBase):
    def __init__(self, ref: str = "r1") -> None:
        super().__init__()
        self.ref = ref


class _Poll(_Job):
    """The app-side poll shape: the lifecycle hook is the cooldown claim."""

    source_cooldown_minutes = {"orders": 5}
    idempotency_cache_results = False

    def __init__(self) -> None:
        super().__init__()
        self.source = "orders"

    def should_execute_based_on_lifecycle(self) -> bool:
        return self._claim_source_cooldown()


def _body_recorder() -> tuple[list[int], object]:
    ran: list[int] = []

    async def body() -> str:
        ran.append(1)
        return "done"

    return ran, body


@pytest.mark.parametrize(
    ("operation", "fault"),
    [
        ("has", _OUTAGE),
        ("add", _OUTAGE),
        # noeviction at maxmemory: reads still work, every write refuses.
        ("add", _OOM),
    ],
)
def test_a_gate_that_cannot_ask_the_cache_throttles_without_running(
    monkeypatch, operation, fault
):
    _install(monkeypatch, _BrokenCache({operation: fault}))
    ran, body = _body_recorder()

    with pytest.raises(IdempotencyGateFaultException) as refused:
        asyncio.run(_Job().wrap_with_idempotency(body))

    assert refused.value.is_throttle
    assert refused.value.retry_after == MakesIdempotentBase.GATE_FAULT_RETRY_SECONDS
    assert refused.value.__cause__ is fault
    assert ran == []


def test_a_fault_reading_a_completed_result_throttles(monkeypatch):
    cache = _install(monkeypatch, _BrokenCache({}))
    first = _Job()
    _, body = _body_recorder()
    asyncio.run(first.wrap_with_idempotency(body))
    cache.broken["get"] = _OUTAGE

    ran, again = _body_recorder()
    with pytest.raises(IdempotencyGateFaultException):
        asyncio.run(_Job().wrap_with_idempotency(again))
    assert ran == []


def test_a_fault_at_the_cooldown_claim_throttles(monkeypatch):
    _install(monkeypatch, _BrokenCache({"add": _OUTAGE}))
    ran, body = _body_recorder()

    with pytest.raises(IdempotencyGateFaultException):
        asyncio.run(_Poll().wrap_with_idempotency(body))
    assert ran == []


def test_a_fault_at_the_lease_check_throttles_and_hands_the_cooldown_back(monkeypatch):
    """A poll has no result cache: the lease acquire is its first ask after
    the cooldown claim (the claim's own ``add`` wins, the lease's fails)."""
    cache = _install(monkeypatch, _BrokenCache({}))
    job = _Poll()
    original_add = cache.add

    def _add_then_lose_the_cache(*args, **kwargs):
        if cache.broken:
            raise cache.broken["add"]
        won = original_add(*args, **kwargs)
        cache.broken["add"] = _OUTAGE
        return won

    cache.add = _add_then_lose_the_cache
    ran, body = _body_recorder()

    with pytest.raises(IdempotencyGateFaultException):
        asyncio.run(job.wrap_with_idempotency(body))

    cache.broken.clear()
    assert ran == []
    assert job._idempotency_cooldown_claim is None
    assert not cache.has("collection_cooldown:orders"), "the retry would meet it"


def test_a_fault_while_waiting_on_another_owner_throttles(monkeypatch):
    """A waiter never ran either; the outage must not read as its failure."""
    cache = _install(monkeypatch, _BrokenCache({}))
    job = _Job()
    job._idempotency_key = job.generate_idempotency_key()
    assert job.acquire_job_lock() is True

    async def _instant(_seconds: float) -> None:
        cache.broken["has"] = _OUTAGE

    monkeypatch.setattr(_orchestrator.asyncio, "sleep", _instant)
    ran, body = _body_recorder()

    with pytest.raises(IdempotencyGateFaultException):
        asyncio.run(_Job().wrap_with_idempotency(body))
    assert ran == []


def test_an_inline_dispatch_fails_at_its_caller_instead_of_skipping(monkeypatch):
    """``Bus`` settles a ``JobThrottledException`` as a quiet ``None``."""
    _install(monkeypatch, _BrokenCache({"has": _OUTAGE}))
    monkeypatch.setattr(
        _bus.facades,
        "DB",
        SimpleNamespace(
            transaction_level=lambda: 0,
            rollback_transactions_above=lambda _level: None,
            commit_transactions_above=lambda _level: None,
        ),
    )
    ran, body = _body_recorder()

    class _Inline(_Job):
        central_job = True

        async def handle(self) -> str:
            return await self.wrap_with_idempotency(body)

    with pytest.raises(IdempotencyGateFaultException), Tenancy.central():
        asyncio.run(_bus.Bus._run_sync_with_tracking(_Inline()))
    assert ran == []


def test_the_callbacks_own_cache_error_is_its_failure_not_a_throttle(monkeypatch):
    _install(monkeypatch, CacheFake())

    async def body() -> None:
        raise _OUTAGE

    with pytest.raises(RedisConnectionError):
        asyncio.run(_Job().wrap_with_idempotency(body))


def test_a_lost_result_write_keeps_the_finished_body_finished(monkeypatch):
    cache = _install(monkeypatch, _BrokenCache({"put": _OUTAGE}))
    job = _Job()
    ran, body = _body_recorder()

    assert asyncio.run(job.wrap_with_idempotency(body)) == "done"

    assert ran == [1]
    assert cache.has(job._lock_key()) is False, "the lease was not released"


def test_a_cooldown_spent_by_a_body_is_kept_when_its_result_write_fails(monkeypatch):
    cache = _install(monkeypatch, _BrokenCache({"put": _OUTAGE}))

    class _CachedPoll(_Poll):
        idempotency_cache_results = True

    job = _CachedPoll()
    ran, body = _body_recorder()

    assert asyncio.run(job.wrap_with_idempotency(body)) == "done"
    assert ran == [1]
    assert job._idempotency_cooldown_claim is not None
    assert cache.has(job._idempotency_cooldown_claim[0])
