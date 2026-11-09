"""A cache fault while re-owning a cooldown is a gate fault, not "cooling down".

``_take_over_own_cooldown`` is the redelivery of a crashed poll asking for its
own window back. It catches every exception and answers "not taken", so the
gate reads a fault as a live window, returns ``None`` and the worker settles a
job whose body never ran as completed: the outcome the take-over exists to
prevent, and the one round 3's gate fault was added to stop everywhere else.
"""

from __future__ import annotations

import asyncio
import sys

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from cara.exceptions import IdempotencyGateFaultException
from cara.queues.idempotency import MakesIdempotentBase
from cara.testing.fakes.CacheFake import CacheFake

_cooldown = sys.modules["cara.queues.idempotency.ClaimsSourceCooldown"]
_orchestrator = sys.modules["cara.queues.idempotency.MakesIdempotentBase"]

_OUTAGE = RedisConnectionError("Error 61 connecting to redis. Connection refused.")
_WINDOW = "collection_cooldown:orders"
_DELIVERY = 42


class _BrokenCache(CacheFake):
    def __init__(self, broken: set[str]) -> None:
        super().__init__()
        self.broken = broken

    def get(self, *args, **kwargs):
        if "get" in self.broken:
            raise _OUTAGE
        return super().get(*args, **kwargs)

    def forget_if(self, *args, **kwargs):
        if "forget_if" in self.broken:
            raise _OUTAGE
        return super().forget_if(*args, **kwargs)


class _RedeliveredPoll(MakesIdempotentBase):
    source_cooldown_minutes = {"orders": 5}
    idempotency_cache_results = False

    def __init__(self) -> None:
        super().__init__()
        self.source = "orders"
        self._db_job_id = _DELIVERY

    def should_execute_based_on_lifecycle(self) -> bool:
        return self._claim_source_cooldown()


def _run(job) -> tuple[list[str], object]:
    ran: list[str] = []

    async def body():
        ran.append("body")
        return "done"

    return ran, asyncio.run(job.wrap_with_idempotency(body))


@pytest.mark.parametrize("operation", ["get", "forget_if"])
def test_a_fault_taking_over_the_crashed_runs_window_throttles(monkeypatch, operation):
    cache = _BrokenCache(set())
    monkeypatch.setattr(_cooldown, "Cache", cache)
    monkeypatch.setattr(_orchestrator, "Cache", cache)
    monkeypatch.setattr(_cooldown, "config", lambda key, default=None: default)
    # The window the same job's dead worker claimed and could not hand back.
    cache.add(_WINDOW, f"2026-11-09T08:00:00+00:00|{_DELIVERY}", 300)
    cache.broken.add(operation)

    with pytest.raises(IdempotencyGateFaultException):
        _run(_RedeliveredPoll())


def test_a_healthy_cache_still_takes_the_window_over(monkeypatch):
    cache = _BrokenCache(set())
    monkeypatch.setattr(_cooldown, "Cache", cache)
    monkeypatch.setattr(_orchestrator, "Cache", cache)
    monkeypatch.setattr(_cooldown, "config", lambda key, default=None: default)
    cache.add(_WINDOW, f"2026-11-09T08:00:00+00:00|{_DELIVERY}", 300)

    ran, result = _run(_RedeliveredPoll())

    assert ran == ["body"]
    assert result == "done"
