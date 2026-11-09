"""A throttled run is put back; it did not fail, so it logs no error.

Live, 2026-11-09: each ``PullChannelReturnsJob`` that Amazon answered with
"reports still preparing" wrote ``[E] Job failed`` from the idempotency gate,
then ``[I] deferred`` from the job runner, 16 false errors in about two hours.
The family is whatever declares ``is_throttle``, the marker the worker settles
on, so a throttle that is not a ``JobThrottledException`` counts as well. The
lease still has to go: a throttled run that kept it would block its own
redelivery.
"""

from __future__ import annotations

import asyncio
import sys

import pytest

from cara.exceptions import IdempotencyGateFaultException, IdempotencyOverlapException
from cara.queues.contracts import JobThrottledException
from cara.queues.idempotency import MakesIdempotentBase
from cara.queues.LeaseHeartbeat import LeaseHeartbeat
from cara.queues.middleware.ConcurrencyBackendUnavailable import (
    ConcurrencyBackendUnavailable,
)
from cara.queues.middleware.ConcurrencyExceeded import ConcurrencyExceeded
from cara.testing.fakes.CacheFake import CacheFake

cooldown_module = sys.modules["cara.queues.idempotency.ClaimsSourceCooldown"]
orchestrator = sys.modules["cara.queues.idempotency.MakesIdempotentBase"]


class _ReportsStillPreparing(JobThrottledException):
    """Same shape as the Amazon package's ``ReportsPreparing``."""

    def __init__(self, read_key: str, retry_after: int) -> None:
        super().__init__(
            f"reports of {read_key} still preparing",
            key=read_key,
            retry_after=retry_after,
        )


class _RecordingLog:
    def __init__(self) -> None:
        self.errors: list[tuple] = []

    def debug(self, *args, **kwargs) -> None:
        pass

    def info(self, *args, **kwargs) -> None:
        pass

    def warning(self, *args, **kwargs) -> None:
        pass

    def error(self, *args, **kwargs) -> None:
        self.errors.append(args)


class _PullReturns(MakesIdempotentBase):
    idempotency_cache_results = False
    timeout = 3600

    def __init__(self) -> None:
        super().__init__()
        self.channel_id = 1920


@pytest.fixture
def cache(monkeypatch):
    fake = CacheFake()
    monkeypatch.setattr(cooldown_module, "Cache", fake)
    monkeypatch.setattr(orchestrator, "Cache", fake)
    monkeypatch.setattr(cooldown_module, "config", lambda key, default=None: default)
    yield fake
    for heartbeat in LeaseHeartbeat.live():
        heartbeat.stop()


@pytest.fixture
def log(monkeypatch):
    recording = _RecordingLog()
    monkeypatch.setattr(orchestrator, "Log", recording)
    return recording


def _run_raising(exc: Exception, cache: CacheFake) -> str:
    job = _PullReturns()
    job._idempotency_key = job.generate_idempotency_key()
    lease_key = job._lock_key()
    seen_during_run: list[bool] = []

    async def callback():
        seen_during_run.append(cache.has(lease_key))
        raise exc

    with pytest.raises(type(exc)) as raised:
        asyncio.run(job.wrap_with_idempotency(callback))

    assert raised.value is exc
    assert seen_during_run == [True], "the callback ran without the lease"
    return lease_key


@pytest.mark.parametrize(
    "throttle",
    [
        JobThrottledException("throttled", key="k", retry_after=60),
        _ReportsStillPreparing("channel:1920:returns", 60),
        ConcurrencyExceeded("over the ceiling"),
        IdempotencyOverlapException("a nested gate is held"),
        IdempotencyGateFaultException("a nested gate lost its cache", retry_after=10),
    ],
    ids=lambda exc: type(exc).__name__,
)
def test_a_throttle_logs_no_error_and_gives_the_lease_back(cache, log, throttle):
    lease_key = _run_raising(throttle, cache)

    assert log.errors == [], "a throttle was logged as a job failure"
    assert not cache.has(lease_key), "the throttled run kept its lease"
    assert LeaseHeartbeat.live() == []


@pytest.mark.parametrize(
    "failure",
    [
        ValueError("the callback broke"),
        # Named after the family and declared out of it: a deployment mistake
        # that has to reach the dead-letter queue and page.
        ConcurrencyBackendUnavailable("no atomic cache driver"),
    ],
    ids=lambda exc: type(exc).__name__,
)
def test_a_real_failure_still_logs_and_gives_the_lease_back(cache, log, failure):
    lease_key = _run_raising(failure, cache)

    assert len(log.errors) == 1
    assert log.errors[0][0] == "Job failed: %s - %s"
    assert log.errors[0][2] is failure
    assert not cache.has(lease_key), "the failed run kept its lease"
    assert LeaseHeartbeat.live() == []
