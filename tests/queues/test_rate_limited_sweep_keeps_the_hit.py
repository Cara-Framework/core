"""The periodic bucket sweep must not throw away the hit being recorded.

Every 500th call ``RateLimited`` sweeps empty buckets out of the process
dict. Run after the call had taken its own (still empty) bucket, the sweep
popped that bucket too, the hit was appended to a list the dict no longer
held, and the limit let one extra run through.

The process dict decides only while the shared rate store cannot answer, so
the store is down here: the defect is in the fallback a worker relies on then.
"""

from __future__ import annotations

import asyncio
import importlib

import pytest

from cara.queues.contracts.JobThrottledException import JobThrottledException
from tests.queues._shared_rate_store import install

rate_limited = importlib.import_module("cara.queues.middleware.RateLimited")


def test_the_call_that_triggers_the_sweep_still_counts(monkeypatch) -> None:
    monkeypatch.setattr(rate_limited, "_rate_buckets", {})
    store, _clock, _log = install(monkeypatch, rate_limited)
    store.down = True
    # The very next call is the one that runs the sweep.
    monkeypatch.setattr(
        rate_limited, "_rate_sweep_counter", rate_limited._RATE_SWEEP_EVERY - 1
    )
    limiter = rate_limited.RateLimited(max_attempts=1, decay_seconds=3600, key="sweep")
    ran: list[int] = []

    async def body(_job):
        ran.append(1)

    async def scenario() -> None:
        await limiter.handle(object(), body)
        with pytest.raises(JobThrottledException):
            await limiter.handle(object(), body)

    asyncio.run(scenario())

    assert ran == [1]
    assert len(rate_limited._rate_buckets["sweep"]) == 1
