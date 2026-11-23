"""``RateLimited`` is ONE budget for every worker process.

The live defect: each worker process kept its own list of run timestamps, so
N processes ran N times ``max_attempts`` per ``decay_seconds`` — a limit
sized to a marketplace API's quota did not hold the moment a pool scaled out.
Every run now spends from the shared rate authority (the cache's atomic GCRA
cell). When that store cannot answer, the call falls back to the process's
own bucket — neither stopping every job nor lifting the limit — and the
outage is logged once, not once per job.

Two processes are one module with its in-process state swapped; the cache
and the clock are the fakes in ``_shared_rate_store``.
"""

from __future__ import annotations

import asyncio
import importlib

import pytest

from cara.queues.contracts.JobThrottledException import JobThrottledException
from tests.queues._shared_rate_store import as_another_process, install

_module = importlib.import_module("cara.queues.middleware.RateLimited")


class _Job:
    pass


@pytest.fixture
def shared(monkeypatch):
    monkeypatch.setattr(_module, "_rate_buckets", {})
    return install(monkeypatch, _module)


def _run(limiter, ran: list) -> None:
    async def body(_job):
        ran.append(1)

    asyncio.run(limiter.handle(_Job(), body))


def _another_process(monkeypatch) -> None:
    as_another_process(monkeypatch, _module, "_rate_buckets")


def test_two_worker_processes_spend_one_budget(shared, monkeypatch) -> None:
    limiter = _module.RateLimited(max_attempts=2, decay_seconds=60, key="ebay_scrape")
    ran: list = []

    _run(limiter, ran)
    _another_process(monkeypatch)
    _run(limiter, ran)
    _another_process(monkeypatch)

    with pytest.raises(JobThrottledException) as refused:
        _run(limiter, ran)

    assert ran == [1, 1], "a third process must not get a budget of its own"
    assert refused.value.key == "ebay_scrape"
    # 2 per 60s refills one run every 30s: the refused job is told 30, not 60.
    assert refused.value.retry_after == 30


def test_a_refused_run_spends_nothing_and_fits_when_it_was_told(
    shared, monkeypatch
) -> None:
    _store, clock, _log = shared
    limiter = _module.RateLimited(max_attempts=1, decay_seconds=60, key="reco")
    ran: list = []
    _run(limiter, ran)
    for _ in range(3):
        with pytest.raises(JobThrottledException) as refused:
            _run(limiter, ran)
    assert refused.value.retry_after == 60

    clock.now += refused.value.retry_after
    _another_process(monkeypatch)
    _run(limiter, ran)
    assert ran == [1, 1]


def test_an_unreachable_store_falls_back_to_the_process_bucket(shared) -> None:
    store, _clock, outage_log = shared
    store.down = True
    limiter = _module.RateLimited(max_attempts=2, decay_seconds=60, key="reco")
    ran: list = []

    _run(limiter, ran)
    _run(limiter, ran)
    for _ in range(5):
        with pytest.raises(JobThrottledException):
            _run(limiter, ran)

    assert ran == [1, 1], "the outage neither stops every job nor lifts the limit"
    assert len(outage_log.warnings) == 1, "one line per outage, not one per job"
    assert "reco" in outage_log.warnings[0]


def test_the_fallback_starts_from_the_runs_the_shared_budget_admitted(shared) -> None:
    store, _clock, _log = shared
    limiter = _module.RateLimited(max_attempts=1, decay_seconds=60, key="reco")
    ran: list = []
    _run(limiter, ran)

    store.down = True
    with pytest.raises(JobThrottledException):
        _run(limiter, ran)

    assert ran == [1], "an outage must not hand the process a fresh budget"


def test_the_shared_budget_resumes_and_says_so_once(shared, monkeypatch) -> None:
    store, clock, outage_log = shared
    limiter = _module.RateLimited(max_attempts=1, decay_seconds=60, key="reco")
    ran: list = []
    store.down = True
    _run(limiter, ran)

    clock.now += 61
    store.down = False
    _run(limiter, ran)
    _another_process(monkeypatch)
    with pytest.raises(JobThrottledException):
        _run(limiter, ran)

    assert ran == [1, 1]
    assert len(outage_log.warnings) == 2
    assert "answers again" in outage_log.warnings[1]
