"""``ThrottlesExceptions`` counts failures and shuts its gate for EVERY worker.

The live defect: the failure history and the gate lived in each worker
process's memory. One process stood the job class down while its siblings
kept calling the broken downstream, and N processes needed N times
``max_exceptions`` failures before the class stood down everywhere. Failures
now count in one shared cache counter whose window opens at the first failure
— exact however the failures are spaced — and the gate is one cache entry. When
the store cannot answer, the process's own count and gate decide for that
call, and the outage is logged once.

Two processes are one module with its in-process state swapped; the cache
and the clock are the fakes in ``_shared_rate_store``.
"""

from __future__ import annotations

import asyncio
import sys

import pytest

from cara.queues.contracts import JobThrottledException
from cara.queues.middleware import ThrottlesExceptions
from tests.queues._shared_rate_store import as_another_process, install

_module = sys.modules["cara.queues.middleware.ThrottlesExceptions"]


class _Job:
    pass


@pytest.fixture
def shared(monkeypatch):
    _another_process(monkeypatch)
    return install(monkeypatch, _module)


def _another_process(monkeypatch) -> None:
    as_another_process(monkeypatch, _module, "_exception_buckets", "_throttle_gates")


async def _fails(_job):
    raise RuntimeError("upstream 503")


async def _ok(_job) -> str:
    return "ran"


def _run(gate: ThrottlesExceptions, next_fn):
    return asyncio.run(gate.handle(_Job(), next_fn))


def _fail(gate: ThrottlesExceptions) -> None:
    with pytest.raises(RuntimeError):
        _run(gate, _fails)


def _gate(max_exceptions: int = 2) -> ThrottlesExceptions:
    return ThrottlesExceptions(
        max_exceptions=max_exceptions, decay_seconds=60, retry_after=300, key="collect"
    )


def test_failures_in_different_processes_add_up(shared, monkeypatch) -> None:
    _fail(_gate())
    _another_process(monkeypatch)
    _fail(_gate())

    _another_process(monkeypatch)
    with pytest.raises(JobThrottledException) as refused:
        _run(_gate(), _ok)
    assert refused.value.retry_after == 300
    assert refused.value.key == "collect"


def test_a_gate_one_process_shut_hands_the_others_the_remaining_wait(
    shared, monkeypatch
) -> None:
    _store, clock, _log = shared
    _fail(_gate(max_exceptions=1))
    clock.now += 120.4

    _another_process(monkeypatch)
    with pytest.raises(JobThrottledException) as refused:
        _run(_gate(max_exceptions=1), _ok)
    assert refused.value.retry_after == 180

    clock.now += 180
    assert _run(_gate(max_exceptions=1), _ok) == "ran"


def test_a_reopened_gate_counts_afresh_in_every_process(shared, monkeypatch) -> None:
    _store, clock, _log = shared
    _fail(_gate())
    _fail(_gate())
    clock.now += 300

    _another_process(monkeypatch)
    _fail(_gate())
    assert _run(_gate(), _ok) == "ran"


def test_n_failures_spread_evenly_across_the_period_close_the_gate(
    shared, monkeypatch
) -> None:
    """Cheapa's AI warmers: 3 per 300s. One failure every 100s is three in the
    period and must trip the breaker — a refilling bucket never did."""
    _store, clock, _log = shared
    gate = ThrottlesExceptions(
        max_exceptions=3, decay_seconds=300, retry_after=900, key="warm"
    )
    for _ in range(3):
        _another_process(monkeypatch)
        _fail(gate)
        clock.now += 99.9

    _another_process(monkeypatch)
    with pytest.raises(JobThrottledException) as refused:
        _run(gate, _ok)
    assert refused.value.retry_after == 900 - 99  # ceil of 900 - 99.9


def test_a_failure_after_the_period_has_run_out_starts_a_new_count(
    shared, monkeypatch
) -> None:
    _store, clock, _log = shared
    _fail(_gate())
    clock.now += 60  # the window opened by the first failure is over

    _another_process(monkeypatch)
    _fail(_gate())
    assert _run(_gate(), _ok) == "ran", "one failure in this window, not two"

    clock.now += 59
    _fail(_gate())
    with pytest.raises(JobThrottledException):
        _run(_gate(), _ok)


def test_an_unreachable_store_falls_back_to_the_process_count(shared) -> None:
    store, _clock, outage_log = shared
    store.down = True

    _fail(_gate())
    assert _run(_gate(), _ok) == "ran", "the outage does not shut the gate"
    _fail(_gate())
    for _ in range(3):
        with pytest.raises(JobThrottledException):
            _run(_gate(), _ok)

    assert len(outage_log.warnings) == 1, "one line per outage, not one per job"


def test_an_outage_does_not_reopen_a_gate_this_process_saw_shut(shared) -> None:
    store, clock, _log = shared
    _fail(_gate())
    _fail(_gate())

    store.down = True
    clock.now += 100
    with pytest.raises(JobThrottledException) as refused:
        _run(_gate(), _ok)
    assert refused.value.retry_after == 200
