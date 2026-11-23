"""``ThrottlesExceptions`` — when the gate shuts, for how long, and on what.

Three defects, reproduced 2026-11-23 against the live middleware:

1. The gate and every failure were stamped with the failing job's START
   time. The gate is "``retry_after`` since the failure", so a body that hung
   180s before failing shut a 300s gate for 120s, and one that ran longer than
   ``retry_after`` never shut it.
2. A job refused inside the gate was handed the FULL ``retry_after``. The
   worker uses that as the floor of the redelivery delay, so a job arriving
   one second before the gate reopened waited the whole gate again.
3. Throttle signals from below (``is_throttle``: a full concurrency cap, the
   idempotency layer putting a job back) and permanent per-job outcomes
   (``do_not_retry``) were counted as downstream failures: three refusals shut
   the class down although nothing downstream had answered.

The clock is the middleware module's own ``time`` name, rebound to a fake that
also drives the shared store (``_shared_rate_store``) the gate lives in.
"""

from __future__ import annotations

import asyncio
import sys

import pytest

from cara.exceptions import IdempotencyGateFaultException
from cara.queues.contracts import JobCancelledException, JobThrottledException
from cara.queues.middleware import ThrottlesExceptions
from cara.queues.middleware.ConcurrencyExceeded import ConcurrencyExceeded
from tests.queues._shared_rate_store import Clock as _Clock
from tests.queues._shared_rate_store import install

_module = sys.modules["cara.queues.middleware.ThrottlesExceptions"]


class _Job:
    pass


@pytest.fixture
def clock(monkeypatch) -> _Clock:
    """The shared store and its clock: the path every healthy worker takes.

    The per-process fallback the gate uses while that store is down is pinned
    in ``test_throttles_exceptions_is_one_gate_for_every_worker``.
    """
    monkeypatch.setattr(_module, "_exception_buckets", {})
    monkeypatch.setattr(_module, "_throttle_gates", {})
    _store, fake, _log = install(monkeypatch, _module)
    return fake


def _failing_after(clock: _Clock, seconds: float, exc: Exception | None = None):
    """A job body that runs for ``seconds`` and then raises."""

    async def _next(_job):
        clock.now += seconds
        raise exc if exc is not None else RuntimeError("upstream 503")

    return _next


async def _ok(_job) -> str:
    return "ran"


def _run(middleware: ThrottlesExceptions, next_fn):
    return asyncio.run(middleware.handle(_Job(), next_fn))


def _fail(middleware: ThrottlesExceptions, next_fn, expected=RuntimeError) -> None:
    with pytest.raises(expected):
        _run(middleware, next_fn)


# ── (1) the stamp is the failure, not the job's start ──────────────────────
def test_the_gate_holds_retry_after_from_the_failure_not_from_the_job_start(clock):
    gate = ThrottlesExceptions(
        max_exceptions=1, decay_seconds=300, retry_after=300, key="collect"
    )
    _fail(gate, _failing_after(clock, 180))  # hung 180s, then failed
    failed_at = clock.now

    clock.now = failed_at + 299
    with pytest.raises(JobThrottledException):
        _run(gate, _ok)

    clock.now = failed_at + 300
    assert _run(gate, _ok) == "ran"


def test_a_body_that_outlived_retry_after_still_shuts_the_gate(clock):
    gate = ThrottlesExceptions(
        max_exceptions=1, decay_seconds=300, retry_after=300, key="collect"
    )
    _fail(gate, _failing_after(clock, 301))

    with pytest.raises(JobThrottledException):
        _run(gate, _ok)


def test_the_decay_window_is_measured_between_failures(clock):
    """Two slow bodies that START inside one window but FAIL further apart
    than it are not "two failures within ``decay_seconds``"."""
    gate = ThrottlesExceptions(
        max_exceptions=2, decay_seconds=60, retry_after=300, key="push"
    )
    _fail(gate, _failing_after(clock, 5))
    _fail(gate, _failing_after(clock, 90))  # started at once, failed 90s later

    assert _run(gate, _ok) == "ran"

    _fail(gate, _failing_after(clock, 1))
    with pytest.raises(JobThrottledException):
        _run(gate, _ok)


# ── (2) a refused job is told when the gate reopens ────────────────────────
@pytest.mark.parametrize(
    ("seconds_into_the_gate", "expected_wait"),
    [(0, 300), (0.5, 300), (120, 180), (299, 1), (299.9, 1)],
)
def test_a_refused_job_is_handed_the_remaining_wait(
    clock, seconds_into_the_gate, expected_wait
):
    gate = ThrottlesExceptions(
        max_exceptions=1, decay_seconds=300, retry_after=300, key="collect"
    )
    _fail(gate, _failing_after(clock, 0))
    clock.now += seconds_into_the_gate

    with pytest.raises(JobThrottledException) as refused:
        _run(gate, _ok)

    assert refused.value.retry_after == expected_wait
    assert refused.value.key == "collect"
    # A whole number of seconds, never zero: the worker reads it as a delay.
    assert isinstance(refused.value.retry_after, int)


# ── (3) what counts as a failure ───────────────────────────────────────────
@pytest.mark.parametrize(
    "not_a_failure",
    [
        ConcurrencyExceeded("cap full"),
        JobThrottledException("cooling down", retry_after=5),
        IdempotencyGateFaultException("cache unreachable", retry_after=10),
        JobCancelledException("cancelled by an operator"),
    ],
    ids=["concurrency", "throttle", "gate-fault", "cancelled"],
)
def test_throttles_and_permanent_outcomes_from_below_do_not_shut_the_gate(
    clock, not_a_failure
):
    gate = ThrottlesExceptions(
        max_exceptions=3, decay_seconds=60, retry_after=300, key="push"
    )
    for _ in range(5):
        # The outcome still reaches the worker untouched: it is not swallowed,
        # only not counted.
        _fail(gate, _failing_after(clock, 0, not_a_failure), type(not_a_failure))

    assert _run(gate, _ok) == "ran"
    assert "push" not in _module._throttle_gates
    assert not _module._exception_buckets.get("push")


def test_real_failures_still_shut_the_gate_at_the_threshold(clock):
    gate = ThrottlesExceptions(
        max_exceptions=3, decay_seconds=60, retry_after=300, key="push"
    )
    _fail(gate, _failing_after(clock, 1))
    _fail(gate, _failing_after(clock, 1, ConcurrencyExceeded("cap full")), Exception)
    _fail(gate, _failing_after(clock, 1))
    assert _run(gate, _ok) == "ran", "two failures and a refusal are not three"

    _fail(gate, _failing_after(clock, 1))
    with pytest.raises(JobThrottledException) as refused:
        _run(gate, _ok)
    assert refused.value.retry_after == 300


def test_an_expired_gate_starts_counting_afresh(clock):
    gate = ThrottlesExceptions(
        max_exceptions=2, decay_seconds=600, retry_after=300, key="push"
    )
    _fail(gate, _failing_after(clock, 0))
    _fail(gate, _failing_after(clock, 0))
    clock.now += 300

    assert _run(gate, _ok) == "ran"
    # One failure after the gate reopened is one, not the third of the old run.
    _fail(gate, _failing_after(clock, 0))
    assert _run(gate, _ok) == "ran"
