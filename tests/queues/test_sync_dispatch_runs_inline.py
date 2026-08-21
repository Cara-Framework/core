"""``Bus.dispatch`` under ``ExecutionContext.sync()`` runs INLINE.

This is the invariant that makes a ``--sync`` run equivalent to the
async worker path. If a dispatch inside a sync context were enqueued
instead, nothing would drain it — the command exits immediately after
— so the sync run would report success over a half-executed pipeline:
the first stage done, every follow-up stranded on a queue no worker is
reading. Synkronus's end-to-end verification harness runs its whole
pipeline this way, so a regression here turns ``e2e:verify`` green
against work that never happened.

Two halves are pinned together, because "runs inline" is only half the
contract: the publish path must ALSO be untouched. A job that both ran
inline and got published would double-execute once a worker picked it
up.
"""

from __future__ import annotations

import asyncio
import builtins
from typing import Any

import pytest

import cara.facades as facades
from cara.context import ExecutionContext, Tenancy
from cara.queues import Bus
from cara.queues.contracts import BaseJob


class _RecordingDB:
    """The narrow ``DB`` surface the sync dispatch path touches.

    Sync dispatch runs inline in the caller's task and shares its
    ContextVar-pinned connection registry, so it records the caller's
    transaction depth and unwinds only back to it. A framework unit
    test boots no container, so the facade is served from here.
    """

    def __init__(self) -> None:
        """Start with a clean call log at depth zero."""
        self.calls: list[tuple[str, int]] = []

    def transaction_level(self) -> int:
        """Report the caller's open-transaction depth."""
        return 0

    def commit_transactions_above(self, level: int) -> None:
        """Record a commit-unwind back to ``level``."""
        self.calls.append(("commit", level))

    def rollback_transactions_above(self, level: int) -> None:
        """Record a rollback-unwind back to ``level``."""
        self.calls.append(("rollback", level))


class _PublishRefusingJob(BaseJob):
    """Runs inline and treats any publish attempt as a hard failure.

    Overriding the ``dispatch`` classmethod is the sharpest available
    probe: it is the single entry point ``Bus`` uses to reach the queue
    rail, so a job that reaches it in sync mode fails loudly here
    instead of silently double-running in production.
    """

    central_job = True
    default_queue = "jobs"
    ran = 0
    saw_sync: bool | None = None

    @classmethod
    def dispatch(cls, *_args, **_kwargs) -> Any:
        """Refuse — the sync path must never reach the publish rail."""
        raise AssertionError(
            "Bus.dispatch reached the publish path inside "
            "ExecutionContext.sync(); the job would be enqueued with no "
            "worker to drain it."
        )

    def handle(self) -> str:
        """Record the inline run and the context it observed."""
        type(self).ran += 1
        type(self).saw_sync = ExecutionContext.is_sync()
        return "inline-result"


class _FailingJob(_PublishRefusingJob):
    """Same probe, but the handler raises."""

    def handle(self) -> str:
        """Fail the way a real job body fails."""
        raise RuntimeError("job body exploded")


class _PendingStub:
    """Stand-in for the ``PendingDispatch`` builder the queue path uses."""

    def __init__(self) -> None:
        """Start with no terminal dispatch recorded."""
        self.dispatched = 0

    def dispatch(self) -> str:
        """The terminal call — returns a durable delivery id."""
        self.dispatched += 1
        return "delivery-uuid"


class _QueueingJob(BaseJob):
    """The contrast case: outside a sync context this MUST be published."""

    central_job = True
    default_queue = "jobs"
    ran = 0
    pending: _PendingStub | None = None

    @classmethod
    def dispatch(cls, **_params) -> _PendingStub:
        """Record that the publish path was entered."""
        cls.pending = _PendingStub()
        return cls.pending

    def handle(self) -> str:
        """Must NOT run — a queued job runs in the worker, not here."""
        type(self).ran += 1
        return "should-not-run"


@pytest.fixture(autouse=True)
def _no_container(monkeypatch) -> None:
    """Run with no bootstrapped application.

    ``Bus`` resolves the container through ``builtins.app`` to route the
    handler through DI when one exists. Removing it pins the plain
    ``job.handle()`` path and keeps the result independent of whichever
    other test in the suite last installed a container.
    """
    monkeypatch.delattr(builtins, "app", raising=False)


@pytest.fixture(autouse=True)
def _db(monkeypatch) -> _RecordingDB:
    """Serve the ``DB`` facade from a recording stub."""
    stub = _RecordingDB()
    monkeypatch.setattr(facades, "DB", stub, raising=False)
    return stub


@pytest.fixture(autouse=True)
def _reset_probes() -> None:
    """Clear the class-level counters between tests."""
    for cls in (_PublishRefusingJob, _FailingJob, _QueueingJob):
        cls.ran = 0
    _PublishRefusingJob.saw_sync = None
    _QueueingJob.pending = None


def test_a_sync_dispatch_runs_the_job_inline() -> None:
    """The job body executes before ``dispatch`` returns."""

    async def _go() -> Any:
        with Tenancy.central(), ExecutionContext.sync():
            return await Bus.dispatch(_PublishRefusingJob())

    result = asyncio.run(_go())

    assert _PublishRefusingJob.ran == 1
    assert result == "inline-result", (
        "the caller must receive the job's own return value in sync mode; "
        "a queued dispatch returns a delivery id instead"
    )


def test_the_inline_job_still_observes_sync_mode() -> None:
    """Recursive propagation: the job's OWN dispatches must stay inline.

    If the context were cleared before invoking the handler, a job that
    dispatches a follow-up would enqueue it — reintroducing the stranded
    tail this invariant exists to prevent, one level down.
    """

    async def _go() -> None:
        with Tenancy.central(), ExecutionContext.sync():
            await Bus.dispatch(_PublishRefusingJob())

    asyncio.run(_go())

    assert _PublishRefusingJob.saw_sync is True


def test_a_failing_inline_job_propagates_to_the_caller(_db) -> None:
    """A sync run must not swallow the failure into a silent retry.

    The whole point of ``--sync`` is that the caller sees what happened,
    so the exception surfaces and the framework-opened levels unwind by
    rollback rather than commit.
    """

    async def _go() -> None:
        with Tenancy.central(), ExecutionContext.sync():
            await Bus.dispatch(_FailingJob())

    with pytest.raises(RuntimeError, match="job body exploded"):
        asyncio.run(_go())

    assert ("rollback", 0) in _db.calls
    assert ("commit", 0) not in _db.calls


def test_the_framework_unwinds_only_to_the_callers_depth(_db) -> None:
    """The recorded baseline is the caller's depth, not zero-by-fiat.

    Committing *every* open level would seize the caller's ambient
    business transaction — committing its writes early on success and
    rolling them back underneath it on failure.
    """

    async def _go() -> None:
        with Tenancy.central(), ExecutionContext.sync():
            await Bus.dispatch(_PublishRefusingJob())

    asyncio.run(_go())

    assert _db.calls == [("commit", 0)]


def test_a_central_job_without_central_scope_is_refused() -> None:
    """Tenancy is fail-closed on the sync path too.

    Sync and async dispatch share the same check — part of what makes
    the two paths equivalent rather than merely similar.
    """

    async def _go() -> None:
        with ExecutionContext.sync():
            await Bus.dispatch(_PublishRefusingJob())

    with pytest.raises(RuntimeError, match="requires Tenancy.central"):
        asyncio.run(_go())

    assert _PublishRefusingJob.ran == 0


def test_outside_a_sync_context_the_job_is_published_not_run() -> None:
    """The contrast half: default mode still goes to the queue rail.

    Without this pin, "runs inline" could be satisfied by a Bus that
    runs everything inline everywhere — which would turn every worker
    into a no-op consumer.
    """

    async def _go() -> Any:
        with Tenancy.central():
            return await Bus.dispatch(_QueueingJob())

    result = asyncio.run(_go())

    assert _QueueingJob.ran == 0, "a queued job must run in the worker, not here"
    assert _QueueingJob.pending is not None
    assert _QueueingJob.pending.dispatched == 1, (
        "the terminal .dispatch() is mandatory — builder destruction never "
        "queues work"
    )
    assert result == "delivery-uuid"
