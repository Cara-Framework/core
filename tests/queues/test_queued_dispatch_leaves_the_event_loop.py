"""A queued ``Bus.dispatch`` never blocks the event loop on its outbox write.

The durable dispatch is synchronous database work: the tracking row and the
delivery row are inserted and committed before ``dispatch`` returns. Run on
the loop, every request that dispatches holds the whole process for that
commit. The api serves from ONE event loop (``craft serve``), so its webhook
ingress was as fast as one commit at a time: a WAL-flush stall of 50-100 ms
froze every request for seconds, 4,000 webhooks/min already showed p95 of
1.5-2.8 s, and past ~9,000/min connections were refused (ingest stress
2026-11-12, ``stress/ingest/results.md``).

Outside a transaction the write has nothing to share with its caller, so it
runs in a worker thread with the caller's context (tenancy, correlation id).
Inside one it MUST stay on the caller's pinned connection: the delivery row
commits or rolls back with the caller's own writes (the transactional
outbox), so that case keeps running inline.
"""

from __future__ import annotations

import asyncio
import builtins
import threading
import time
from typing import Any

import pytest

import cara.facades as facades
from cara.context import Tenancy
from cara.queues import Bus
from cara.queues.contracts import BaseJob


class _DB:
    """The ``DB`` facade surface ``Bus.dispatch`` reads: the caller's depth."""

    def __init__(self) -> None:
        self.level = 0

    def transaction_level(self) -> int:
        return self.level


class _Pending:
    """The ``PendingDispatch`` builder: the terminal call is the outbox write."""

    seconds = 0.0
    seen: list[dict[str, Any]] = []

    def dispatch(self) -> str:
        type(self).seen.append(
            {"thread": threading.get_ident(), "central": Tenancy.is_central()}
        )
        if self.seconds:
            time.sleep(self.seconds)
        return "delivery-uuid"


class _Job(BaseJob):
    central_job = True
    default_queue = "jobs"

    @classmethod
    def dispatch(cls, **_params) -> _Pending:
        return _Pending()

    def handle(self) -> None:  # pragma: no cover - never runs here
        raise AssertionError("a queued job must not run in the caller")


@pytest.fixture(autouse=True)
def _no_container(monkeypatch) -> None:
    monkeypatch.delattr(builtins, "app", raising=False)


@pytest.fixture(autouse=True)
def _db(monkeypatch) -> _DB:
    stub = _DB()
    monkeypatch.setattr(facades, "DB", stub, raising=False)
    _Pending.seen = []
    _Pending.seconds = 0.0
    return stub


def test_outside_a_transaction_the_outbox_write_runs_off_the_loop() -> None:
    async def _go() -> tuple[int, str]:
        loop_thread = threading.get_ident()
        with Tenancy.central():
            delivery = await Bus.dispatch(_Job())
        return loop_thread, delivery

    loop_thread, delivery = asyncio.run(_go())

    assert delivery == "delivery-uuid"
    assert _Pending.seen[0]["thread"] != loop_thread, (
        "the outbox insert + commit ran on the event loop thread: every other "
        "request on this process waited for that commit"
    )
    assert _Pending.seen[0]["central"] is True, (
        "the write must see the caller's tenancy scope in its thread"
    )


def test_inside_a_transaction_the_outbox_write_stays_on_the_callers_connection(
    _db,
) -> None:
    _db.level = 1

    async def _go() -> int:
        with Tenancy.central():
            await Bus.dispatch(_Job())
        return threading.get_ident()

    loop_thread = asyncio.run(_go())

    assert _Pending.seen[0]["thread"] == loop_thread, (
        "a dispatch inside an open transaction must join it (transactional "
        "outbox), never commit on another connection"
    )


def test_concurrent_dispatches_overlap_their_commits() -> None:
    _Pending.seconds = 0.2

    async def _go() -> float:
        started = time.perf_counter()
        with Tenancy.central():
            await asyncio.gather(*(Bus.dispatch(_Job()) for _ in range(5)))
        return time.perf_counter() - started

    elapsed = asyncio.run(_go())

    assert len(_Pending.seen) == 5
    assert elapsed < 0.6, (
        f"5 dispatches of a 200 ms commit took {elapsed:.2f}s: they ran one "
        "after another on the loop"
    )
