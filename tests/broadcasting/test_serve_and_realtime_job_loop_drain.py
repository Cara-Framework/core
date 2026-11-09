"""A job's event loop must not end with its broadcasts still pending.

A job's sync code only SCHEDULES its broadcast (``_run_or_schedule``), and
``asyncio.run`` cancels what is left the moment the job returns. Live on
2026-11-09, a Redis subscriber on a tenant channel for 35 minutes: every
door-side ``queued``, and 0 ``sent``, 0 ``confirmed``, 0 ``order.*`` from about
60 tasks and 5 orders run by workers. Nothing logged it.

These drive real ``asyncio.run`` loops through the entry points that own them,
against a fake broadcast backend.
"""

from __future__ import annotations

import asyncio
import threading
import time
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from cara.broadcasting import helpers
from cara.broadcasting.drivers.RedisBroadcaster import RedisBroadcaster
from cara.commands.core.JobProcessor import JobProcessor
from cara.context import Tenancy
from cara.events.Event import Event
from cara.queues.delivery import _DeliverySettlement
from cara.queues.delivery.TerminalHookClaim import TerminalHookClaim
from cara.queues.middleware import run_through_middleware, run_through_middleware_async


class _Backend:
    """Stands in for ``facades.Broadcast``; a publish takes a real loop turn."""

    def __init__(self, delay: float = 0.0) -> None:
        self.frames: list[str] = []
        self.delay = delay

    async def broadcast(self, _channels: Any, event: str, _data: Any, **_kw: Any) -> None:
        await asyncio.sleep(self.delay)
        self.frames.append(event)


@pytest.fixture
def backend():
    fake = _Backend()
    with patch("cara.facades.Broadcast", fake):
        yield fake


@pytest.fixture
def log():
    with patch("cara.facades.Log") as mocked:
        yield mocked


def _central_job() -> Any:
    return SimpleNamespace(central_job=True, _tenant_mode="central", _tenant_id=None)


async def _job_that_returns_at_once() -> str:
    helpers.broadcast("private-orders", "order.updated", {"id": "ord_1"})
    return "done"


def test_the_control_a_bare_loop_really_does_lose_the_frame(backend, log):
    # Without this the drain tests could pass on a backend that never loses one.
    asyncio.run(_job_that_returns_at_once())
    assert backend.frames == []
    assert log.warning.called, "a frame lost to a loop that ended must be reported"


def test_the_worker_entry_point_publishes_before_its_loop_ends(backend):
    result = JobProcessor._execute_async_job_with_timeout(
        _job_that_returns_at_once, (), 10
    )
    assert result == "done"
    assert backend.frames == ["order.updated"]


def test_a_failing_job_still_publishes_and_still_fails(backend):
    async def _job() -> None:
        helpers.broadcast("private-orders", "order.updated", {})
        raise ValueError("the job's own failure")

    with pytest.raises(ValueError, match="the job's own failure"):
        JobProcessor._execute_async_job_with_timeout(_job, (), 10)
    assert backend.frames == ["order.updated"]


def test_the_sync_middleware_runner_publishes_before_its_loop_ends(backend):
    def _handler(_job: Any) -> str:
        helpers.broadcast("private-orders", "order.updated", {})
        return "done"

    with Tenancy.central():
        assert run_through_middleware(_central_job(), _handler) == "done"
    assert backend.frames == ["order.updated"]


def test_a_terminal_hook_publishes_before_its_loop_ends(backend):
    class _Job:
        async def failed(self, _payload, _error, idempotency_key=None) -> None:
            helpers.broadcast("private-orders", "sync.task.updated", {})

    store = MagicMock()
    store.hook_timeout_seconds = 5
    store.claim_terminal_hooks.return_value = TerminalHookClaim(
        outcome="claimed", lease_token="lease", signed_envelope=b"x", status="failed"
    )
    with patch.object(_DeliverySettlement, "_has_terminal_callback", lambda *_a: True):
        assert _DeliverySettlement._delivery_process_terminal_hooks(
            store, "job-1", instance=_Job(), message={"_tenant_mode": "central"}
        )
    assert backend.frames == ["sync.task.updated"]
    store.complete_terminal_hooks.assert_called_once_with("job-1", "lease")


def test_a_detached_listener_finishes_and_its_own_broadcast_goes_out(backend):
    # The frame is still on the wire when the listener ends, so the drain has
    # to look again rather than stop at what was pending when it started.
    backend.delay = 0.05

    async def _listener() -> None:
        await asyncio.sleep(0.05)
        helpers.broadcast("private-orders", "after.listener", {})

    async def _job() -> None:
        Event._track(asyncio.create_task(_listener()))

    asyncio.run(helpers.drained(_job()))
    assert backend.frames == ["after.listener"]


def test_a_slow_redis_costs_the_job_the_bound_and_no_more(log):
    # The publish outlasts the bound by far: the job's result still comes back
    # on time, and what was given up is counted once, not once per frame.
    slow = _Backend(delay=60)

    async def _job() -> str:
        for _ in range(3):
            helpers.broadcast("private-orders", "order.updated", {})
        return "done"

    async def _bounded() -> str:
        try:
            return await _job()
        finally:
            await helpers.drain_detached_tasks(timeout=0.2)

    started = time.monotonic()
    with patch("cara.facades.Broadcast", slow):
        assert asyncio.run(_bounded()) == "done"
    assert time.monotonic() - started < 2.0
    assert slow.frames == []
    log.warning.assert_called_once()
    assert log.warning.call_args.args[1] == 3


def test_a_publish_stuck_in_redis_does_not_hold_the_loops_shutdown(log):
    # ``asyncio.run`` joins the default executor before it returns, so a
    # publish on it outlives every bound here. Real driver, a client that
    # blocks like a black-holed socket (measured: 5.09 s for a 2 s drain).
    release = threading.Event()

    class _StuckClient:
        def publish(self, _channel: str, _payload: str) -> int:
            release.wait(10)
            return 0

    node = RedisBroadcaster(
        {"websocket": {"heartbeat_interval": 0, "max_connections": 10}},
        redis_url="redis://localhost:6379/0",
    )
    node._publisher._client = _StuckClient()

    async def _job() -> str:
        task = asyncio.create_task(node.broadcast("orders", "order.updated", {}))
        helpers._pending_broadcast_tasks.add(task)
        task.add_done_callback(helpers._pending_broadcast_tasks.discard)
        await asyncio.sleep(0.02)
        await helpers.drain_detached_tasks(timeout=0.1)
        return "done"

    started = time.monotonic()
    try:
        assert asyncio.run(_job()) == "done"
        elapsed = time.monotonic() - started
    finally:
        release.set()
    assert elapsed < 3.0


def test_a_nested_pipeline_never_waits_on_the_loop_that_goes_on(log):
    # A job dispatched inline, inside a request or another job, runs the same
    # pipeline: it must not stop the surrounding loop on a slow Redis.
    slow = _Backend(delay=60)

    async def _handler(_job: Any) -> str:
        helpers.broadcast("private-orders", "order.updated", {})
        return "done"

    async def _scenario() -> float:
        started = time.monotonic()
        with Tenancy.central():
            await run_through_middleware_async(_central_job(), _handler)
        elapsed = time.monotonic() - started
        for task in list(helpers._pending_broadcast_tasks):
            task.remove_done_callback(helpers._handle_broadcast_task_exception)
            task.cancel()
        return elapsed

    with patch("cara.facades.Broadcast", slow):
        assert asyncio.run(_scenario()) < 0.5


def test_one_jobs_drain_does_not_wait_on_another_threads_loop():
    # Scheduler ticks run concurrently, one loop a thread, on one global set.
    release = threading.Event()
    other_started = threading.Event()
    other_loop_done = threading.Event()

    async def _forever() -> None:
        while not release.is_set():
            await asyncio.sleep(0.01)

    async def _other_jobs_loop() -> None:
        task = asyncio.get_running_loop().create_task(_forever())
        helpers._pending_broadcast_tasks.add(task)
        task.add_done_callback(helpers._pending_broadcast_tasks.discard)
        other_started.set()
        await task
        other_loop_done.set()

    other = threading.Thread(target=lambda: asyncio.run(_other_jobs_loop()), daemon=True)
    other.start()
    assert other_started.wait(5)

    async def _mine() -> str:
        return "done"

    started = time.monotonic()
    try:
        assert asyncio.run(helpers.drained(_mine())) == "done"
        elapsed = time.monotonic() - started
    finally:
        release.set()
        other.join(5)
    assert elapsed < helpers.DETACHED_DRAIN_SECONDS / 2
    assert other_loop_done.is_set(), "the drain must not touch another loop's task"


def test_the_socket_loop_handoff_still_delivers_from_a_job_loop():
    # The API process: frames go to the loop its sockets live on, which goes on
    # after the job's loop has ended, so the drain has nothing to hold.
    backend = _Backend()
    home = asyncio.new_event_loop()
    thread = threading.Thread(target=home.run_forever, daemon=True)
    thread.start()
    helpers.bind_socket_loop(home)
    try:
        with patch("cara.facades.Broadcast", backend):
            asyncio.run(helpers.drained(_job_that_returns_at_once()))
            deadline = time.monotonic() + 5
            while not backend.frames and time.monotonic() < deadline:
                time.sleep(0.01)
    finally:
        helpers._socket_loop = None
        home.call_soon_threadsafe(home.stop)
        thread.join(5)
        home.close()
    assert backend.frames == ["order.updated"]
