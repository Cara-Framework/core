"""ThreadLane — a bounded pool that refuses instead of queueing.

Pins the three contracts a caller relies on when it moves a call a
remote party can stall off the shared executor:
1. Context vars cross into the lane thread, as with ``run_in_thread``.
2. A full lane raises ``ThreadLaneBusy`` at once; it never queues.
3. A slot frees when the CALL finishes, not when the await is cancelled.
"""

from __future__ import annotations

import asyncio
import threading

import pytest

from cara.context import Tenancy, ThreadLane, ThreadLaneBusy


def test_context_vars_cross_into_the_lane_thread() -> None:
    lane = ThreadLane("test", size=1)

    async def _drive() -> int | None:
        with Tenancy.as_tenant(7):
            return await lane.run(Tenancy.id)

    assert asyncio.run(_drive()) == 7


def test_a_full_lane_refuses_at_once_instead_of_queueing() -> None:
    lane = ThreadLane("test", size=2)
    release = threading.Event()

    async def _drive() -> list[str]:
        held = [asyncio.ensure_future(lane.run(release.wait, 5)) for _ in range(2)]
        await asyncio.sleep(0.05)
        with pytest.raises(ThreadLaneBusy) as refused:
            await lane.run(lambda: "never runs")
        assert refused.value.lane == "test" and refused.value.size == 2
        release.set()
        await asyncio.gather(*held)
        return [await lane.run(lambda: "ran")]

    assert asyncio.run(_drive()) == ["ran"]


def test_a_cancelled_await_keeps_the_slot_until_the_call_returns() -> None:
    lane = ThreadLane("test", size=1)
    release = threading.Event()

    async def _drive() -> str:
        held = asyncio.ensure_future(lane.run(release.wait, 5))
        await asyncio.sleep(0.05)
        held.cancel()
        await asyncio.sleep(0.05)
        # The thread is still blocked, so the lane is still full.
        with pytest.raises(ThreadLaneBusy):
            await lane.run(lambda: "never runs")
        release.set()
        for _ in range(100):
            await asyncio.sleep(0.01)
            try:
                return await lane.run(lambda: "ran")
            except ThreadLaneBusy:
                continue
        raise AssertionError("the slot never came back")

    assert asyncio.run(_drive()) == "ran"


def test_a_lane_needs_at_least_one_thread() -> None:
    with pytest.raises(ValueError):
        ThreadLane("test", size=0)
