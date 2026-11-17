"""BoundedProbes: concurrent, budgeted, single-flight readiness probes."""

from __future__ import annotations

import asyncio
import threading
import time

from cara.support import BoundedProbes


def _ok() -> dict:
    return {"status": "ok", "latency_ms": 1}


def test_probes_run_concurrently_inside_one_budget():
    def slow() -> dict:
        time.sleep(0.05)
        return _ok()

    async def scenario() -> tuple[dict, float]:
        started = time.monotonic()
        result = await BoundedProbes.run(
            {"a": slow, "b": slow, "c": slow}, budget_ms=500, namespace="conc"
        )
        return result, time.monotonic() - started

    result, elapsed = asyncio.run(scenario())
    assert {k: v["status"] for k, v in result.items()} == {
        "a": "ok",
        "b": "ok",
        "c": "ok",
    }
    assert elapsed < 0.14  # concurrent: ~one probe, not three back to back


def test_a_stalled_probe_times_out_and_is_not_stacked():
    release = threading.Event()
    calls: list[int] = []

    def stalled() -> dict:
        calls.append(1)
        release.wait(timeout=2)
        return _ok()

    async def scenario() -> None:
        loop_ticked = asyncio.Event()

        async def tick() -> None:
            await asyncio.sleep(0.01)
            loop_ticked.set()

        marker = asyncio.create_task(tick())
        first = await BoundedProbes.run(
            {"db": stalled, "cache": _ok}, budget_ms=50, namespace="st"
        )
        assert first["db"]["status"] == "timeout"
        assert first["cache"]["status"] == "ok"
        assert loop_ticked.is_set()  # the event loop stayed responsive
        second = await BoundedProbes.run({"db": stalled}, budget_ms=50, namespace="st")
        assert second["db"]["status"] == "timeout"
        assert len(calls) == 1  # the second call did not start another probe
        assert BoundedProbes.in_flight("db", namespace="st")
        release.set()
        while BoundedProbes.in_flight("db", namespace="st"):
            await asyncio.sleep(0.005)
        third = await BoundedProbes.run({"db": stalled}, budget_ms=500, namespace="st")
        assert third["db"]["status"] == "ok"
        assert len(calls) == 2
        await marker

    asyncio.run(scenario())


def test_a_raising_probe_reports_down_with_its_type():
    def broken() -> dict:
        raise ConnectionRefusedError("no")

    result = asyncio.run(BoundedProbes.run({"q": broken}, budget_ms=200, namespace="err"))
    assert result == {"q": {"status": "down", "error": "ConnectionRefusedError"}}
