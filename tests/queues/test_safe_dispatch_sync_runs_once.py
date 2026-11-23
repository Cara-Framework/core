"""``safe_dispatch`` never re-runs a job body in sync mode.

Its retry loop exists for broker blips on a PUBLISH. Under
``ExecutionContext.sync()`` ``Bus.dispatch`` is the execution itself, so the
loop re-ran a failing body three times (27 times through three nested
levels) with the back-off sleeps in between.
"""

from __future__ import annotations

import asyncio
import importlib
from types import SimpleNamespace

import pytest

from cara.context import ExecutionContext

helpers = importlib.import_module("cara.queues.helpers")


def test_a_failing_sync_job_runs_once_and_its_error_propagates(monkeypatch) -> None:
    calls: list[dict] = []
    slept: list[float] = []

    async def dispatch(_job, **kwargs):
        calls.append(kwargs)
        raise ValueError("bad row")

    async def sleep(seconds):
        slept.append(seconds)

    monkeypatch.setattr(helpers, "Bus", SimpleNamespace(dispatch=dispatch))
    monkeypatch.setattr(helpers, "asyncio", SimpleNamespace(sleep=sleep))

    async def scenario() -> None:
        with ExecutionContext.sync():
            await helpers.safe_dispatch(object(), routing_key="orders.high")

    with pytest.raises(ValueError, match="bad row"):
        asyncio.run(scenario())

    assert calls == [{"routing_key": "orders.high"}]
    assert slept == []
