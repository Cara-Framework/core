"""Sync broadcast helpers must not leak / silently swallow tasks.

When ``broadcast()`` / ``broadcast_event()`` are invoked from a sync
context that happens to be inside a running event loop, they schedule
the coroutine with ``loop.create_task``. asyncio only keeps a *weak*
reference to tasks, so a discarded broadcast task could be garbage-
collected mid-flight (the message vanishes) and any exception it raised
was never retrieved (silently swallowed).

The fix mirrors ``cara.events.Event._track``: hold a strong ref until the
task completes, and attach a done-callback that routes exceptions to
``Log.error`` instead of dropping them.
"""

from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest

from cara.broadcasting import helpers


@pytest.mark.asyncio
async def test_scheduled_task_is_strongly_referenced_until_done() -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    async def _coro() -> None:
        started.set()
        await release.wait()

    task = helpers._run_or_schedule(_coro())
    await started.wait()
    # While in-flight, a strong ref is held so the loop can't GC it.
    assert task in helpers._pending_broadcast_tasks

    release.set()
    await task
    # Done-callback removes it from the tracking set.
    assert task not in helpers._pending_broadcast_tasks


@pytest.mark.asyncio
async def test_task_exception_is_logged_not_swallowed() -> None:
    async def _boom() -> None:
        raise ValueError("kaboom")

    with patch("cara.facades.Log") as mock_log:
        task = helpers._run_or_schedule(_boom())
        with pytest.raises(ValueError):
            await task
        # Allow the done-callbacks to run on the loop.
        await asyncio.sleep(0)

    assert mock_log.error.called
    # The exception class + message are passed as printf args (now actually
    # interpolated by Logger), not swallowed.
    joined = " ".join(str(a) for a in mock_log.error.call_args.args)
    assert "ValueError" in joined or "kaboom" in joined
