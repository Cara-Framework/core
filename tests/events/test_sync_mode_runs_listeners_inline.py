"""Regression: under ExecutionContext.sync(), ShouldQueue listeners run INLINE.

Background
~~~~~~~~~~
Event listeners that implement ``ShouldQueue`` are normally pushed to a broker
(Laravel parity). But under ``--sync`` (CLI / tests) nothing drains that broker —
the command exits right after — so a queued listener strands the event-driven
pipeline mid-flight ("first product fully processed, the rest stuck in the
queue"). In sync mode the whole pipeline must run inline to completion.

The dispatcher now skips the queue for ShouldQueue listeners when
``ExecutionContext.is_sync()`` and runs them in-process instead. Async mode is
unchanged (still queued). These tests pin both halves.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from cara.events import Event as EventDispatcher
from cara.queues.contracts import ShouldQueue


class UserRegisteredEvent:
    """Test-local event fixture."""
    name = "user.registered"
    def __init__(self, user_id, email, **extra):
        self.user_id = user_id
        self.email = email
        self._stopped = False
    @property
    def is_propagation_stopped(self):
        return self._stopped

_SYNC = "cara.context.ExecutionContext.ExecutionContext.is_sync"


class _QueuedListener(ShouldQueue):
    def __init__(self, log: list[str]) -> None:
        self.log = log

    def handle(self, event) -> None:  # noqa: ANN001
        self.log.append("ran-inline")


@pytest.mark.asyncio
async def test_should_queue_listener_runs_inline_in_sync_mode():
    d = EventDispatcher()
    log: list[str] = []
    d.subscribe("user.registered", _QueuedListener(log))
    event = UserRegisteredEvent(user_id=1, email="a@b.com")

    with (
        patch(_SYNC, return_value=True),
        patch.object(EventDispatcher, "_queue_listener") as mock_queue,
    ):
        await d.dispatch(event)

    assert log == ["ran-inline"], "sync-mode ShouldQueue listener must run inline"
    mock_queue.assert_not_called()


@pytest.mark.asyncio
async def test_should_queue_listener_is_queued_in_async_mode():
    d = EventDispatcher()
    log: list[str] = []
    d.subscribe("user.registered", _QueuedListener(log))
    event = UserRegisteredEvent(user_id=1, email="a@b.com")

    with patch(_SYNC, return_value=False), patch.object(
        EventDispatcher, "_queue_listener", return_value=True
    ) as mock_queue:
        await d.dispatch(event)

    mock_queue.assert_called_once()
    assert log == [], "async-mode ShouldQueue listener must be queued, not inline"
