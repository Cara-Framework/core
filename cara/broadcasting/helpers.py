"""
Broadcasting helpers — Laravel-style ``broadcast()`` shortcuts.

Cara is async-first; the canonical helpers are the ``*_async``
variants. The synchronous wrappers exist for the rare caller (a
sync queue handler, a script) that must dispatch a broadcast from a
purely synchronous context — they do the right thing in either
context but are NEVER the preferred API. Code running inside an
event loop should always use the ``_async`` versions.
"""

from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Dict, List, Optional, Sequence, Union

from cara.broadcasting.BroadcastEvent import BroadcastEvent
from cara.broadcasting.Channel import Channel
from cara.broadcasting.contracts import ShouldBroadcast
from cara.facades import Broadcast

ChannelLike = Union[str, Channel]


# ---------------------------------------------------------------------
# Async — preferred API.
# ---------------------------------------------------------------------
async def broadcast_async(
    channels: Union[ChannelLike, Sequence[ChannelLike]],
    event: str = "message",
    data: Optional[Dict[str, Any]] = None,
    *,
    except_socket_id: Optional[str] = None,
) -> None:
    """Fan out an ad-hoc event to one or more channels.

    Use ``broadcast_event_async`` instead for any event with a stable
    payload shape — the event class keeps the wire contract close to
    its data.
    """
    await Broadcast.broadcast(
        channels, event, data or {}, except_socket_id=except_socket_id
    )


async def broadcast_event_async(event: ShouldBroadcast) -> None:
    """Dispatch a ``ShouldBroadcast`` event."""
    await Broadcast.broadcast_event(event)


async def broadcast_to_user_async(
    user_id: str,
    event: str = "message",
    data: Optional[Dict[str, Any]] = None,
    *,
    except_socket_id: Optional[str] = None,
) -> None:
    """Push an event to every (cross-process) connection of a user."""
    await Broadcast.broadcast_to_user(
        user_id, event, data or {}, except_socket_id=except_socket_id
    )


# ---------------------------------------------------------------------
# Sync — only for the rare caller in a non-async context. Each helper
# routes to the async implementation; if a loop is already running,
# we schedule a task and return it (caller may `await` if they care
# about completion). If no loop is running, we run-to-completion in a
# fresh loop. We do NOT close the loop afterwards — closing tears
# down per-loop Redis pools that other callers in this thread may
# still be using.
# ---------------------------------------------------------------------
def _run_or_schedule(coro: Awaitable[None]) -> Optional[asyncio.Task]:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # No running loop in this thread → block-and-run.
        return asyncio.run(coro)  # type: ignore[func-returns-value]
    # Loop running → schedule. Caller may await the returned task.
    return loop.create_task(coro)


def broadcast(
    channels: Union[ChannelLike, Sequence[ChannelLike]],
    event: str = "message",
    data: Optional[Dict[str, Any]] = None,
    *,
    except_socket_id: Optional[str] = None,
) -> Optional[asyncio.Task]:
    """Sync entry-point — async callers should prefer ``broadcast_async``.

    Returns the scheduled ``asyncio.Task`` when invoked inside a
    running loop (caller may await), or ``None`` when invoked from a
    fully synchronous context.
    """
    return _run_or_schedule(
        broadcast_async(channels, event, data, except_socket_id=except_socket_id)
    )


def broadcast_event(event: ShouldBroadcast) -> Optional[asyncio.Task]:
    """Sync entry-point — async callers should prefer
    ``broadcast_event_async``."""
    return _run_or_schedule(broadcast_event_async(event))


__all__ = [
    "broadcast",
    "broadcast_async",
    "broadcast_event",
    "broadcast_event_async",
    "broadcast_to_user_async",
]
