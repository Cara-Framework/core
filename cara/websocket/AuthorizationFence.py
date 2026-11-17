"""The fence that keeps an open socket inside the authority that admitted it.

A WebSocket is authorized ONCE, at the handshake, and then lives for hours.
Everything that admitted it can end while it is open: the device session is
revoked, the member leaves the group the channel belongs to, a role is taken
away. Without a re-check the socket keeps delivering frames to someone who no
longer has the right to read them — the HTTP twin answers 401/403 while the
stream beside it carries on.

So a socket that serves a private stream runs this fence beside its frame
loop (``Socket.serve_frames(fence_task, ...)``): every ``interval_seconds``
it re-asks each question in order, and the first answer that is not a yes
closes the socket with that question's close code. ``serve_frames`` returns
as soon as the fence task finishes, so the controller's ``finally`` runs.

FAIL CLOSED. An error while answering is not "the last answer still holds";
it is no answer. The failure is logged by its exception type only (a message
can carry a token or an address) and the socket is closed as if the answer
had been no. A stream that outlives a broken authority check is the leak this
exists to stop.

The questions are the caller's — a session-liveness read, a channel
re-authorization, anything awaitable that returns truthy for "still allowed".
Nothing here knows what a user, a session or a channel is.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any

from cara.facades import Log
from cara.websocket.FenceCheck import FenceCheck

__all__ = ["AuthorizationFence"]


class AuthorizationFence:
    """Re-check an established socket's authority until it fails or closes."""

    def __init__(
        self,
        socket: Any,
        checks: Sequence[FenceCheck],
        *,
        interval_seconds: float = 30.0,
        log_category: str = "websocket",
    ) -> None:
        if not checks:
            raise ValueError("AuthorizationFence needs at least one check")
        if interval_seconds < 0:
            raise ValueError("AuthorizationFence interval must not be negative")
        self._socket = socket
        self._checks = tuple(checks)
        self._interval = float(interval_seconds)
        self._category = log_category

    def start(self) -> asyncio.Task:
        """Run the fence as a task — the ``fence`` ``serve_frames`` waits on."""
        return asyncio.create_task(self.hold())

    async def hold(self) -> None:
        """Re-ask every check each interval; close on the first "no"."""
        while self._socket.is_connected:
            await asyncio.sleep(self._interval)
            if not self._socket.is_connected:
                return
            for check in self._checks:
                if not await self._still_true(check):
                    await self._socket.close(check.close_code, check.reason)
                    return

    async def _still_true(self, check: FenceCheck) -> bool:
        try:
            return bool(await check.ask())
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            Log.warning(
                "WS %s revalidation failed closed: %s",
                check.name,
                type(exc).__name__,
                category=self._category,
            )
            return False
