"""Telling and closing a socket the connection registry has let go of.

Forgetting a connection does not end it: the socket stays open, its controller
and authorization fence keep running, it gets no event and no heartbeat ever
again, and it no longer counts against any cap. So every drop also closes.

A close runs in a task of its own and the dropper waits for it only briefly,
because what closes a client that stopped reading is the ASGI server's own close
timeout (``websockets``: can't write the close frame in ``close_timeout``, fail
the connection). Cancelling the close at a bound of ours skips exactly that, and
leaves the socket and its controller open until the peer happens to go away.
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any

from cara.facades import Log

# How long a dropper waits for the close before leaving it to finish alone.
_SETTLE_SECONDS = 1.0
# The most a close may run: past ``Socket.send_timeout`` (a reply can hold the
# write lock that long) plus the ASGI server's own close timeouts, so this only
# ever ends a close the server has no bound for.
_CLOSE_CEILING_SECONDS = 90.0


class SocketRetirement:
    """The closes in flight for sockets the registry has already dropped."""

    # Close codes a dropped client sees. 4012 = the per-user cap took this
    # tab's place (its own code: a client that must not reconnect on it cannot
    # share one with a malformed frame's 4009); 1013 = the server gave up
    # delivering to it ("try again later").
    CLOSE_EVICTED = 4012
    CLOSE_UNRESPONSIVE = 1013

    def __init__(self, send_timeout: float) -> None:
        self._send_timeout = send_timeout
        self._closing: set[asyncio.Task] = set()

    def begin(
        self,
        socket: Any,
        code: int,
        reason: str,
        *,
        notice: dict[str, Any] | None = None,
    ) -> asyncio.Task:
        """Start telling and closing ``socket``. Never blocks the caller, so
        it is safe under the registry lock."""
        task = asyncio.create_task(self._retire(socket, code, reason, notice))
        self._closing.add(task)
        task.add_done_callback(self._closing.discard)
        return task

    @staticmethod
    async def settle(tasks: list[asyncio.Task]) -> None:
        """Give the closes a moment; whatever is left finishes on its own."""
        if tasks:
            await asyncio.wait(tasks, timeout=_SETTLE_SECONDS)

    async def _retire(
        self, socket: Any, code: int, reason: str, notice: dict[str, Any] | None
    ) -> None:
        if notice is not None:
            try:
                await asyncio.wait_for(
                    socket.send_json(notice), timeout=self._send_timeout
                )
            except Exception as exc:
                Log.debug(
                    "Notice to a dropped socket failed: %r",
                    exc,
                    category="cara.broadcasting",
                )
        close = getattr(socket, "close", None)
        if close is None:
            return
        try:
            await asyncio.wait_for(close(code, reason), timeout=_CLOSE_CEILING_SECONDS)
        except Exception as exc:
            Log.debug(
                "Closing a dropped socket failed: %r",
                exc,
                category="cara.broadcasting",
            )

    async def cancel_all(self) -> None:
        for task in list(self._closing):
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        self._closing.clear()
