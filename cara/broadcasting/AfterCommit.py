"""The realtime door: a frame leaves after the commit, and never breaks a write.

Two failure modes this removes from every call site:

* **Sent inside the transaction.** A browser that reacts to the frame
  re-reads rows that are not committed yet (it sees the old state), or the
  frame announces a write a rollback then erases. :func:`broadcast_after_commit`
  hands the send to ``DB.after_commit``: deferred to the OUTERMOST commit of
  the open transaction, discarded on rollback, sent at once when no
  transaction is open.
* **A transport error fails the caller.** The business write already
  committed; a Redis blip must not turn it into a failed job or a 500. Every
  transport error is logged and swallowed HERE, in one place — call sites
  write no ``try/except`` of their own.

The event object is built by the caller before the commit, so its payload is
fixed at that point; anything it should read after the commit belongs in its
``broadcast_with``.
"""

from __future__ import annotations

from typing import Any

from cara.facades import DB, Log

from .contracts import ShouldBroadcast
from .helpers import _run_or_schedule, broadcast_event_async


async def _send_guarded(event: ShouldBroadcast) -> None:
    try:
        await broadcast_event_async(event)
    except Exception as exc:
        Log.warning(
            "realtime frame %s not sent: %s: %s",
            type(event).__name__,
            type(exc).__name__,
            exc,
            category="cara.broadcasting",
        )


def broadcast_after_commit(event: ShouldBroadcast, *, connection: Any = None) -> None:
    """Send ``event`` after the open transaction commits (or now, if none).

    Never raises for a transport failure. A non-``ShouldBroadcast`` is refused
    up front (a programming error, not a transport one).
    """
    if not isinstance(event, ShouldBroadcast):
        raise TypeError(
            f"broadcast_after_commit needs a ShouldBroadcast, got {type(event).__name__}"
        )

    def send() -> None:
        try:
            _run_or_schedule(_send_guarded(event))
        except Exception as exc:  # scheduling itself failed (no loop, closed loop)
            Log.warning(
                "realtime frame %s not scheduled: %s",
                type(event).__name__,
                type(exc).__name__,
                category="cara.broadcasting",
            )

    DB.after_commit(send, connection)


__all__ = ["broadcast_after_commit"]
