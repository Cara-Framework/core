"""Fire an in-process event only once its write is durable.

``Event.fire_sync`` inside a repository write runs the listeners while the
transaction is still open: a listener that queues work or pushes a realtime
frame acts on a row a rollback may still erase ("archived" announced for an
update that never landed). :func:`fire_after_commit` defers the dispatch to
the OUTERMOST commit through ``DB.after_commit`` (dropped on rollback, run at
once when no transaction is open), and logs-and-swallows a listener failure
there: the write already committed, and failing the caller now would report
a rolled-back write that is in fact in the database.
"""

from __future__ import annotations

from typing import Any

from cara.facades import DB, Log

from .Event import Event


def fire_after_commit(event: Any, *, connection: Any = None) -> None:
    """Dispatch ``event`` after the open transaction commits (or now)."""

    def fire() -> None:
        try:
            Event.fire_sync(event)
        except Exception as exc:
            Log.error(
                "event %s failed after commit: %s: %s",
                type(event).__name__,
                type(exc).__name__,
                exc,
                category="cara.events",
            )

    DB.after_commit(fire, connection)


__all__ = ["fire_after_commit"]
