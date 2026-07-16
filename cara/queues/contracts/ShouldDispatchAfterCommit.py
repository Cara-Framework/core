"""Marker contract: queue the job only after the DB transaction commits."""

from __future__ import annotations


class ShouldDispatchAfterCommit:
    """Defer queueing until the enclosing database transaction commits.

    A job dispatched from inside ``with DB.transaction():`` is normally
    pushed to the broker IMMEDIATELY — a worker can pick it up before the
    transaction commits (the row it needs doesn't exist yet), and if the
    transaction rolls back the job still runs against undone work
    (a "ghost job"). Marking the job class with this contract (or terminating a
    PendingDispatch with ``.after_commit().send()``) routes the actual push
    through ``DB.after_commit``:

    * inside a transaction → pushed right after the OUTERMOST commit
      succeeds; discarded entirely on rollback,
    * no transaction open → pushed immediately (nothing to wait for).

    Laravel parity: ``ShouldDispatchAfterCommit`` /
    ``dispatch(...)->afterCommit()``.

    Caveats:

    * Sync execution mode (``ExecutionContext.sync()``) runs the job
      inline and ignores this marker — same as it ignores ``delay``.
    * A deferred dispatch returns no job id (the push hasn't happened
      yet when the dispatch call returns).
    * ``UniqueJob`` + rollback: the unique lock was acquired at dispatch
      time and is only released when the job runs — a rolled-back
      transaction leaves it held until the ``unique_for`` TTL expires.
    """

    __slots__ = ()
