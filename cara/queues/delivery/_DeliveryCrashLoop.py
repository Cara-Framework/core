"""The crash ceiling's one settlement: a row whose executions keep dying."""

from __future__ import annotations

from typing import Any

import pendulum

from cara.facades import Log


def _delivery_settle_crash_loop(
    self,
    database: Any,
    job_id: str,
    db_job_id: int,
    now: pendulum.DateTime,
    *,
    holds: str,
    holds_params: list[Any],
) -> bool:
    """Dead-letter one ``processing`` row whose executions hit the ceiling.

    The single definition of what the crash ceiling does to a row, shared by
    stale-lease recovery, a broker copy reclaiming a lapsed row, and a forced
    exit abandoning a run that outlived its timeout. Re-running the body
    would take another worker with it until the envelope expires, days away.

    ``holds`` is the caller's SQL proof that it may settle this row (with
    ``holds_params`` bound in order): a lapsed lease for recovery and
    reclaim, the caller's own lease token for an abandon. A row another
    holder has taken does not match, nothing is written, and the answer is
    ``False``; the caller decides what losing the row means for it.

    ``post_hooks_completed_at`` is left as it is (NULL on a processing row):
    ``failed()`` is still due, and the tracker is marked failed in the same
    transaction, so the hooks runner and the dashboard agree.
    """
    affected = database.statement(
        f"UPDATE {self.table} SET status = %s, completed_at = %s, "
        "terminal_reason = %s, lease_token = NULL, "
        "lease_expires_at = NULL, publish_lease_token = NULL, "
        "publish_lease_expires_at = NULL, updated_at = %s "
        f"WHERE job_id = %s AND status = %s AND {holds}",
        [
            self.STATUS_DEAD_LETTERED,
            now,
            self.CRASH_LOOP_REASON,
            now,
            job_id,
            self.STATUS_PROCESSING,
            *holds_params,
        ],
    )
    if not self._affected(affected):
        return False
    self._mark_tracker_failed(database, db_job_id, now)
    Log.error(
        "Queue delivery %s dead-lettered: %s",
        job_id,
        self.CRASH_LOOP_REASON,
        category="cara.queue.delivery",
    )
    return True


def _delivery_abandon_at_crash_ceiling(
    self, database: Any, job_id: str, lease_token: str
) -> bool:
    """Settle a forced-exit abandon whose spent attempt reached the ceiling.

    An abandon that keeps its attempt (a run past its own timeout) used to
    return the row to ``pending`` whatever ``attempts`` said, and the ceiling
    was only checked when a lease LAPSED, so a hang that kept meeting worker
    stops ran again and again with ``attempts`` growing past the ceiling.
    Runs inside the abandon's transaction; ``False`` leaves the ordinary
    release to it (below the ceiling, a finished tracker, a row not held).
    """
    row = database.select_one(
        f"SELECT db_job_id, attempts FROM {self.table} "
        "WHERE job_id = %s AND status = %s AND lease_token = %s FOR UPDATE",
        [job_id, self.STATUS_PROCESSING, lease_token],
    )
    if row is None:
        return False
    db_job_id = self._row_value(row, "db_job_id")
    if (
        db_job_id is None
        or int(self._row_value(row, "attempts") or 0) < self.CRASH_RECLAIM_CEILING
        # A finished tracker means the body ended before the exit; recovery
        # and reclaim never call that a crash loop, and neither does this.
        or not self._tracker_is_open(database, int(db_job_id))
    ):
        return False
    return self._settle_crash_loop(
        database,
        job_id,
        int(db_job_id),
        pendulum.now("UTC"),
        holds="lease_token = %s",
        holds_params=[lease_token],
    )
