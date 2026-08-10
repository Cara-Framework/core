"""Read/delete access to the job tracker and the delivery ledger, for operators.

Two tables, one place. The ``job`` tracker table records what the worker *did*;
the ``queue_job_delivery`` ledger records what was *published* and is the only
thing the relay ever reads. Operator commands need both, and conflating them is
the single most expensive mistake in this area — a "dead letter queue" count
taken from failed tracker rows describes jobs that may never have been
dead-lettered at all, and a retry that rewrites a tracker row republishes
nothing, because publication happens exclusively from the ledger outbox.

The ledger's own due/claim predicates are NOT here: those belong to
:class:`~cara.queues.delivery.QueueJobDeliveryStore.QueueJobDeliveryStore`,
which the relay and the hooks runner claim by. This store is the read-and-purge
surface an operator drives from a terminal.

Two ``ON DELETE RESTRICT`` foreign keys shape the delete methods:
``queue_job_delivery.db_job_id -> job.id`` and
``queue_job_delivery.replay_of -> queue_job_delivery.job_id``. A tracker row
still referenced by a ledger row cannot be deleted, and a ledger row that
already has a replay child cannot be deleted — including one such row in a
DELETE aborts the whole statement, deleting nothing. Both are excluded by
predicate rather than discovered by exception.
"""

from __future__ import annotations

from typing import Any

from cara.facades import DB
from cara.queues.delivery.QueueJobDeliveryStore import QueueJobDeliveryStore

__all__ = ["QueueOperationsStore"]


class QueueOperationsStore:
    """Operator-facing queries over the job tracker and the delivery ledger."""

    tracker_table = "job"
    ledger_table = "queue_job_delivery"

    # Tracker lifecycle vocabulary, as written by
    # ``cara.queues.tracking.JobTracker``.
    TRACKER_STATUS_PENDING = "pending"
    TRACKER_STATUS_PROCESSING = "processing"
    TRACKER_STATUS_SUCCESS = "success"
    TRACKER_STATUS_FAILED = "failed"
    TRACKER_STATUS_RETRYING = "retrying"

    TERMINAL_LEDGER_STATUSES = (
        QueueJobDeliveryStore.STATUS_DEAD_LETTERED,
        QueueJobDeliveryStore.STATUS_EXPIRED,
    )

    # ── tracker (job) ─────────────────────────────────────────────────
    def list_distinct_queues_since(self, *, cutoff: Any) -> list[str]:
        """Distinct queue names touched since ``cutoff`` (NULL reads as 'default')."""
        rows = (
            DB.select(
                f"SELECT DISTINCT COALESCE(queue, 'default') AS queue "
                f"FROM {self.tracker_table} WHERE created_at >= %s",
                [cutoff],
            )
            or []
        )
        return sorted(str(self._value(row, "queue")) for row in rows)

    def count_jobs(
        self,
        *,
        queue: str | None = None,
        status: str | None = None,
        created_at_gte: Any | None = None,
    ) -> int:
        """Count tracker jobs matching the given filters."""
        conditions = ["1 = 1"]
        bindings: list[Any] = []
        if queue is not None:
            conditions.append("queue = %s")
            bindings.append(queue)
        if status is not None:
            conditions.append("status = %s")
            bindings.append(status)
        if created_at_gte is not None:
            conditions.append("created_at >= %s")
            bindings.append(created_at_gte)
        row = DB.select_one(
            f"SELECT COUNT(*) AS total FROM {self.tracker_table} "
            f"WHERE {' AND '.join(conditions)}",
            bindings,
        )
        return int(self._value(row or {}, "total") or 0)

    def list_recent_failed_jobs(
        self, *, queue: str, cutoff: Any, limit: int
    ) -> list[dict]:
        """Recent failed tracker jobs for one queue, newest first."""
        return list(
            DB.select(
                f"SELECT id, public_id, name, queue, status, error, created_at "
                f"FROM {self.tracker_table} WHERE queue = %s AND status = %s "
                "AND created_at >= %s ORDER BY created_at DESC LIMIT %s",
                [queue, self.TRACKER_STATUS_FAILED, cutoff, int(limit)],
            )
            or []
        )

    def list_failed_jobs_older_than(
        self, *, cutoff: Any, queue: str | None = None
    ) -> list[dict]:
        """Failed tracker jobs older than ``cutoff`` that the ledger has released.

        A row the ledger still references cannot be deleted
        (``queue_job_delivery_db_job_id_foreign`` is ``ON DELETE RESTRICT``),
        so it is excluded here rather than surfacing as a failed delete.
        """
        conditions = [
            "status = %s",
            "created_at < %s",
            f"NOT EXISTS (SELECT 1 FROM {self.ledger_table} "
            f"WHERE {self.ledger_table}.db_job_id = {self.tracker_table}.id)",
        ]
        bindings: list[Any] = [self.TRACKER_STATUS_FAILED, cutoff]
        if queue is not None:
            conditions.append("queue = %s")
            bindings.append(queue)
        return list(
            DB.select(
                f"SELECT id, public_id, name, queue, status, error, created_at "
                f"FROM {self.tracker_table} WHERE {' AND '.join(conditions)} "
                "ORDER BY created_at ASC",
                bindings,
            )
            or []
        )

    def delete_job(self, job_id: Any) -> int:
        """Delete one tracker job row by primary key; returns the row count."""
        return int(
            DB.statement(
                f"DELETE FROM {self.tracker_table} WHERE id = %s",
                [int(job_id)],
            )
            or 0
        )

    # ── delivery ledger (queue_job_delivery) ──────────────────────────
    def list_dead_lettered_deliveries(
        self, *, queue: str | None = None, limit: int = 100
    ) -> list[dict]:
        """Terminal (dead-lettered/expired) ledger rows, oldest-stalled first."""
        conditions = "status IN (%s, %s)"
        bindings: list[Any] = list(self.TERMINAL_LEDGER_STATUSES)
        if queue is not None:
            conditions += " AND queue = %s"
            bindings.append(queue)
        bindings.append(int(limit))
        return list(
            DB.select(
                "SELECT job_id, db_job_id, queue, status, terminal_reason, "
                f"updated_at FROM {self.ledger_table} WHERE {conditions} "
                "ORDER BY updated_at ASC LIMIT %s",
                bindings,
            )
            or []
        )

    def find_dead_lettered_delivery(self, job_id: str) -> dict | None:
        """One terminal ledger row by ``job_id``; ``None`` when missing or live."""
        return DB.select_one(
            "SELECT job_id, db_job_id, queue, status, terminal_reason, "
            f"updated_at FROM {self.ledger_table} WHERE job_id = %s "
            "AND status IN (%s, %s)",
            [job_id, *self.TERMINAL_LEDGER_STATUSES],
        )

    def count_dead_lettered(self, *, queue: str | None = None) -> int:
        """Count terminal ledger rows, optionally scoped to one queue."""
        conditions = "status IN (%s, %s)"
        bindings: list[Any] = list(self.TERMINAL_LEDGER_STATUSES)
        if queue is not None:
            conditions += " AND queue = %s"
            bindings.append(queue)
        row = DB.select_one(
            f"SELECT COUNT(*) AS total FROM {self.ledger_table} WHERE {conditions}",
            bindings,
        )
        return int(self._value(row or {}, "total") or 0)

    def count_dead_lettered_older_than(
        self, *, cutoff: Any, queue: str | None = None
    ) -> int:
        """Count exactly the rows ``delete_terminal_deliveries_older_than`` removes."""
        conditions = self._purgeable_ledger_conditions()
        bindings: list[Any] = [*self.TERMINAL_LEDGER_STATUSES, cutoff]
        if queue is not None:
            conditions += " AND queue = %s"
            bindings.append(queue)
        row = DB.select_one(
            f"SELECT COUNT(*) AS total FROM {self.ledger_table} WHERE {conditions}",
            bindings,
        )
        return int(self._value(row or {}, "total") or 0)

    def delete_terminal_deliveries_older_than(
        self, *, cutoff: Any, queue: str | None = None
    ) -> int:
        """Delete terminal ledger rows stalled since before ``cutoff``.

        Rows that already have a replay child are excluded: ``replay_of`` is
        ``ON DELETE RESTRICT``, so including one would abort the statement and
        delete nothing at all.
        """
        conditions = self._purgeable_ledger_conditions()
        bindings: list[Any] = [*self.TERMINAL_LEDGER_STATUSES, cutoff]
        if queue is not None:
            conditions += " AND queue = %s"
            bindings.append(queue)
        return int(
            DB.statement(
                f"DELETE FROM {self.ledger_table} WHERE {conditions}",
                bindings,
            )
            or 0
        )

    def _purgeable_ledger_conditions(self) -> str:
        return (
            "status IN (%s, %s) AND updated_at < %s AND NOT EXISTS ("
            f"SELECT 1 FROM {self.ledger_table} AS replay_child "
            f"WHERE replay_child.replay_of = {self.ledger_table}.job_id)"
        )

    @staticmethod
    def _value(row: Any, key: str) -> Any:
        """Read one column from a mapping row or an attribute-style row."""
        if isinstance(row, dict):
            return row.get(key)
        return getattr(row, key, None)
