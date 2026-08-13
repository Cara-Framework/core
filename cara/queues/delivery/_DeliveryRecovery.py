"""Expiry recovery, metrics reads and delivery statistics."""

from __future__ import annotations

from typing import Any

import pendulum

from cara.exceptions import QueueException
from cara.facades import Log

QueueJobDeliveryStore: type


def _bind_store(store_type: type) -> None:
    global QueueJobDeliveryStore
    QueueJobDeliveryStore = store_type


def _delivery_expire_job_if_due(
    self,
    job_id: str,
    now: pendulum.DateTime,
) -> bool:
    database = self._db()
    with database.transaction():
        row = database.select_one(
            f"UPDATE {self.table} SET status = %s, completed_at = %s, "
            "terminal_reason = %s, publish_status = CASE "
            "WHEN published_at IS NOT NULL THEN %s ELSE %s END, "
            "publish_lease_token = NULL, "
            "publish_lease_expires_at = NULL, updated_at = %s "
            "WHERE job_id = %s AND status = %s AND expires_at <= %s "
            "RETURNING db_job_id",
            [
                self.STATUS_EXPIRED,
                now,
                "envelope_expired_before_publish",
                self.PUBLISH_PUBLISHED,
                self.PUBLISH_FAILED,
                now,
                job_id,
                self.STATUS_PENDING,
                now,
            ],
        )
        expired = row is not None
        if expired:
            self._mark_tracker_failed(
                database,
                int(self._row_value(row, "db_job_id")),
                now,
            )
    if expired:
        Log.error(
            "Queue delivery %s expired before publication and remains "
            "available for ledger replay.",
            job_id,
            category="cara.queue.delivery",
        )
    return expired


def _delivery_expire_publish(self, job_id: str, token: str, reason: str) -> None:
    now = pendulum.now("UTC")
    database = self._db()
    with database.transaction():
        row = database.select_one(
            f"UPDATE {self.table} SET status = %s, completed_at = %s, "
            "terminal_reason = %s, publish_status = %s, "
            "publish_lease_token = NULL, "
            "publish_lease_expires_at = NULL, updated_at = %s "
            "WHERE job_id = %s AND publish_status = %s "
            "AND publish_lease_token = %s RETURNING db_job_id",
            [
                self.STATUS_EXPIRED,
                now,
                self._safe_error(reason),
                self.PUBLISH_FAILED,
                now,
                job_id,
                self.PUBLISH_PROCESSING,
                token,
            ],
        )
        if row is None:
            raise QueueException(
                f"Queue delivery {job_id} lost its expiry settlement lease."
            )
        self._mark_tracker_failed(
            database,
            int(self._row_value(row, "db_job_id")),
            now,
        )
    Log.error(
        "Queue delivery %s expired during publication and remains "
        "available for ledger replay.",
        job_id,
        category="cara.queue.delivery",
    )


def _delivery_backlog_metrics_if_installed(self) -> dict[str, int | float] | None:
    """``backlog_metrics`` for callers that may run without the ledger.

    Returns ``None`` when this database carries no delivery ledger at
    all. Cara is shared by products that do not deploy the outbox, so
    a diagnostic caller (see ``PublicationBacklogProbe``) must be able
    to tell "no ledger here" apart from "ledger is fine" — and must
    not explode on the former. ``to_regclass`` answers NULL for a
    missing relation instead of raising, which additionally leaves any
    surrounding PostgreSQL transaction usable; a bare ``SELECT`` on a
    missing table would abort it.

    The relay's own :meth:`backlog_metrics` deliberately does NOT get
    this treatment: if the ledger vanishes underneath the publisher,
    that must fail loud.
    """
    row = self._db().select_one(
        "SELECT to_regclass(%s) IS NOT NULL AS present",
        [self.table],
    )
    if not self._row_value(row, "present"):
        return None
    return self.backlog_metrics()


def _delivery_backlog_metrics(self) -> dict[str, int | float]:
    due = self._PUBLISH_BACKLOG_FILTER_TEMPLATE.format(now="NOW()")
    row = self._db().select_one(
        f"SELECT COUNT(*) AS count, COALESCE(EXTRACT(EPOCH FROM "
        f"(NOW() - MIN(available_at))), 0) AS age FROM {self.table} "
        f"WHERE {due}",
        [self.STATUS_PENDING, self.PUBLISH_PUBLISHED],
    )
    return {
        "count": int(self._row_value(row, "count") or 0),
        "age": max(float(self._row_value(row, "age") or 0), 0.0),
    }


def _delivery_outbox_health_metrics_if_installed(self) -> dict[str, float] | None:
    """``outbox_health_metrics`` for callers that may run without the ledger.

    Same contract as :meth:`backlog_metrics_if_installed`: ``None``
    means this database carries no delivery ledger at all, which a
    watchdog must distinguish from "ledger is fine".
    """
    row = self._db().select_one(
        "SELECT to_regclass(%s) IS NOT NULL AS present",
        [self.table],
    )
    if not self._row_value(row, "present"):
        return None
    return self.outbox_health_metrics()


def _delivery_outbox_health_metrics(self) -> dict[str, float]:
    """One bounded aggregate over BOTH halves of the durable outbox.

    Read by the scheduler-side watchdog (``QueueOutboxHealth``) from a
    process with no dependency on the relay or the hooks runner —
    publication gauges emitted by the relay cannot describe the relay
    being dead. Fleet-wide and label-safe: no tenant, queue or job
    identifiers are read.

    Keys: ``due_pending`` / ``oldest_due_age`` (publication half),
    ``last_publish_age`` (diagnostic; ``-1.0`` means nothing has ever
    been published), ``hook_due_pending`` / ``hook_oldest_due_age``
    (terminal-hook half).

    Both due predicates now have exactly ONE home in this class:
    ``_PUBLISH_BACKLOG_FILTER_TEMPLATE`` and
    ``_HOOK_DUE_FILTER_TEMPLATE``. The publication half deliberately
    measures BACKLOG, which is a SUPERSET of what
    :meth:`_claim_next_publish` can lease at any instant — rows in
    publish backoff, under a live publish lease, past expiry, or held
    back by the per-queue broker window are counted on purpose, because
    an alarm that drops them cannot report how much work is waiting.
    Do not "align" the two: they answer different questions.
    """
    due = self._PUBLISH_BACKLOG_FILTER_TEMPLATE.format(now="NOW()")
    hook_due = self._HOOK_DUE_FILTER_TEMPLATE.format(now="NOW()")
    hook_statuses = list(self.HOOK_TERMINAL_STATUSES)
    row = self._db().select_one(
        f"SELECT COUNT(*) FILTER (WHERE {due}) AS due_pending, "
        "COALESCE(EXTRACT(EPOCH FROM (NOW() - "
        f"(MIN(available_at) FILTER (WHERE {due})))), 0) "
        "AS oldest_due_age, "
        "COALESCE(EXTRACT(EPOCH FROM (NOW() - MAX(published_at))), -1) "
        "AS last_publish_age, "
        f"COUNT(*) FILTER (WHERE {hook_due}) AS hook_due_pending, "
        "COALESCE(EXTRACT(EPOCH FROM (NOW() - "
        "MIN(COALESCE(post_hooks_lease_expires_at, completed_at, created_at)) "
        f"FILTER (WHERE {hook_due}))), 0) "
        "AS hook_oldest_due_age "
        f"FROM {self.table}",
        [
            self.STATUS_PENDING,
            self.PUBLISH_PUBLISHED,
            self.STATUS_PENDING,
            self.PUBLISH_PUBLISHED,
            hook_statuses,
            hook_statuses,
        ],
    )
    last_publish = self._row_value(row, "last_publish_age")
    return {
        "due_pending": max(float(self._row_value(row, "due_pending") or 0), 0.0),
        "oldest_due_age": max(float(self._row_value(row, "oldest_due_age") or 0), 0.0),
        # -1 means "nothing has ever been published"; a sentinel for
        # human-readable alert bodies only, never emitted as a gauge.
        "last_publish_age": float(last_publish if last_publish is not None else -1),
        "hook_due_pending": max(
            float(self._row_value(row, "hook_due_pending") or 0), 0.0
        ),
        "hook_oldest_due_age": max(
            float(self._row_value(row, "hook_oldest_due_age") or 0), 0.0
        ),
    }


def _delivery_delivery_stats(
    self,
    queue_name: str,
    *,
    recent_hours: int,
) -> dict[str, Any]:
    """Return bounded operator stats from the canonical delivery ledger."""
    if queue_name not in self.canonical_queues:
        valid = ", ".join(self.canonical_queues)
        raise QueueException(f"Unknown canonical queue {queue_name!r}. Valid: {valid}.")
    hours = self._bounded_int(
        recent_hours,
        minimum=1,
        maximum=8760,
        field="recent_hours",
    )
    database = self._db()
    due = self._PUBLISH_BACKLOG_FILTER_TEMPLATE.format(now="NOW()")
    active = database.select_one(
        f"SELECT COUNT(*) AS active_total, "
        "COUNT(*) FILTER (WHERE status = %s) AS pending, "
        "COUNT(*) FILTER (WHERE status = %s) AS processing, "
        f"COUNT(*) FILTER (WHERE {due}) AS due_unpublished, "
        "COALESCE(EXTRACT(EPOCH FROM (NOW() - "
        f"(MIN(available_at) FILTER (WHERE {due})))), 0) "
        "AS oldest_due_age, "
        "COUNT(*) FILTER (WHERE publish_status = %s) "
        "AS publish_processing, "
        "COUNT(*) FILTER (WHERE publish_status = %s "
        "AND publish_lease_expires_at <= NOW()) AS stale_publish, "
        "COUNT(*) FILTER (WHERE status = %s "
        f"AND lease_expires_at <= NOW()) AS stale_execution "
        f"FROM {self.table} WHERE queue = %s "
        "AND status IN ('pending', 'processing')",
        [
            self.STATUS_PENDING,
            self.STATUS_PROCESSING,
            self.STATUS_PENDING,
            self.PUBLISH_PUBLISHED,
            self.STATUS_PENDING,
            self.PUBLISH_PUBLISHED,
            self.PUBLISH_PROCESSING,
            self.PUBLISH_PROCESSING,
            self.STATUS_PROCESSING,
            queue_name,
        ],
    )
    terminal = database.select_one(
        f"SELECT COUNT(*) AS terminal_recent_total, "
        "COUNT(*) FILTER (WHERE status = %s) AS completed, "
        "COUNT(*) FILTER (WHERE status = %s) AS retry_scheduled, "
        "COUNT(*) FILTER (WHERE status = %s) AS dead_lettered, "
        "COUNT(*) FILTER (WHERE status = %s) AS expired "
        f"FROM {self.table} WHERE queue = %s "
        "AND completed_at >= NOW() - make_interval(hours => %s)",
        [
            self.STATUS_COMPLETED,
            self.STATUS_RETRY_SCHEDULED,
            self.STATUS_DEAD_LETTERED,
            self.STATUS_EXPIRED,
            queue_name,
            hours,
        ],
    )
    hooks = database.select_one(
        f"SELECT COUNT(*) FILTER (WHERE "
        "post_hooks_quarantined_at IS NULL AND "
        "post_hooks_lease_token IS NULL) AS hook_pending, "
        "COUNT(*) FILTER (WHERE "
        "post_hooks_quarantined_at IS NULL AND "
        "post_hooks_lease_token IS NOT NULL AND "
        "post_hooks_lease_expires_at > NOW()) AS hook_processing, "
        "COUNT(*) FILTER (WHERE "
        "post_hooks_quarantined_at IS NULL AND "
        "post_hooks_lease_token IS NOT NULL AND "
        "post_hooks_lease_expires_at <= NOW()) AS hook_stale, "
        "COUNT(*) FILTER (WHERE "
        "post_hooks_quarantined_at IS NULL AND "
        "post_hooks_last_error IS NOT NULL) AS hook_failed, "
        "COUNT(*) FILTER (WHERE post_hooks_quarantined_at IS NOT NULL) "
        f"AS hook_quarantined FROM {self.table} "
        "WHERE queue = %s AND status = ANY(%s) "
        "AND post_hooks_completed_at IS NULL",
        [queue_name, list(self.HOOK_TERMINAL_STATUSES)],
    )

    def value(row: Any, key: str) -> int:
        return int(self._row_value(row, key) or 0)

    return {
        "queue": queue_name,
        "recent_hours": hours,
        "active_total": value(active, "active_total"),
        "terminal_recent_total": value(
            terminal,
            "terminal_recent_total",
        ),
        "statuses": {
            self.STATUS_PENDING: value(active, self.STATUS_PENDING),
            self.STATUS_PROCESSING: value(
                active,
                self.STATUS_PROCESSING,
            ),
            self.STATUS_COMPLETED: value(
                terminal,
                self.STATUS_COMPLETED,
            ),
            self.STATUS_RETRY_SCHEDULED: value(
                terminal,
                self.STATUS_RETRY_SCHEDULED,
            ),
            self.STATUS_DEAD_LETTERED: value(
                terminal,
                self.STATUS_DEAD_LETTERED,
            ),
            self.STATUS_EXPIRED: value(
                terminal,
                self.STATUS_EXPIRED,
            ),
        },
        "due_unpublished": value(active, "due_unpublished"),
        "oldest_due_age": max(
            float(self._row_value(active, "oldest_due_age") or 0),
            0.0,
        ),
        "publish_processing": value(active, "publish_processing"),
        "stale_leases": {
            "publish": value(active, "stale_publish"),
            "execution": value(active, "stale_execution"),
        },
        "hooks": {
            "pending": value(hooks, "hook_pending"),
            "processing": value(hooks, "hook_processing"),
            "stale": value(hooks, "hook_stale"),
            "failed": value(hooks, "hook_failed"),
            "quarantined": value(hooks, "hook_quarantined"),
        },
    }
