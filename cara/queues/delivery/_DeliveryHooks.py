"""Terminal-hook claims, execution leases and timeout policy."""

from __future__ import annotations

import inspect
import time
from typing import Any

import pendulum

from cara.exceptions import QueueException
from cara.facades import Log

from .DeliveryLeaseLost import DeliveryLeaseLost

QueueJobDeliveryStore: type


def _bind_store(store_type: type) -> None:
    global QueueJobDeliveryStore
    QueueJobDeliveryStore = store_type


def _delivery_due_terminal_hook_ids(self, batch_size: int | None = None) -> list[str]:
    """List a bounded batch for isolated hook subprocess execution."""
    limit = self._bounded_int(
        batch_size or self.claim_batch,
        minimum=1,
        maximum=1000,
        field="delivery_hook_batch",
    )
    now = pendulum.now("UTC")
    rows = (
        self._db().select(
            f"SELECT job_id FROM {self.table} "
            f"WHERE {self._HOOK_DUE_FILTER_TEMPLATE.format(now='%s')} "
            "ORDER BY completed_at, created_at LIMIT %s",
            [list(self.HOOK_TERMINAL_STATUSES), now, limit],
        )
        or []
    )
    return [str(self._row_value(row, "job_id")) for row in rows]


def _delivery_retry_quarantined_terminal_hooks(
    self,
    job_id: str,
    *,
    operator: str,
    reason: str,
) -> None:
    """Audit and re-arm one quarantined terminal hook for operator retry."""
    actor = self._bounded_text(operator, "operator", 200)
    audit_reason = self._safe_persisted_text(
        self._bounded_text(reason, "reason", 1000),
        maximum=1000,
    )
    now = pendulum.now("UTC")
    database = self._db()
    with database.transaction():
        row = database.select_one(
            f"SELECT status, terminal_reason, post_hooks_completed_at, "
            "post_hooks_quarantined_at, post_hooks_attempts, "
            f"post_hooks_last_error FROM {self.table} "
            "WHERE job_id = %s FOR UPDATE",
            [job_id],
        )
        if row is None:
            raise QueueException(f"Queue delivery {job_id} does not exist.")
        if str(self._row_value(row, "status")) not in (self.HOOK_TERMINAL_STATUSES):
            raise QueueException(
                f"Queue delivery {job_id} is not terminal-hook eligible."
            )
        if self._row_value(row, "post_hooks_completed_at") is not None:
            raise QueueException(
                f"Queue delivery {job_id} terminal hooks already completed."
            )
        quarantined_at = self._row_value(
            row,
            "post_hooks_quarantined_at",
        )
        if quarantined_at is None:
            raise QueueException(
                f"Queue delivery {job_id} terminal hooks are not quarantined."
            )
        if str(self._row_value(row, "terminal_reason") or "").startswith(
            "publish_envelope_invalid:"
        ):
            raise QueueException(
                "Invalid signed envelopes cannot execute terminal hooks; "
                "replay a verified delivery instead."
            )
        database.statement(
            "INSERT INTO queue_job_delivery_hook_retry_audit ("
            "job_id, requested_by, reason, prior_attempts, prior_error, "
            "requested_at) VALUES (%s, %s, %s, %s, %s, %s)",
            [
                job_id,
                actor,
                audit_reason,
                int(self._row_value(row, "post_hooks_attempts") or 0),
                self._safe_error(self._row_value(row, "post_hooks_last_error") or ""),
                now,
            ],
        )
        affected = database.statement(
            f"UPDATE {self.table} SET post_hooks_attempts = 0, "
            "post_hooks_quarantined_at = NULL, "
            "post_hooks_lease_token = NULL, "
            "post_hooks_lease_expires_at = NULL, "
            "post_hooks_last_error = NULL, updated_at = %s "
            "WHERE job_id = %s AND post_hooks_completed_at IS NULL "
            "AND post_hooks_quarantined_at = %s",
            [now, job_id, quarantined_at],
        )
        if not self._affected(affected):
            raise DeliveryLeaseLost(
                f"Queue delivery {job_id} lost its hook retry quarantine."
            )


def _delivery_settle_with_retry(
    self,
    job_id: str,
    lease_token: str,
    status: str,
    *,
    reason: str | None = None,
) -> None:
    last_error: Exception | None = None
    for attempt, delay in enumerate(
        (0.0, *self._SETTLEMENT_BACKOFF_SECONDS),
        start=1,
    ):
        if delay:
            time.sleep(delay)
        try:
            self._settle(
                job_id,
                lease_token,
                status,
                reason=reason,
            )
            return
        except DeliveryLeaseLost:
            raise
        except Exception as exc:
            last_error = exc
            if attempt <= len(self._SETTLEMENT_BACKOFF_SECONDS):
                Log.warning(
                    "Queue delivery %s terminal settlement attempt %s "
                    "failed; retrying on the same broker delivery: %s",
                    job_id,
                    attempt,
                    exc,
                    category="cara.queue.delivery",
                )
    raise QueueException(
        f"Queue delivery {job_id} terminal settlement remained unavailable."
    ) from last_error


def _delivery_settle(
    self,
    job_id: str,
    lease_token: str,
    status: str,
    *,
    db: Any | None = None,
    reason: str | None = None,
) -> None:
    if status not in self.TERMINAL_STATUSES:
        raise QueueException(f"Invalid queue delivery terminal status: {status}.")
    database = db or self._db()
    now = pendulum.now("UTC")
    affected = database.statement(
        f"UPDATE {self.table} SET status = %s, completed_at = %s, "
        "terminal_reason = %s, lease_token = NULL, "
        "lease_expires_at = NULL, updated_at = %s "
        "WHERE job_id = %s AND status = %s AND lease_token = %s",
        [
            status,
            now,
            self._safe_error(reason) if reason else None,
            now,
            job_id,
            self.STATUS_PROCESSING,
            lease_token,
        ],
    )
    if not self._affected(affected):
        existing = database.select_one(
            f"SELECT status FROM {self.table} WHERE job_id = %s",
            [job_id],
        )
        if self._row_value(existing, "status") == status:
            return
        raise DeliveryLeaseLost(
            f"Queue delivery {job_id} lost its execution lease before settlement."
        )


def _delivery_expire_due(self, batch_size: int = 5000) -> int:
    """Terminalize overdue accepted work without deleting its evidence."""
    limit = self._bounded_int(
        batch_size,
        minimum=1,
        maximum=10000,
        field="delivery_expire_batch",
    )
    now = pendulum.now("UTC")
    database = self._db()
    with database.transaction():
        rows = (
            database.select(
                f"SELECT delivery.job_id, delivery.db_job_id, "
                "tracked.status AS tracker_status "
                f"FROM {self.table} AS delivery "
                "JOIN job AS tracked ON tracked.id = delivery.db_job_id "
                "WHERE delivery.expires_at <= %s AND ("
                "delivery.status = %s OR (delivery.status = %s "
                "AND (delivery.lease_expires_at IS NULL OR "
                "delivery.lease_expires_at <= %s))) "
                "ORDER BY delivery.expires_at LIMIT %s "
                "FOR UPDATE OF delivery, tracked SKIP LOCKED",
                [
                    now,
                    self.STATUS_PENDING,
                    self.STATUS_PROCESSING,
                    now,
                    limit,
                ],
            )
            or []
        )
        for row in rows:
            job_id = str(self._row_value(row, "job_id"))
            db_job_id = int(self._row_value(row, "db_job_id"))
            tracker_status = str(self._row_value(row, "tracker_status"))
            recovered_completion = tracker_status in {
                "completed",
                "success",
            }
            target = (
                self.STATUS_COMPLETED if recovered_completion else self.STATUS_EXPIRED
            )
            reason = (
                "tracker_completion_recovered_after_stale_lease"
                if recovered_completion
                else "envelope_expired_before_terminal_settlement"
            )
            affected = database.statement(
                f"UPDATE {self.table} SET status = %s, "
                "completed_at = %s, terminal_reason = %s, "
                "lease_token = NULL, lease_expires_at = NULL, "
                "publish_status = CASE WHEN published_at IS NOT NULL "
                "THEN %s ELSE %s END, "
                "publish_lease_token = NULL, "
                "publish_lease_expires_at = NULL, updated_at = %s "
                "WHERE job_id = %s AND status IN (%s, %s)",
                [
                    target,
                    now,
                    reason,
                    self.PUBLISH_PUBLISHED,
                    self.PUBLISH_FAILED,
                    now,
                    job_id,
                    self.STATUS_PENDING,
                    self.STATUS_PROCESSING,
                ],
            )
            if not self._affected(affected):
                raise QueueException(
                    f"Queue delivery {job_id} expiry settlement was lost."
                )
            if not recovered_completion:
                self._mark_tracker_failed(database, db_job_id, now)
    count = len(rows or [])
    if count:
        Log.error(
            "%s queue deliveries expired without a terminal execution; "
            "the signed envelopes remain available for audit and replay.",
            count,
            category="cara.queue.delivery",
        )
    return count


def _delivery_recover_stale_executions(
    self,
    batch_size: int = 5000,
) -> dict[str, int]:
    """Recover worker crashes through the DB-owned publication outbox.

    Quorum queues count channel/session redeliveries toward their delivery
    limit. A worker crash therefore cannot rely on repeatedly reconnecting
    the sole broker copy while a DB execution lease remains live. Once the
    lease expires, this transaction either reconciles a terminal tracker or
    resets the delivery to the publication outbox and its tracker to
    ``pending``. A live-lease broker duplicate can then be ACKed safely:
    PostgreSQL will republish the immutable signed envelope.
    """
    limit = self._bounded_int(
        batch_size,
        minimum=1,
        maximum=10000,
        field="delivery_recovery_batch",
    )
    now = pendulum.now("UTC")
    database = self._db()
    result = {"requeued": 0, "reconciled": 0}
    with database.transaction():
        rows = (
            database.select(
                f"SELECT delivery.job_id, delivery.db_job_id, "
                "delivery.lease_token, tracked.status AS tracker_status "
                f"FROM {self.table} AS delivery "
                "JOIN job AS tracked ON tracked.id = delivery.db_job_id "
                "WHERE delivery.status = %s "
                "AND delivery.lease_expires_at IS NOT NULL "
                "AND delivery.lease_expires_at <= %s "
                "AND delivery.expires_at > %s "
                "ORDER BY delivery.lease_expires_at LIMIT %s "
                "FOR UPDATE OF delivery, tracked SKIP LOCKED",
                [
                    self.STATUS_PROCESSING,
                    now,
                    now,
                    limit,
                ],
            )
            or []
        )
        for row in rows:
            job_id = str(self._row_value(row, "job_id"))
            db_job_id = int(self._row_value(row, "db_job_id"))
            lease_token = str(self._row_value(row, "lease_token") or "")
            tracker_status = str(self._row_value(row, "tracker_status"))

            if tracker_status in {"completed", "success"}:
                target_status = self.STATUS_COMPLETED
                reason = "tracker_completion_recovered_after_worker_crash"
            elif tracker_status in {"failed", "cancelled"}:
                target_status = self.STATUS_DEAD_LETTERED
                reason = "tracker_failure_recovered_after_worker_crash"
            elif tracker_status in {"pending", "processing", "retrying"}:
                affected = database.statement(
                    f"UPDATE {self.table} SET status = %s, "
                    "publish_status = %s, publish_retry_at = %s, "
                    "published_at = NULL, lease_token = NULL, "
                    "lease_expires_at = NULL, publish_lease_token = NULL, "
                    "publish_lease_expires_at = NULL, "
                    "last_publish_error = NULL, updated_at = %s "
                    "WHERE job_id = %s AND status = %s "
                    "AND lease_token = %s AND lease_expires_at <= %s",
                    [
                        self.STATUS_PENDING,
                        self.PUBLISH_PENDING,
                        now,
                        now,
                        job_id,
                        self.STATUS_PROCESSING,
                        lease_token,
                        now,
                    ],
                )
                if not self._affected(affected):
                    raise DeliveryLeaseLost(
                        f"Queue delivery {job_id} lost its stale execution "
                        "recovery lease."
                    )
                tracker_affected = database.statement(
                    "UPDATE job SET status = %s, started_at = NULL, "
                    "completed_at = NULL, updated_at = %s WHERE id = %s "
                    "AND status = ANY(%s)",
                    [
                        "pending",
                        now,
                        db_job_id,
                        ["pending", "processing", "retrying"],
                    ],
                )
                if not self._affected(tracker_affected):
                    raise QueueException(
                        f"Tracked queue job {db_job_id} could not be reset "
                        "after a stale execution lease."
                    )
                result["requeued"] += 1
                continue
            else:
                target_status = self.STATUS_DEAD_LETTERED
                reason = "unsupported_tracker_status_after_worker_crash"
                tracker_affected = database.statement(
                    "UPDATE job SET status = %s, completed_at = COALESCE("
                    "completed_at, %s), updated_at = %s WHERE id = %s "
                    "AND status = %s",
                    [
                        "failed",
                        now,
                        now,
                        db_job_id,
                        tracker_status,
                    ],
                )
                if not self._affected(tracker_affected):
                    Log.error(
                        "Tracked queue job %s retained unsupported crash "
                        "recovery status %r; delivery will still be "
                        "quarantined.",
                        db_job_id,
                        tracker_status,
                        category="cara.queue.delivery",
                    )

            affected = database.statement(
                f"UPDATE {self.table} SET status = %s, completed_at = %s, "
                "terminal_reason = %s, lease_token = NULL, "
                "lease_expires_at = NULL, publish_lease_token = NULL, "
                "publish_lease_expires_at = NULL, updated_at = %s "
                "WHERE job_id = %s AND status = %s "
                "AND lease_token = %s AND lease_expires_at <= %s",
                [
                    target_status,
                    now,
                    reason,
                    now,
                    job_id,
                    self.STATUS_PROCESSING,
                    lease_token,
                    now,
                ],
            )
            if not self._affected(affected):
                raise DeliveryLeaseLost(
                    f"Queue delivery {job_id} lost its terminal crash recovery lease."
                )
            result["reconciled"] += 1

    if result["requeued"]:
        Log.warning(
            "%s stale queue execution lease(s) returned to the durable "
            "publication outbox after worker crashes.",
            result["requeued"],
            category="cara.queue.delivery",
        )
    return result


def _delivery_prune_terminal(self, batch_size: int = 5000) -> int:
    """Delete terminal audit rows only after the configured retention."""
    limit = self._bounded_int(
        batch_size,
        minimum=1,
        maximum=10000,
        field="delivery_prune_batch",
    )
    cutoff = pendulum.now("UTC").subtract(days=self.audit_retention_days)
    affected = self._db().statement(
        f"DELETE FROM {self.table} AS source WHERE source.job_id IN ("
        f"SELECT candidate.job_id FROM {self.table} AS candidate "
        "WHERE candidate.status = ANY(%s) "
        "AND candidate.completed_at IS NOT NULL "
        "AND (candidate.post_hooks_completed_at IS NOT NULL OR "
        "candidate.post_hooks_quarantined_at IS NOT NULL) "
        "AND candidate.completed_at < %s "
        f"AND NOT EXISTS (SELECT 1 FROM {self.table} AS replay "
        "WHERE replay.replay_of = candidate.job_id) "
        "ORDER BY candidate.completed_at LIMIT %s "
        "FOR UPDATE SKIP LOCKED)",
        [
            list(self.TERMINAL_STATUSES),
            cutoff,
            limit,
        ],
    )
    try:
        return int(affected or 0)
    except TypeError, ValueError:
        return 0


def _delivery_execution_timeout_for(self, job_or_class: Any) -> int:
    """Resolve a trusted class timeout and keep it inside the DB lease."""
    job_class = job_or_class if isinstance(job_or_class, type) else type(job_or_class)
    raw = inspect.getattr_static(
        job_class,
        "timeout",
        self.default_job_timeout_seconds,
    )
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise QueueException(
            f"{job_class.__name__}.timeout must be a numeric class policy."
        )
    timeout = int(raw)
    if (
        timeout <= 0
        or timeout + self.execution_lease_grace_seconds > self.execution_lease_seconds
    ):
        raise QueueException(
            f"{job_class.__name__}.timeout must be positive and timeout + "
            f"lease grace ({self.execution_lease_grace_seconds}s) must not "
            f"exceed the delivery execution lease "
            f"({self.execution_lease_seconds}s)."
        )
    return timeout
