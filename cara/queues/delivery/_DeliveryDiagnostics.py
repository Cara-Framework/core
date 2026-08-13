"""Delivery diagnostics, envelope validation and scalar normalization."""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import datetime
from typing import Any

import pendulum

from cara.exceptions import QueueException
from cara.support import redact_log_secrets

QueueJobDeliveryStore: type


def _bind_store(store_type: type) -> None:
    global QueueJobDeliveryStore
    QueueJobDeliveryStore = store_type


def _delivery_delivery_metrics(self) -> dict[str, Any]:
    """Return one bounded aggregate snapshot for relay-owned metrics."""
    due = self._PUBLISH_BACKLOG_FILTER_TEMPLATE.format(now="NOW()")
    priority_columns: list[str] = []
    priority_params: list[Any] = []
    for priority in self._PRIORITY_RANKS:
        alias = priority.replace("-", "_")
        due_filter = f"{due} AND priority = %s"
        priority_columns.extend(
            (
                f"COUNT(*) FILTER (WHERE {due_filter}) AS priority_{alias}_pending",
                "COALESCE(EXTRACT(EPOCH FROM (NOW() - "
                f"(MIN(available_at) FILTER (WHERE {due_filter})))), 0) "
                f"AS priority_{alias}_oldest_due_age",
            )
        )
        priority_params.extend(
            (
                self.STATUS_PENDING,
                self.PUBLISH_PUBLISHED,
                priority,
                self.STATUS_PENDING,
                self.PUBLISH_PUBLISHED,
                priority,
            )
        )
    priority_select = ", ".join(priority_columns)
    row = self._db().select_one(
        f"SELECT "
        "COUNT(*) FILTER (WHERE status = %s) AS pending, "
        "COUNT(*) FILTER (WHERE status = %s) AS processing, "
        "COUNT(*) FILTER (WHERE status = %s) AS completed, "
        "COUNT(*) FILTER (WHERE status = %s) AS retry_scheduled, "
        "COUNT(*) FILTER (WHERE status = %s) AS dead_lettered, "
        "COUNT(*) FILTER (WHERE status = %s) AS expired, "
        "COUNT(*) FILTER (WHERE status = %s AND publish_status = %s) "
        "AS publish_pending, "
        "COUNT(*) FILTER (WHERE publish_status = %s) "
        "AS publish_processing, "
        "COUNT(*) FILTER (WHERE status = %s AND "
        # psycopg2 interpolates every lone %, so the LIKE wildcard must be
        # doubled — a bare % here is read as a placeholder and detonates
        # with "IndexError: list index out of range" at execute time.
        "terminal_reason LIKE 'publish_envelope_invalid:%%') "
        "AS publish_quarantined, "
        "COUNT(*) FILTER (WHERE publish_status = %s AND "
        "publish_lease_expires_at <= NOW()) AS stale_publish, "
        "COUNT(*) FILTER (WHERE status = %s AND "
        "lease_expires_at <= NOW()) AS stale_execution, "
        "COUNT(*) FILTER (WHERE status = ANY(%s) AND "
        "post_hooks_completed_at IS NULL AND "
        "post_hooks_lease_token IS NULL) AS hook_pending, "
        "COUNT(*) FILTER (WHERE status = ANY(%s) AND "
        "post_hooks_completed_at IS NULL AND "
        "post_hooks_lease_token IS NOT NULL AND "
        "post_hooks_lease_expires_at > NOW()) AS hook_processing, "
        "COUNT(*) FILTER (WHERE status = ANY(%s) AND "
        "post_hooks_completed_at IS NULL AND "
        "post_hooks_lease_token IS NOT NULL AND "
        "post_hooks_lease_expires_at <= NOW()) AS hook_stale, "
        "COUNT(*) FILTER (WHERE status = ANY(%s) AND "
        "post_hooks_completed_at IS NULL AND "
        "post_hooks_last_error IS NOT NULL) AS hook_failed, "
        "COUNT(*) FILTER (WHERE status = ANY(%s) AND "
        "post_hooks_quarantined_at IS NOT NULL) AS hook_quarantined, "
        "COALESCE(EXTRACT(EPOCH FROM (NOW() - "
        f"(MIN(available_at) FILTER (WHERE {due})))), 0) "
        "AS oldest_due_age, "
        f"{priority_select}, "
        "COALESCE((SELECT MAX(window_count) FROM ("
        f"SELECT COUNT(*) AS window_count FROM {self.table} AS window_row "
        "WHERE window_row.status IN (%s, %s) AND ("
        "window_row.publish_status = %s OR "
        "(window_row.publish_status = %s AND "
        "window_row.publish_lease_expires_at > NOW())) "
        "GROUP BY window_row.queue) AS broker_windows), 0) "
        "AS broker_max_outstanding "
        f"FROM {self.table}",
        [
            self.STATUS_PENDING,
            self.STATUS_PROCESSING,
            self.STATUS_COMPLETED,
            self.STATUS_RETRY_SCHEDULED,
            self.STATUS_DEAD_LETTERED,
            self.STATUS_EXPIRED,
            self.STATUS_PENDING,
            self.PUBLISH_PENDING,
            self.PUBLISH_PROCESSING,
            self.STATUS_DEAD_LETTERED,
            self.PUBLISH_PROCESSING,
            self.STATUS_PROCESSING,
            list(self.HOOK_TERMINAL_STATUSES),
            list(self.HOOK_TERMINAL_STATUSES),
            list(self.HOOK_TERMINAL_STATUSES),
            list(self.HOOK_TERMINAL_STATUSES),
            list(self.HOOK_TERMINAL_STATUSES),
            self.STATUS_PENDING,
            self.PUBLISH_PUBLISHED,
            *priority_params,
            self.STATUS_PENDING,
            self.STATUS_PROCESSING,
            self.PUBLISH_PUBLISHED,
            self.PUBLISH_PROCESSING,
        ],
    )
    lane_rows = self._db().select(
        f"SELECT queue, "
        f"COUNT(*) FILTER (WHERE {due}) AS pending, "
        "COUNT(*) FILTER (WHERE status = %s) AS processing, "
        "COUNT(*) FILTER (WHERE status IN (%s, %s) AND "
        "publish_status = %s) AS broker_outstanding, "
        "COUNT(*) FILTER (WHERE status = %s AND "
        "completed_at >= NOW() - INTERVAL '5 minutes') AS completed_5m, "
        "COALESCE(EXTRACT(EPOCH FROM (NOW() - "
        f"(MIN(available_at) FILTER (WHERE {due})))), 0) "
        "AS oldest_due_age "
        f"FROM {self.table} WHERE queue = ANY(%s) GROUP BY queue",
        [
            self.STATUS_PENDING,
            self.PUBLISH_PUBLISHED,
            self.STATUS_PROCESSING,
            self.STATUS_PENDING,
            self.STATUS_PROCESSING,
            self.PUBLISH_PUBLISHED,
            self.STATUS_COMPLETED,
            self.STATUS_PENDING,
            self.PUBLISH_PUBLISHED,
            list(self.canonical_queues),
        ],
    )
    lane_backlog = {
        queue: {
            "pending": 0,
            "processing": 0,
            "broker_outstanding": 0,
            "oldest_due_age": 0.0,
            "throughput_per_second": 0.0,
        }
        for queue in self.canonical_queues
    }
    for lane_row in lane_rows or ():
        queue = str(self._row_value(lane_row, "queue") or "")
        if queue not in lane_backlog:
            continue
        lane_backlog[queue] = {
            "pending": int(self._row_value(lane_row, "pending") or 0),
            "processing": int(self._row_value(lane_row, "processing") or 0),
            "broker_outstanding": int(
                self._row_value(lane_row, "broker_outstanding") or 0
            ),
            "oldest_due_age": max(
                float(self._row_value(lane_row, "oldest_due_age") or 0),
                0.0,
            ),
            "throughput_per_second": max(
                float(self._row_value(lane_row, "completed_5m") or 0) / 300.0,
                0.0,
            ),
        }

    lane_rows = self._db().select(
        f"SELECT queue, "
        f"COUNT(*) FILTER (WHERE {due}) AS pending, "
        "COUNT(*) FILTER (WHERE status = %s) AS processing, "
        "COUNT(*) FILTER (WHERE status IN (%s, %s) AND "
        "publish_status = %s) AS broker_outstanding, "
        "COUNT(*) FILTER (WHERE status = %s AND "
        "completed_at >= NOW() - INTERVAL '5 minutes') AS completed_5m, "
        "COALESCE(EXTRACT(EPOCH FROM (NOW() - "
        f"(MIN(available_at) FILTER (WHERE {due})))), 0) "
        "AS oldest_due_age "
        f"FROM {self.table} WHERE queue = ANY(%s) GROUP BY queue",
        [
            self.STATUS_PENDING,
            self.PUBLISH_PUBLISHED,
            self.STATUS_PROCESSING,
            self.STATUS_PENDING,
            self.STATUS_PROCESSING,
            self.PUBLISH_PUBLISHED,
            self.STATUS_COMPLETED,
            self.STATUS_PENDING,
            self.PUBLISH_PUBLISHED,
            list(self.canonical_queues),
        ],
    )
    lane_backlog = {
        queue: {
            "pending": 0,
            "processing": 0,
            "broker_outstanding": 0,
            "oldest_due_age": 0.0,
            "throughput_per_second": 0.0,
        }
        for queue in self.canonical_queues
    }
    for lane_row in lane_rows or ():
        queue = str(self._row_value(lane_row, "queue") or "")
        if queue not in lane_backlog:
            continue
        lane_backlog[queue] = {
            "pending": int(self._row_value(lane_row, "pending") or 0),
            "processing": int(self._row_value(lane_row, "processing") or 0),
            "broker_outstanding": int(
                self._row_value(lane_row, "broker_outstanding") or 0
            ),
            "oldest_due_age": max(
                float(self._row_value(lane_row, "oldest_due_age") or 0),
                0.0,
            ),
            "throughput_per_second": max(
                float(self._row_value(lane_row, "completed_5m") or 0) / 300.0,
                0.0,
            ),
        }

    def value(key: str) -> int:
        return int(self._row_value(row, key) or 0)

    return {
        "statuses": {
            status: value(status)
            for status in (
                self.STATUS_PENDING,
                self.STATUS_PROCESSING,
                self.STATUS_COMPLETED,
                self.STATUS_RETRY_SCHEDULED,
                self.STATUS_DEAD_LETTERED,
                self.STATUS_EXPIRED,
            )
        },
        "publish_pending": value("publish_pending"),
        "publish_processing": value("publish_processing"),
        "publish_quarantined": value("publish_quarantined"),
        "oldest_due_age": max(
            float(self._row_value(row, "oldest_due_age") or 0),
            0.0,
        ),
        "priority_backlog": {
            priority: {
                "pending": value(f"priority_{priority}_pending"),
                "oldest_due_age": max(
                    float(
                        self._row_value(
                            row,
                            f"priority_{priority}_oldest_due_age",
                        )
                        or 0
                    ),
                    0.0,
                ),
                "latency_budget": (self._PRIORITY_RANKS[priority] + 1)
                * self.priority_aging_seconds,
            }
            for priority in self._PRIORITY_RANKS
        },
        "lane_backlog": lane_backlog,
        "broker_window": {
            "max_outstanding": value("broker_max_outstanding"),
            "limit": self.broker_window_per_queue,
        },
        "stale_leases": {
            "publish": value("stale_publish"),
            "execution": value("stale_execution"),
        },
        "hooks": {
            "pending": value("hook_pending"),
            "processing": value("hook_processing"),
            "stale": value("hook_stale"),
            "failed": value("hook_failed"),
            "quarantined": value("hook_quarantined"),
        },
    }


def _delivery_verify_schema(self) -> None:
    """Fail readiness if the durable queue schema/permissions are absent."""
    self._db().select_one(
        f"SELECT job_id, db_job_id, tenant_mode, tenant_id, status, "
        "publish_status, post_hooks_attempts, "
        "post_hooks_completed_at, post_hooks_lease_token, "
        "post_hooks_lease_expires_at, post_hooks_quarantined_at "
        f"FROM {self.table} LIMIT 0"
    )
    self._db().select_one("SELECT id, status FROM job LIMIT 0")
    self._db().select_one(
        "SELECT id, job_id, requested_by, requested_at "
        "FROM queue_job_delivery_hook_retry_audit LIMIT 0"
    )


def _delivery_mark_tracker_failed(
    database: Any,
    db_job_id: int,
    now: pendulum.DateTime,
) -> None:
    QueueJobDeliveryStore._set_tracker_status(
        database,
        db_job_id,
        "failed",
        now=now,
    )


def _delivery_set_tracker_status(
    database: Any,
    db_job_id: int,
    status: str,
    *,
    now: pendulum.DateTime | None = None,
) -> None:
    timestamp = now or pendulum.now("UTC")
    if status == "completed":
        allowed = ("processing", "completed")
    elif status == "failed":
        allowed = ("pending", "retrying", "processing", "failed")
    else:
        raise QueueException(f"Unsupported atomic tracker terminal status: {status}.")
    affected = database.statement(
        "UPDATE job SET status = %s, completed_at = COALESCE("
        "completed_at, %s), updated_at = %s WHERE id = %s "
        "AND status = ANY(%s)",
        [status, timestamp, timestamp, db_job_id, list(allowed)],
    )
    if QueueJobDeliveryStore._affected(affected):
        return
    row = database.select_one(
        "SELECT status FROM job WHERE id = %s",
        [db_job_id],
    )
    if QueueJobDeliveryStore._row_value(row, "status") == status:
        return
    raise QueueException(
        f"Tracked queue job {db_job_id} contradicts terminal status {status!r}."
    )


def _delivery_db(self) -> Any:
    if self.application is None or not self.application.has("DB"):
        raise QueueException("Durable AMQP delivery requires the application DB binding.")
    return self.application.make("DB")


def _delivery_require_ledger_schema(self, database: Any) -> None:
    """Fail with an actionable message when the outbox table is absent.

    Since the direct-to-broker path was removed, ``AMQPDriver.push`` ALWAYS
    writes here first, so this table is a hard runtime dependency of the
    amqp driver rather than an optional feature. An app that upgrades cara
    without running the ledger migration otherwise learns this as a raw
    ``UndefinedTable`` surfacing from inside a dispatch transaction — which
    reads like an app bug and, where the caller logs-and-continues, is
    invisible until someone notices the jobs never ran.

    Checked lazily on first register() rather than at driver registration:
    the driver is built during provider boot, where an api process may have
    no DB yet (or none at all), and refusing to boot there would be a
    regression for a process that never dispatches. The flag makes it one
    probe per store instance, off the per-dispatch path.
    """
    if self._ledger_schema_verified:
        return
    present = database.select_one(
        "SELECT to_regclass(%s) AS table_name", [f"public.{self.table}"]
    )
    name = (present or {}).get("table_name") if isinstance(present, dict) else None
    if not name:
        raise QueueException(
            f"The queue delivery ledger table {self.table!r} does not "
            "exist, so no job can be dispatched: since cara's durable-queue "
            "cutover the amqp driver has no direct-to-broker path and every "
            "dispatch is written to this outbox first (`craft queue:relay` "
            "is what publishes it). Run `craft migrate` to apply the "
            "delivery-ledger migration. If the app has no such migration, "
            "it never adopted the durable-queue contract — the ledger "
            "tables are declared by cara.models and generated with "
            "`craft make:migration --overwrite --force`."
        )
    unique_schema = database.select_one(
        "SELECT "
        "EXISTS (SELECT 1 FROM information_schema.columns "
        "WHERE table_schema = 'public' AND table_name = %s "
        "AND column_name = 'unique_key') AS has_unique_key, "
        "EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' "
        "AND tablename = %s AND indexname = %s) AS has_unique_index",
        [
            self.table,
            self.table,
            "queue_job_delivery_open_unique_key_idx",
        ],
    )
    if not bool(self._row_value(unique_schema, "has_unique_key")) or not bool(
        self._row_value(unique_schema, "has_unique_index")
    ):
        raise QueueException(
            "Queue delivery ledger is missing its unique_key column or "
            "open-delivery unique index. Run `craft migrate` to apply the "
            "forward-only delivery-ledger uniqueness migration before "
            "dispatching jobs."
        )
    self._ledger_schema_verified = True


def _delivery_tenant_scope(payload: Mapping[str, Any]) -> tuple[str, int | None]:
    mode = payload.get("_tenant_mode")
    tenant_id = payload.get("_tenant")
    if mode == "central" and tenant_id is None:
        return "central", None
    if (
        mode == "tenant"
        and isinstance(tenant_id, int)
        and not isinstance(tenant_id, bool)
        and tenant_id > 0
    ):
        return "tenant", tenant_id
    raise QueueException("Queue delivery requires a canonical signed tenant scope.")


def _delivery_safe_error(error: Any) -> str:
    return QueueJobDeliveryStore._safe_persisted_text(
        error,
        maximum=2000,
    )


def _delivery_safe_persisted_text(value: Any, *, maximum: int) -> str:

    return redact_log_secrets(value).replace("\x00", "")[:maximum]


def _delivery_envelope_bytes(value: Any) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf-8")
    if isinstance(value, dict):
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    raise QueueException("Queue delivery envelope is not a JSON object.")


def _delivery_row_value(row: Any, key: str) -> Any:
    if row is None:
        return None
    if isinstance(row, dict):
        return row.get(key)
    return getattr(row, key, None)


def _delivery_affected(value: Any) -> bool:
    try:
        return int(value or 0) > 0
    except TypeError, ValueError:
        return bool(value)


def _delivery_as_datetime(value: Any) -> pendulum.DateTime | None:
    if value is None:
        return None
    if isinstance(value, pendulum.DateTime):
        return value.in_timezone("UTC")
    if isinstance(value, str):
        return pendulum.parse(value).in_timezone("UTC")
    if isinstance(value, datetime):
        return pendulum.instance(value).in_timezone("UTC")
    return pendulum.parse(str(value)).in_timezone("UTC")


def _delivery_bounded_int(value: Any, *, minimum: int, maximum: int, field: str) -> int:
    if isinstance(value, bool):
        raise QueueException(f"{field} must be an integer.")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise QueueException(f"{field} must be an integer.") from exc
    if not minimum <= parsed <= maximum:
        raise QueueException(f"{field} must be between {minimum} and {maximum}.")
    return parsed


def _delivery_bounded_text(value: Any, field: str, maximum: int) -> str:
    if not isinstance(value, str) or not value.strip():
        raise QueueException(f"{field} must be a non-empty string.")
    normalized = value.strip()
    if len(normalized) > maximum:
        raise QueueException(f"{field} exceeds {maximum} characters.")
    return normalized
