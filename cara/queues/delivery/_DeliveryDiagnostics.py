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
    """Return one aggregate snapshot for relay-owned metrics.

    The driver runs this on a cadence (``AMQPDriver.refresh_delivery_metrics``),
    never once per relay iteration. ``dead_lettered``/``expired`` count the
    rows AWAITING REPLAY (``_AWAITING_REPLAY_FILTER_TEMPLATE``), as the
    operator tools do: a replay never touches its source, so counting every
    terminal row kept ``QueueDeliveryTerminalFailure`` firing for the whole
    90-day retention after one dead letter, recovered or not.

    No part of it reads the ledger without a predicate an index answers.
    It used to be one ``COUNT(*) FILTER``
    pass over the whole table: at 700k retained rows (1.5 GB of heap, live
    2026-11-12) that sequential scan took 0.6 s warm and up to 12 s cold,
    every 15 s, in the relay AND the hooks runner, inline in the relay loop.
    Each family now reads its own scope, and the predicates that make a
    scope index-served are implied by the table's CHECK constraints, so
    every count means exactly what it did over the whole table:

    * open rows — ``status IN (pending, processing)``: the
      ``active_stats``/``execution_lease`` indexes;
    * publish leases — ``publish_status = processing``: ``publish_due``;
    * terminal hooks — ``status`` hook-terminal AND
      ``post_hooks_completed_at IS NULL``: ``post_hook_stats``. A quarantined
      hook always has a NULL completion stamp
      (``queue_job_delivery_post_hook_quarantine_check``);
    * settled history — ``completed``/``retry_scheduled`` AND
      ``completed_at IS NOT NULL``, which every terminal row carries
      (``queue_job_delivery_execution_state_check``): an index-only count of
      ``terminal_retention``, never the heap;
    * failures awaiting replay — ``awaiting_replay`` anti-joined to
      ``replay``, both index-only;
    * publish quarantine — the ``publish_quarantine`` partial index.
    """
    due = self._PUBLISH_BACKLOG_FILTER_TEMPLATE.format(now="NOW()")
    awaiting_replay = self._AWAITING_REPLAY_FILTER_TEMPLATE.format(table=self.table)
    table = self.table
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
    hook_statuses = list(self.HOOK_TERMINAL_STATUSES)
    # One statement, so every family reads the same snapshot.
    row = self._db().select_one(
        "SELECT open_rows.*, publish_rows.*, hook_rows.*, settled_rows.*, "
        "failed_rows.*, quarantined_rows.*, "
        "COALESCE((SELECT MAX(window_count) FROM ("
        f"SELECT COUNT(*) AS window_count FROM {table} AS window_row "
        "WHERE window_row.status IN (%s, %s) AND ("
        "window_row.publish_status = %s OR "
        "(window_row.publish_status = %s AND "
        "window_row.publish_lease_expires_at > NOW())) "
        "GROUP BY window_row.queue) AS broker_windows), 0) "
        "AS broker_max_outstanding "
        "FROM ("
        "SELECT COUNT(*) FILTER (WHERE status = %s) AS pending, "
        "COUNT(*) FILTER (WHERE status = %s) AS processing, "
        "COUNT(*) FILTER (WHERE status = %s AND publish_status = %s) "
        "AS publish_pending, "
        "COUNT(*) FILTER (WHERE status = %s AND "
        "lease_expires_at <= NOW()) AS stale_execution, "
        "COALESCE(EXTRACT(EPOCH FROM (NOW() - "
        f"(MIN(available_at) FILTER (WHERE {due})))), 0) "
        "AS oldest_due_age, "
        f"{priority_select} "
        f"FROM {table} WHERE status IN (%s, %s)) AS open_rows "
        "CROSS JOIN ("
        "SELECT COUNT(*) AS publish_processing, "
        "COUNT(*) FILTER (WHERE publish_lease_expires_at <= NOW()) "
        f"AS stale_publish FROM {table} WHERE publish_status = %s"
        ") AS publish_rows "
        "CROSS JOIN ("
        "SELECT COUNT(*) FILTER (WHERE post_hooks_lease_token IS NULL) "
        "AS hook_pending, "
        "COUNT(*) FILTER (WHERE post_hooks_lease_token IS NOT NULL AND "
        "post_hooks_lease_expires_at > NOW()) AS hook_processing, "
        "COUNT(*) FILTER (WHERE post_hooks_lease_token IS NOT NULL AND "
        "post_hooks_lease_expires_at <= NOW()) AS hook_stale, "
        "COUNT(*) FILTER (WHERE post_hooks_last_error IS NOT NULL) "
        "AS hook_failed, "
        "COUNT(*) FILTER (WHERE post_hooks_quarantined_at IS NOT NULL) "
        f"AS hook_quarantined FROM {table} WHERE status = ANY(%s) "
        "AND post_hooks_completed_at IS NULL"
        ") AS hook_rows "
        "CROSS JOIN ("
        "SELECT COUNT(*) FILTER (WHERE status = %s) AS completed, "
        "COUNT(*) FILTER (WHERE status = %s) AS retry_scheduled "
        f"FROM {table} WHERE status IN (%s, %s) AND completed_at IS NOT NULL"
        ") AS settled_rows "
        "CROSS JOIN ("
        "SELECT COUNT(*) FILTER (WHERE status = %s) AS dead_lettered, "
        "COUNT(*) FILTER (WHERE status = %s) AS expired "
        f"FROM {table} WHERE status IN (%s, %s) AND {awaiting_replay}"
        ") AS failed_rows "
        "CROSS JOIN ("
        "SELECT COUNT(*) AS publish_quarantined "
        f"FROM {table} WHERE status = %s AND "
        f"{self._PUBLISH_QUARANTINE_FILTER}"
        ") AS quarantined_rows",
        [
            self.STATUS_PENDING,
            self.STATUS_PROCESSING,
            self.PUBLISH_PUBLISHED,
            self.PUBLISH_PROCESSING,
            # open_rows
            self.STATUS_PENDING,
            self.STATUS_PROCESSING,
            self.STATUS_PENDING,
            self.PUBLISH_PENDING,
            self.STATUS_PROCESSING,
            self.STATUS_PENDING,
            self.PUBLISH_PUBLISHED,
            *priority_params,
            self.STATUS_PENDING,
            self.STATUS_PROCESSING,
            # publish_rows
            self.PUBLISH_PROCESSING,
            # hook_rows
            hook_statuses,
            # settled_rows
            self.STATUS_COMPLETED,
            self.STATUS_RETRY_SCHEDULED,
            self.STATUS_COMPLETED,
            self.STATUS_RETRY_SCHEDULED,
            # failed_rows
            self.STATUS_DEAD_LETTERED,
            self.STATUS_EXPIRED,
            self.STATUS_DEAD_LETTERED,
            self.STATUS_EXPIRED,
            # quarantined_rows
            self.STATUS_DEAD_LETTERED,
        ],
    )
    # Every column reads open rows or the last five minutes of completions,
    # so the scan is held to exactly those: the active-stats and
    # queue-terminal partial indexes serve it, not the retained history.
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
        f"FROM {self.table} WHERE queue = ANY(%s) AND ("
        "status IN (%s, %s) OR (status = %s AND "
        "completed_at >= NOW() - INTERVAL '5 minutes')) GROUP BY queue",
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
            self.STATUS_PENDING,
            self.STATUS_PROCESSING,
            self.STATUS_COMPLETED,
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
