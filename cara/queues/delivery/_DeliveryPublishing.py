"""Broker publication, envelope validation and dead-letter operations."""

from __future__ import annotations

import uuid
from typing import Any

import pendulum

from cara.exceptions import QueueException
from cara.facades import Log
from cara.queues.serializers import SignedJsonJobSerializer

from .DeliveryClaim import DeliveryClaim
from .DeliveryEnvelopeExpired import DeliveryEnvelopeExpired
from .DeliveryEnvelopeMismatch import DeliveryEnvelopeMismatch
from .DeliveryLeaseLost import DeliveryLeaseLost

QueueJobDeliveryStore: type


def _bind_store(store_type: type) -> None:
    global QueueJobDeliveryStore
    QueueJobDeliveryStore = store_type


def _delivery_claim_next_publish(self) -> tuple[Any, str] | None:
    """Atomically lease the next due row: fair across tenants, then by aged priority.

    A bounded per-queue broker window keeps the backlog in this ledger
    instead of preloading RabbitMQ, so THIS statement is the scheduler: the
    order it picks rows in is the order work starts.

    Within one queue the pick is, in order:

    1. **Tenant fair share.** Each tenant with due work offers its own head
       row (one per priority lane); the tenant holding the FEWEST rows in
       this queue's broker window goes first, and among those the tenant
       this queue served LEAST RECENTLY (its newest ``published_at``; never
       served = first) — a round robin over tenants. One tenant's
       10,000-row backlog, or a hundred tenants' backlogs, therefore never
       sit ahead of another tenant's single job: the newcomer holds nothing
       and was never served, so its head is the next row published. Central
       (tenantless) rows share one fair-share key.
    2. **Aged priority.** Between a tenant's own rows, the highest
       effective tier wins; each ``priority_aging_seconds`` of waiting
       promotes a row one tier, so a tenant's low work reaches the critical
       tier after three intervals and cannot starve behind its own urgent
       traffic. Priority is deliberately NOT ranked above the round robin:
       an aged backlog would otherwise outrank every other tenant's fresh
       work again.
    3. **Age.** Oldest ``available_at`` / ``created_at`` first.

    ``tenant_window_per_queue`` (when > 0) additionally caps how many
    window slots one tenant may hold in a queue, so a backlogged tenant
    alone never fills the whole window and a newly arrived tenant's job
    finds a free slot at once rather than after an in-flight job finishes.

    The tenants with due work are found by a recursive skip scan over
    ``queue_job_delivery_tenant_head_idx`` and each one's last service by
    ``queue_job_delivery_tenant_served_idx`` — a few index probes per tenant
    per priority lane, never a walk of a backlogged tenant's rows. Relay
    replicas serialize this short reservation statement with a PostgreSQL
    advisory transaction lock; ``publish_status=processing`` is the durable
    window reservation, so replicas cannot overfill RabbitMQ concurrently.
    """
    token = uuid.uuid4().hex
    now = pendulum.now("UTC")
    database = self._db()
    due_row = (
        "AND delivery.status = 'pending' "
        "AND delivery.publish_status IN ('pending','processing') "
        "AND delivery.available_at <= settings.now "
        "AND delivery.publish_retry_at <= settings.now "
        "AND delivery.expires_at > settings.now "
        "AND (delivery.publish_status = 'pending' OR "
        "(delivery.publish_status = 'processing' AND "
        "(delivery.publish_lease_expires_at IS NULL OR "
        "delivery.publish_lease_expires_at <= settings.now))) "
    )
    open_row = (
        "AND delivery.status = 'pending' "
        "AND delivery.publish_status IN ('pending','processing') "
    )
    with database.transaction():
        database.select_one(
            "SELECT pg_advisory_xact_lock(%s) AS locked",
            [self._PUBLISH_CLAIM_ADVISORY_LOCK],
        )
        row = database.select_one(
            "WITH RECURSIVE settings AS (SELECT %s::timestamptz AS now, "
            "%s::numeric AS aging_seconds, %s::integer AS broker_window, "
            "%s::integer AS tenant_window), "
            "priority_lanes(priority, base_rank) AS (VALUES "
            "('critical', 0), ('high', 1), ('default', 2), ('low', 3)), "
            "outstanding AS (SELECT outstanding.queue, "
            "COALESCE(outstanding.tenant_id, 0) AS tenant_key, COUNT(*) AS held "
            f"FROM {self.table} AS outstanding CROSS JOIN settings "
            "WHERE outstanding.queue = ANY(%s::varchar[]) "
            "AND outstanding.status IN ('pending', 'processing') "
            "AND (outstanding.publish_status = 'published' OR "
            "(outstanding.publish_status = 'processing' AND "
            "outstanding.publish_lease_expires_at > settings.now)) "
            "GROUP BY outstanding.queue, COALESCE(outstanding.tenant_id, 0)), "
            "eligible_queues AS (SELECT candidate.queue "
            "FROM unnest(%s::varchar[]) AS candidate(queue) CROSS JOIN settings "
            "WHERE COALESCE((SELECT SUM(outstanding.held) FROM outstanding "
            "WHERE outstanding.queue = candidate.queue), 0) "
            "< settings.broker_window), "
            "tenant_keys(queue, priority, base_rank, tenant_key) AS ("
            "SELECT eligible_queues.queue, priority_lanes.priority, "
            "priority_lanes.base_rank, first_key.tenant_key "
            "FROM eligible_queues CROSS JOIN priority_lanes CROSS JOIN LATERAL ("
            "SELECT COALESCE(delivery.tenant_id, 0) AS tenant_key "
            f"FROM {self.table} AS delivery "
            "WHERE delivery.queue = eligible_queues.queue "
            "AND delivery.priority = priority_lanes.priority "
            f"{open_row}"
            "ORDER BY COALESCE(delivery.tenant_id, 0) LIMIT 1) AS first_key "
            "UNION ALL "
            "SELECT tenant_keys.queue, tenant_keys.priority, "
            "tenant_keys.base_rank, next_key.tenant_key "
            "FROM tenant_keys CROSS JOIN LATERAL ("
            "SELECT COALESCE(delivery.tenant_id, 0) AS tenant_key "
            f"FROM {self.table} AS delivery "
            "WHERE delivery.queue = tenant_keys.queue "
            "AND delivery.priority = tenant_keys.priority "
            f"{open_row}"
            "AND COALESCE(delivery.tenant_id, 0) > tenant_keys.tenant_key "
            "ORDER BY COALESCE(delivery.tenant_id, 0) LIMIT 1) AS next_key), "
            "heads AS (SELECT head.job_id, head.available_at, head.created_at, "
            "tenant_keys.base_rank, COALESCE(outstanding.held, 0) AS held, "
            "last_served.published_at AS last_served_at "
            "FROM tenant_keys CROSS JOIN settings "
            "LEFT JOIN outstanding ON outstanding.queue = tenant_keys.queue "
            "AND outstanding.tenant_key = tenant_keys.tenant_key "
            "LEFT JOIN LATERAL (SELECT served.published_at "
            f"FROM {self.table} AS served "
            "WHERE served.queue = tenant_keys.queue "
            "AND COALESCE(served.tenant_id, 0) = tenant_keys.tenant_key "
            "AND served.published_at IS NOT NULL "
            "ORDER BY served.published_at DESC LIMIT 1) AS last_served ON TRUE "
            "CROSS JOIN LATERAL ("
            "SELECT delivery.job_id, delivery.available_at, delivery.created_at "
            f"FROM {self.table} AS delivery "
            "WHERE delivery.queue = tenant_keys.queue "
            "AND delivery.priority = tenant_keys.priority "
            "AND COALESCE(delivery.tenant_id, 0) = tenant_keys.tenant_key "
            f"{due_row}"
            "ORDER BY delivery.available_at, delivery.created_at LIMIT 1"
            ") AS head "
            "WHERE settings.tenant_window <= 0 OR tenant_keys.tenant_key = 0 "
            "OR COALESCE(outstanding.held, 0) < settings.tenant_window), "
            "candidate AS (SELECT heads.job_id "
            "FROM heads CROSS JOIN settings "
            "ORDER BY heads.held, heads.last_served_at NULLS FIRST, "
            "GREATEST(heads.base_rank - FLOOR(EXTRACT(EPOCH FROM "
            "(settings.now - heads.available_at)) / "
            "settings.aging_seconds), 0), heads.available_at, "
            "heads.created_at LIMIT 1) "
            f"UPDATE {self.table} AS delivery "
            "SET publish_status = 'processing', "
            "publish_lease_token = %s, publish_lease_expires_at = %s, "
            "updated_at = %s FROM candidate, settings "
            "WHERE delivery.job_id = candidate.job_id "
            # Re-checked under the row lock: the head was read without one.
            f"{due_row}"
            "RETURNING delivery.job_id, delivery.db_job_id, "
            "delivery.payload_sha256, delivery.signed_envelope, "
            "delivery.tenant_mode, delivery.tenant_id, delivery.queue, "
            "delivery.priority, delivery.publish_attempts",
            [
                now,
                self.priority_aging_seconds,
                self.broker_window_per_queue,
                self.tenant_window_per_queue,
                list(self.canonical_queues),
                list(self.canonical_queues),
                token,
                now.add(seconds=self.publish_lease_seconds),
                now,
            ],
        )
    if row is None:
        return None
    return row, token


def _delivery_publish_due(self) -> dict[str, int]:
    """Claim and publish due rows one-at-a-time with bounded leases."""
    recovery = self.recover_stale_executions(self.claim_batch)
    expired = self.expire_due(self.claim_batch)
    result = {
        "claimed": 0,
        "published": 0,
        "retried": 0,
        "quarantined": 0,
        "settle_lost": 0,
        "expired": expired,
        "stale_requeued": recovery["requeued"],
        "stale_reconciled": recovery["reconciled"],
        "unclaimed_requeued": recovery["unclaimed"],
    }
    for _ in range(self.claim_batch):
        claimed = self._claim_next_publish()
        if claimed is None:
            break
        row, token = claimed
        job_id = str(self._row_value(row, "job_id"))
        result["claimed"] += 1
        try:
            settled = self._publish_claimed(row, token)
        except DeliveryEnvelopeExpired as exc:
            self._expire_publish(
                job_id,
                token,
                str(exc),
            )
            result["expired"] += 1
        except DeliveryEnvelopeMismatch as exc:
            self._quarantine_publish(
                job_id,
                token,
                str(exc),
            )
            result["quarantined"] += 1
            Log.error(
                "Queue outbox row %s is invalid and was quarantined: %s",
                self._row_value(row, "job_id"),
                exc,
                category="cara.queue.delivery",
            )
        except Exception as exc:
            released = self._release_publish(
                job_id,
                token,
                int(self._row_value(row, "publish_attempts") or 0),
                str(exc),
            )
            result["retried" if released else "settle_lost"] += 1
            Log.warning(
                "Queue outbox publish failed for %s%s: %s",
                self._row_value(row, "job_id"),
                ("" if released else " and its retry lease was concurrently lost"),
                exc,
                category="cara.queue.delivery",
            )
            # A generic publish exception is treated as a systemic relay
            # failure. Continuing would multiply reconnect/socket timeout
            # cost by claim_batch and stampede the broker during outages.
            break
        else:
            result["published" if settled else "settle_lost"] += 1
            if not settled:
                break
    return result


def _delivery_publish_claimed(self, row: Any, token: str) -> bool:
    body = self._envelope_bytes(self._row_value(row, "signed_envelope"))
    try:
        envelope = SignedJsonJobSerializer.inspect_envelope(
            body,
            signing_keys=self.options.get("signing_keys", {}),
            clock_skew_seconds=int(self.options.get("clock_skew_seconds", 30)),
            max_age_seconds=int(
                self.options.get(
                    "envelope_max_age_seconds",
                    SignedJsonJobSerializer.DEFAULT_MAX_AGE_SECONDS,
                )
            ),
            allow_expired=True,
        )
    except QueueException as exc:
        raise DeliveryEnvelopeMismatch(str(exc)) from exc
    if SignedJsonJobSerializer.is_expired(
        envelope,
        clock_skew_seconds=int(self.options.get("clock_skew_seconds", 30)),
    ):
        raise DeliveryEnvelopeExpired(
            "Signed queue envelope expired before broker publication."
        )
    payload = envelope["payload"]
    digest = SignedJsonJobSerializer.canonical_envelope_sha256(body)
    checks = {
        "job_id": str(payload["job_id"]),
        "db_job_id": payload.get("db_job_id"),
        "payload_sha256": digest,
        "tenant_mode": payload.get("_tenant_mode"),
        "tenant_id": payload.get("_tenant"),
        "queue": payload["queue"],
        "priority": payload["priority"],
    }
    for field, value in checks.items():
        stored = self._row_value(row, field)
        if field == "job_id":
            stored = str(stored)
        if stored != value:
            raise DeliveryEnvelopeMismatch(
                f"Queue outbox immutable {field} does not match signed envelope."
            )
    self.driver._publish_registered_envelope(
        body,
        payload,
        capability=self,
    )
    now = pendulum.now("UTC")
    affected = self._db().statement(
        f"UPDATE {self.table} SET publish_status = %s, published_at = %s, "
        "publish_lease_token = NULL, publish_lease_expires_at = NULL, "
        "last_publish_error = NULL, updated_at = %s "
        "WHERE job_id = %s AND publish_status = %s "
        "AND publish_lease_token = %s",
        [
            self.PUBLISH_PUBLISHED,
            now,
            now,
            str(payload["job_id"]),
            self.PUBLISH_PROCESSING,
            token,
        ],
    )
    if self._affected(affected):
        return True
    current = self._db().select_one(
        f"SELECT publish_status, published_at FROM {self.table} WHERE job_id = %s",
        [str(payload["job_id"])],
    )
    return (
        self._row_value(current, "publish_status") == self.PUBLISH_PUBLISHED
        and self._row_value(current, "published_at") is not None
    )


def _delivery_release_publish(
    self,
    job_id: str,
    token: str,
    attempts: int,
    error: str,
) -> bool:
    next_attempt = max(attempts, 0) + 1
    index = min(next_attempt - 1, len(self._PUBLISH_BACKOFF_SECONDS) - 1)
    now = pendulum.now("UTC")
    affected = self._db().statement(
        f"UPDATE {self.table} SET publish_status = %s, "
        "publish_attempts = %s, publish_retry_at = %s, "
        "publish_lease_token = NULL, publish_lease_expires_at = NULL, "
        "last_publish_error = %s, updated_at = %s "
        "WHERE job_id = %s AND publish_status = %s "
        "AND publish_lease_token = %s",
        [
            self.PUBLISH_PENDING,
            next_attempt,
            now.add(seconds=self._PUBLISH_BACKOFF_SECONDS[index]),
            self._safe_error(error),
            now,
            job_id,
            self.PUBLISH_PROCESSING,
            token,
        ],
    )
    return self._affected(affected)


def _delivery_quarantine_publish(self, job_id: str, token: str, error: str) -> None:
    now = pendulum.now("UTC")
    safe_error = self._safe_error(error)
    database = self._db()
    with database.transaction():
        row = database.select_one(
            f"UPDATE {self.table} SET status = %s, completed_at = %s, "
            "terminal_reason = %s, publish_status = %s, "
            "publish_lease_token = NULL, publish_lease_expires_at = NULL, "
            "last_publish_error = %s, "
            "post_hooks_quarantined_at = %s, "
            "post_hooks_last_error = %s, updated_at = %s "
            "WHERE job_id = %s AND publish_status = %s "
            "AND publish_lease_token = %s RETURNING db_job_id",
            [
                self.STATUS_DEAD_LETTERED,
                now,
                self._safe_error(f"publish_envelope_invalid:{safe_error}"),
                self.PUBLISH_FAILED,
                safe_error,
                now,
                "terminal hooks skipped: signed envelope is invalid",
                now,
                job_id,
                self.PUBLISH_PROCESSING,
                token,
            ],
        )
        if row is None:
            raise QueueException(f"Queue delivery {job_id} lost its quarantine lease.")
        self._mark_tracker_failed(
            database,
            int(self._row_value(row, "db_job_id")),
            now,
        )


def _delivery_claim_execution(
    self,
    *,
    body: bytes | str | dict[str, Any],
    payload: dict[str, Any],
) -> DeliveryClaim:
    """Atomically validate identity and lease one delivery for execution."""
    digest = SignedJsonJobSerializer.canonical_envelope_sha256(body)
    job_id = str(payload["job_id"])
    # The signed budget fitted the lease policy it was signed under; a later,
    # shorter policy wins instead of failing every redelivery of trusted work.
    # The worker runs the job for min(signed, current class timeout), which
    # ``execution_timeout_for`` keeps inside the current lease, so the clamped
    # lease still covers the whole execution.
    timeout_seconds = min(
        self._bounded_int(
            payload.get("timeout_seconds"),
            minimum=1,
            maximum=86400,
            field="timeout_seconds",
        ),
        self.execution_lease_seconds - self.execution_lease_grace_seconds,
    )
    # ``timeout + grace`` is the run's whole budget and so the lease's
    # CEILING; the lease itself is the short heartbeat lease the worker
    # renews while the job runs (``renew_execution``), so a dead worker's
    # row lapses within one heartbeat lease, not within the whole budget.
    budget_seconds = timeout_seconds + self.execution_lease_grace_seconds
    lease_seconds = min(budget_seconds, self.execution_heartbeat_seconds)
    now = pendulum.now("UTC")
    database = self._db()
    with database.transaction():
        row = database.select_one(
            f"SELECT job_id, db_job_id, payload_sha256, tenant_mode, "
            f"tenant_id, status, "
            f"lease_token, lease_expires_at, expires_at, available_at, "
            f"terminal_reason "
            f"FROM {self.table} "
            "WHERE job_id = %s FOR UPDATE",
            [job_id],
        )
        if row is None:
            return DeliveryClaim("unknown")
        if (
            self._row_value(row, "payload_sha256") != digest
            or self._row_value(row, "db_job_id") != payload.get("db_job_id")
            or self._row_value(row, "tenant_mode") != payload.get("_tenant_mode")
            or self._row_value(row, "tenant_id") != payload.get("_tenant")
        ):
            return DeliveryClaim("mismatch")

        status = str(self._row_value(row, "status"))
        available_at = self._as_datetime(self._row_value(row, "available_at"))
        if available_at is None:
            return DeliveryClaim("mismatch")
        if available_at > now:
            affected = database.statement(
                f"UPDATE {self.table} SET publish_status = %s, "
                "publish_retry_at = %s, published_at = NULL, "
                "publish_lease_token = NULL, "
                "publish_lease_expires_at = NULL, updated_at = %s "
                "WHERE job_id = %s AND status = %s",
                [
                    self.PUBLISH_PENDING,
                    available_at,
                    now,
                    job_id,
                    self.STATUS_PENDING,
                ],
            )
            if not self._affected(affected):
                raise QueueException(
                    f"Queue delivery {job_id} early-publication recovery was lost."
                )
            return DeliveryClaim("not_ready")

        self._reconcile_broker_receipt(database, job_id, now)
        if status in self.TERMINAL_STATUSES:
            return DeliveryClaim(
                status,
                terminal_reason=self._row_value(row, "terminal_reason"),
            )
        reclaimed = False
        if status == self.STATUS_PROCESSING:
            lease_expiry = self._as_datetime(self._row_value(row, "lease_expires_at"))
            if lease_expiry is not None and lease_expiry > now:
                return DeliveryClaim("live_lease")
            reclaimed = True
        elif status != self.STATUS_PENDING:
            return DeliveryClaim("mismatch")

        expires_at = self._as_datetime(self._row_value(row, "expires_at"))
        if expires_at is None:
            return DeliveryClaim("mismatch")
        if expires_at <= now:
            affected = database.statement(
                f"UPDATE {self.table} SET status = %s, completed_at = %s, "
                "terminal_reason = %s, lease_token = NULL, "
                "lease_expires_at = NULL, publish_lease_token = NULL, "
                "publish_lease_expires_at = NULL, updated_at = %s "
                "WHERE job_id = %s AND status IN (%s, %s)",
                [
                    self.STATUS_EXPIRED,
                    now,
                    "envelope_expired_before_execution",
                    now,
                    job_id,
                    self.STATUS_PENDING,
                    self.STATUS_PROCESSING,
                ],
            )
            if not self._affected(affected):
                raise QueueException("Queue delivery expiry settlement was lost.")
            self._mark_tracker_failed(
                database,
                int(payload["db_job_id"]),
                now,
            )
            Log.error(
                "Queue delivery %s expired before execution; retained in "
                "the ledger for audit and operator replay.",
                job_id,
                category="cara.queue.delivery",
            )
            return DeliveryClaim(self.STATUS_EXPIRED)

        token = uuid.uuid4().hex
        affected = database.statement(
            f"UPDATE {self.table} SET status = %s, attempts = attempts + 1, "
            "lease_token = %s, lease_expires_at = %s, updated_at = %s "
            "WHERE job_id = %s AND status IN (%s, %s)",
            [
                self.STATUS_PROCESSING,
                token,
                now.add(seconds=lease_seconds),
                now,
                job_id,
                self.STATUS_PENDING,
                self.STATUS_PROCESSING,
            ],
        )
        if not self._affected(affected):
            raise QueueException("Queue delivery execution lease was lost.")
    return DeliveryClaim(
        "claimed",
        token,
        reclaimed=reclaimed,
        lease_deadline=now.add(seconds=budget_seconds),
    )


def _delivery_renew_execution(
    self,
    job_id: str,
    lease_token: str,
    *,
    deadline: Any | None = None,
) -> bool:
    """Heartbeat: extend this worker's execution lease while its job runs.

    Compare-and-set on ``lease_token`` while the row is still
    ``processing``: a lease that lapsed and was recovered (the relay
    returned the row to the outbox) or re-claimed by another worker is never
    extended. ``deadline`` — the claim's ``timeout + grace`` budget — caps
    the extension, so the heartbeat can never hold a row longer than the
    pre-heartbeat lease did. Returns False once the lease is no longer this
    worker's, or the budget is spent; the heartbeat then stops.
    """
    now = pendulum.now("UTC")
    expiry = now.add(seconds=self.execution_heartbeat_seconds)
    if deadline is not None:
        if deadline <= now:
            return False
        expiry = min(expiry, deadline)
    affected = self._db().statement(
        f"UPDATE {self.table} SET lease_expires_at = %s, updated_at = %s "
        "WHERE job_id = %s AND status = %s AND lease_token = %s",
        [
            expiry,
            now,
            job_id,
            self.STATUS_PROCESSING,
            lease_token,
        ],
    )
    return self._affected(affected)


def _delivery_reconcile_broker_receipt(
    self,
    database: Any,
    job_id: str,
    now: pendulum.DateTime,
) -> None:
    """Close the publisher-confirm/ledger-CAS crash window on receipt."""
    affected = database.statement(
        f"UPDATE {self.table} SET publish_status = %s, "
        "published_at = COALESCE(published_at, %s), "
        "publish_lease_token = NULL, publish_lease_expires_at = NULL, "
        "last_publish_error = NULL, updated_at = %s "
        "WHERE job_id = %s AND publish_status IN (%s, %s, %s, %s)",
        [
            self.PUBLISH_PUBLISHED,
            now,
            now,
            job_id,
            self.PUBLISH_PENDING,
            self.PUBLISH_PROCESSING,
            self.PUBLISH_PUBLISHED,
            self.PUBLISH_FAILED,
        ],
    )
    if not self._affected(affected):
        raise DeliveryLeaseLost(
            f"Queue delivery {job_id} broker receipt was not reconciled."
        )


def _delivery_complete(self, job_id: str, lease_token: str) -> None:
    self._settle_with_retry(job_id, lease_token, self.STATUS_COMPLETED)


def _delivery_complete_with_tracker(
    self,
    job_id: str,
    lease_token: str,
    *,
    db_job_id: int,
) -> None:
    self._settle_execution_with_tracker(
        job_id,
        lease_token,
        db_job_id=db_job_id,
        status=self.STATUS_COMPLETED,
        tracker_status="completed",
    )


def _delivery_dead_letter(
    self,
    job_id: str,
    lease_token: str,
    *,
    reason: str,
) -> None:
    self._settle_with_retry(
        job_id,
        lease_token,
        self.STATUS_DEAD_LETTERED,
        reason=reason,
    )
