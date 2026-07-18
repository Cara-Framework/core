"""Durable delivery ledger and transactional AMQP publish outbox."""

from __future__ import annotations

import asyncio
import inspect
import json
import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pendulum

from cara.exceptions import QueueException
from cara.facades import Log
from cara.queues.serializers import SignedJsonJobSerializer


@dataclass(frozen=True)
class DeliveryClaim:
    outcome: str
    lease_token: str | None = None
    reclaimed: bool = False
    terminal_reason: str | None = None


@dataclass(frozen=True)
class TerminalHookClaim:
    outcome: str
    lease_token: str | None = None
    signed_envelope: bytes | None = None
    status: str | None = None
    terminal_reason: str | None = None


@dataclass(frozen=True)
class ReplayDelivery:
    job_id: str
    status: str
    publish_status: str
    expires_at: pendulum.DateTime | None

    def is_accepted(self, now: pendulum.DateTime | None = None) -> bool:
        current = now or pendulum.now("UTC")
        if self.status in {
            QueueJobDeliveryStore.STATUS_COMPLETED,
            QueueJobDeliveryStore.STATUS_RETRY_SCHEDULED,
        }:
            return True
        return (
            self.status
            in {
                QueueJobDeliveryStore.STATUS_PENDING,
                QueueJobDeliveryStore.STATUS_PROCESSING,
            }
            and self.expires_at is not None
            and self.expires_at > current
        )


class DeliveryEnvelopeMismatch(QueueException):
    """Persisted outbox metadata and its signed envelope disagree."""


class DeliveryEnvelopeExpired(QueueException):
    """A valid immutable envelope expired before broker publication."""


class DeliveryLeaseLost(QueueException):
    """Execution settlement no longer owns the delivery lease."""


class QueueJobDeliveryStore:
    """Single source of truth for queue publication and execution state."""

    STATUS_PENDING = "pending"
    STATUS_PROCESSING = "processing"
    STATUS_COMPLETED = "completed"
    STATUS_RETRY_SCHEDULED = "retry_scheduled"
    STATUS_DEAD_LETTERED = "dead_lettered"
    STATUS_EXPIRED = "expired"
    TERMINAL_STATUSES = frozenset(
        {
            STATUS_COMPLETED,
            STATUS_RETRY_SCHEDULED,
            STATUS_DEAD_LETTERED,
            STATUS_EXPIRED,
        }
    )
    HOOK_TERMINAL_STATUSES = frozenset(
        {
            STATUS_COMPLETED,
            STATUS_DEAD_LETTERED,
            STATUS_EXPIRED,
        }
    )

    PUBLISH_PENDING = "pending"
    PUBLISH_PROCESSING = "processing"
    PUBLISH_PUBLISHED = "published"
    PUBLISH_FAILED = "failed"

    _PUBLISH_BACKOFF_SECONDS = (1, 5, 30, 60, 300)
    _HOOK_BACKOFF_SECONDS = (60, 300, 900, 3600, 21600, 86400)
    _SETTLEMENT_BACKOFF_SECONDS = (0.05, 0.25, 1.0, 2.0, 5.0)
    _PRIORITY_RANKS = {
        "critical": 0,
        "high": 1,
        "default": 2,
        "low": 3,
    }
    _PUBLISH_CLAIM_ADVISORY_LOCK = 7_190_342_541
    DEFAULT_JOB_TIMEOUT_SECONDS = 300
    POST_HOOK_LEASE_SECONDS = 300

    def __init__(self, application: Any, driver: Any, options: dict[str, Any]):
        self.application = application
        self.driver = driver
        self.options = options
        self.table = str(options.get("delivery_table") or "queue_job_delivery")
        if self.table != "queue_job_delivery":
            raise QueueException(
                "AMQP delivery_table must be the canonical 'queue_job_delivery'."
            )
        canonical_queues = getattr(driver, "_canonical_queues", None) or options.get(
            "canonical_queues"
        )
        self.canonical_queues = tuple(
            sorted(
                {
                    str(queue).strip()
                    for queue in (canonical_queues or ())
                    if str(queue).strip()
                }
            )
        )
        if not self.canonical_queues:
            raise QueueException(
                "AMQP delivery ledger requires canonical_queues."
            )
        self.claim_batch = self._bounded_int(
            options.get("delivery_claim_batch", 100),
            minimum=1,
            maximum=1000,
            field="delivery_claim_batch",
        )
        self.publish_lease_seconds = self._bounded_int(
            options.get("delivery_publish_lease_seconds", 300),
            minimum=30,
            maximum=3600,
            field="delivery_publish_lease_seconds",
        )
        self.priority_aging_seconds = self._bounded_int(
            options.get("delivery_priority_aging_seconds", 300),
            minimum=30,
            maximum=86400,
            field="delivery_priority_aging_seconds",
        )
        self.broker_window_per_queue = self._bounded_int(
            options.get("delivery_broker_window_per_queue", 2),
            minimum=1,
            maximum=10000,
            field="delivery_broker_window_per_queue",
        )
        self.execution_lease_seconds = self._bounded_int(
            options.get("delivery_execution_lease_seconds", 7200),
            minimum=60,
            maximum=86400,
            field="delivery_execution_lease_seconds",
        )
        self.execution_lease_grace_seconds = self._bounded_int(
            options.get("delivery_execution_lease_grace_seconds", 300),
            minimum=30,
            maximum=3600,
            field="delivery_execution_lease_grace_seconds",
        )
        self.default_job_timeout_seconds = self._bounded_int(
            options.get(
                "delivery_default_job_timeout_seconds",
                self.DEFAULT_JOB_TIMEOUT_SECONDS,
            ),
            minimum=1,
            maximum=86399,
            field="delivery_default_job_timeout_seconds",
        )
        if (
            self.default_job_timeout_seconds
            + self.execution_lease_grace_seconds
            > self.execution_lease_seconds
        ):
            raise QueueException(
                "delivery_default_job_timeout_seconds plus "
                "delivery_execution_lease_grace_seconds must not exceed "
                "delivery_execution_lease_seconds."
            )
        self.audit_retention_days = self._bounded_int(
            options.get("delivery_audit_retention_days", 90),
            minimum=1,
            maximum=3650,
            field="delivery_audit_retention_days",
        )
        self.audit_safety_days = self._bounded_int(
            options.get("delivery_audit_safety_days", 7),
            minimum=1,
            maximum=365,
            field="delivery_audit_safety_days",
        )
        self.hook_timeout_seconds = self._bounded_int(
            options.get("delivery_hook_timeout_seconds", 60),
            minimum=1,
            maximum=self.POST_HOOK_LEASE_SECONDS - 1,
            field="delivery_hook_timeout_seconds",
        )
        self.hook_max_attempts = self._bounded_int(
            options.get("delivery_hook_max_attempts", 10),
            minimum=1,
            maximum=100,
            field="delivery_hook_max_attempts",
        )
        envelope_max_age_seconds = self._bounded_int(
            options.get(
                "envelope_max_age_seconds",
                SignedJsonJobSerializer.DEFAULT_MAX_AGE_SECONDS,
            ),
            minimum=300,
            maximum=10 * 365 * 24 * 60 * 60,
            field="envelope_max_age_seconds",
        )
        if self.audit_retention_days * 86400 <= (
            envelope_max_age_seconds + self.audit_safety_days * 86400
        ):
            raise QueueException(
                "delivery_audit_retention_days must exceed the envelope "
                "maximum age plus delivery_audit_safety_days."
            )

    def register(
        self,
        *,
        body: bytes | str | dict[str, Any],
        payload: dict[str, Any],
        envelope: dict[str, Any],
        replay_of: str | None = None,
        replay_requested_by: str | None = None,
        replay_reason: str | None = None,
        db: Any | None = None,
    ) -> bool:
        """Insert an immutable delivery row before any broker publication."""
        database = db or self._db()
        canonical = SignedJsonJobSerializer.canonical_envelope_bytes(body)
        digest = SignedJsonJobSerializer.canonical_envelope_sha256(canonical)
        job_id = str(payload["job_id"])
        db_job_id = self._bounded_int(
            payload.get("db_job_id"),
            minimum=1,
            maximum=9_223_372_036_854_775_807,
            field="db_job_id",
        )
        tenant_mode, tenant_id = self._tenant_scope(payload)
        now = pendulum.now("UTC")
        available_at = pendulum.from_timestamp(
            int(envelope["not_before"]),
            tz="UTC",
        )
        expires_at = pendulum.from_timestamp(
            int(envelope["expires_at"]),
            tz="UTC",
        )
        row = database.select_one(
            f"INSERT INTO {self.table} ("
            "job_id, db_job_id, replay_of, replay_requested_by, replay_reason, "
            "payload_sha256, signed_envelope, "
            "tenant_mode, tenant_id, "
            "queue, priority, status, attempts, lease_token, lease_expires_at, "
            "completed_at, terminal_reason, post_hooks_completed_at, "
            "expires_at, available_at, publish_status, "
            "publish_attempts, publish_retry_at, publish_lease_token, "
            "publish_lease_expires_at, published_at, last_publish_error, "
            "created_at, updated_at"
            ") VALUES ("
            "%s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, "
            "%s, %s, %s, 0, NULL, NULL, "
            "NULL, NULL, NULL, "
            "%s, %s, %s, 0, %s, NULL, NULL, NULL, NULL, %s, %s"
            ") ON CONFLICT (job_id) DO NOTHING RETURNING job_id",
            [
                job_id,
                db_job_id,
                replay_of,
                replay_requested_by,
                replay_reason,
                digest,
                canonical.decode("utf-8"),
                tenant_mode,
                tenant_id,
                payload["queue"],
                payload["priority"],
                self.STATUS_PENDING,
                expires_at,
                available_at,
                self.PUBLISH_PENDING,
                available_at,
                now,
                now,
            ],
        )
        if row is not None:
            return True

        existing = database.select_one(
            f"SELECT job_id, db_job_id, replay_of, replay_requested_by, "
            f"replay_reason, payload_sha256, queue, priority, "
            f"expires_at, available_at FROM {self.table} WHERE job_id = %s",
            [job_id],
        )
        if existing is None:
            raise QueueException("Queue delivery ledger insert was not persisted.")
        expected = {
            "payload_sha256": digest,
            "db_job_id": db_job_id,
            "replay_of": replay_of,
            "replay_requested_by": replay_requested_by,
            "replay_reason": replay_reason,
            "queue": payload["queue"],
            "priority": payload["priority"],
        }
        for field, value in expected.items():
            if self._row_value(existing, field) != value:
                raise QueueException(
                    f"Queue delivery id {job_id} conflicts on immutable {field}."
                )
        return False

    def publish_after_commit(self, job_id: str) -> None:
        """Wake the broker-independent relay after commit without doing I/O."""
        def _wake_hint() -> None:
            wake = getattr(self.driver, "wake_outbox_relay", None)
            if not callable(wake):
                return
            try:
                wake()
            except Exception as exc:
                Log.warning(
                    "Queue delivery %s relay wake hint failed; durable polling "
                    "remains authoritative: %s",
                    job_id,
                    exc,
                    category="cara.queue.delivery",
                )

        self._db().after_commit(_wake_hint)

    def replay_from_ledger(
        self,
        source_job_id: str,
        *,
        operator: str,
        reason: str,
    ) -> str:
        """Create an audited immutable replay without requiring a DLQ copy."""
        actor = self._bounded_text(operator, "operator", 200)
        audit_reason = self._safe_persisted_text(
            self._bounded_text(reason, "reason", 1000),
            maximum=1000,
        )
        database = self._db()
        with database.transaction():
            source = database.select_one(
                f"SELECT job_id, db_job_id, status, payload_sha256, "
                f"signed_envelope FROM {self.table} WHERE job_id = %s "
                "FOR UPDATE",
                [source_job_id],
            )
            if source is None:
                raise QueueException("Queue delivery replay source does not exist.")
            source_status = str(self._row_value(source, "status"))
            if source_status not in {
                self.STATUS_DEAD_LETTERED,
                self.STATUS_EXPIRED,
            }:
                raise QueueException(
                    "Only dead-lettered or expired ledger deliveries can be replayed."
                )

            existing_row = database.select_one(
                f"SELECT job_id, status, publish_status, expires_at "
                f"FROM {self.table} WHERE replay_of = %s",
                [source_job_id],
            )
            if existing_row is not None:
                existing = ReplayDelivery(
                    job_id=str(self._row_value(existing_row, "job_id")),
                    status=str(self._row_value(existing_row, "status")),
                    publish_status=str(
                        self._row_value(existing_row, "publish_status")
                    ),
                    expires_at=self._as_datetime(
                        self._row_value(existing_row, "expires_at")
                    ),
                )
                if existing.is_accepted():
                    return existing.job_id
                raise QueueException(
                    f"Replay child {existing.job_id} is terminal "
                    f"({existing.status}); replay that child delivery instead."
                )

            source_body = self._envelope_bytes(
                self._row_value(source, "signed_envelope")
            )
            source_envelope = SignedJsonJobSerializer.inspect_envelope(
                source_body,
                signing_keys=self.options.get("signing_keys", {}),
                clock_skew_seconds=int(
                    self.options.get("clock_skew_seconds", 30)
                ),
                max_age_seconds=int(
                    self.options.get(
                        "envelope_max_age_seconds",
                        SignedJsonJobSerializer.DEFAULT_MAX_AGE_SECONDS,
                    )
                ),
                allow_not_before=True,
                allow_expired=True,
            )
            source_payload = source_envelope["payload"]
            source_digest = SignedJsonJobSerializer.canonical_envelope_sha256(
                source_body
            )
            if (
                str(source_payload["job_id"]) != str(source_job_id)
                or source_digest != self._row_value(source, "payload_sha256")
                or source_payload.get("db_job_id")
                != self._row_value(source, "db_job_id")
            ):
                raise DeliveryEnvelopeMismatch(
                    "Ledger replay source does not match its signed envelope."
                )

            replay_job_id = str(uuid.uuid4())
            tracker = (
                self.application.make("JobTracker")
                if self.application is not None
                and self.application.has("JobTracker")
                else None
            )
            if tracker is None or getattr(tracker, "job_model", None) is None:
                raise QueueException(
                    "Queue replay requires a persistent JobTracker model."
                )
            descriptor = source_payload["job"]
            replay_db_job_id = tracker.create_job_record(
                job_name=str(descriptor["class"]),
                job_class=(
                    f"{descriptor['module']}.{descriptor['class']}"
                ),
                queue=str(source_payload["queue"]),
                execution_mode="queued",
                payload=dict(descriptor["kwargs"]),
                metadata={
                    "job_id": replay_job_id,
                    "driver": "amqp",
                    "replay_of": str(source_job_id),
                    "replay_requested_by": actor,
                },
            )
            if (
                isinstance(replay_db_job_id, bool)
                or not isinstance(replay_db_job_id, int)
                or replay_db_job_id <= 0
            ):
                raise QueueException(
                    "JobTracker did not persist a positive replay db_job_id."
                )
            replay_body = SignedJsonJobSerializer.serialize_replay(
                source_payload,
                new_job_id=replay_job_id,
                new_db_job_id=replay_db_job_id,
                signing_key_id=self.options.get("signing_key_id", ""),
                signing_keys=self.options.get("signing_keys", {}),
                ttl_seconds=int(
                    self.options.get(
                        "envelope_ttl_seconds",
                        SignedJsonJobSerializer.DEFAULT_TTL_SECONDS,
                    )
                ),
                max_age_seconds=int(
                    self.options.get(
                        "envelope_max_age_seconds",
                        SignedJsonJobSerializer.DEFAULT_MAX_AGE_SECONDS,
                    )
                ),
            )
            replay_envelope = SignedJsonJobSerializer.inspect_envelope(
                replay_body,
                signing_keys=self.options.get("signing_keys", {}),
                clock_skew_seconds=int(
                    self.options.get("clock_skew_seconds", 30)
                ),
                max_age_seconds=int(
                    self.options.get(
                        "envelope_max_age_seconds",
                        SignedJsonJobSerializer.DEFAULT_MAX_AGE_SECONDS,
                    )
                ),
            )
            self.register(
                body=replay_body,
                payload=replay_envelope["payload"],
                envelope=replay_envelope,
                replay_of=str(source_job_id),
                replay_requested_by=actor,
                replay_reason=audit_reason,
                db=database,
            )
            self.publish_after_commit(replay_job_id)
            return replay_job_id

    def publish_one(self, job_id: str) -> bool | None:
        claimed = self._claim_publish(job_id)
        if claimed is None:
            return None
        row, token = claimed
        try:
            settled = self._publish_claimed(row, token)
        except DeliveryEnvelopeExpired as exc:
            self._expire_publish(job_id, token, str(exc))
            return True
        except DeliveryEnvelopeMismatch as exc:
            self._quarantine_publish(job_id, token, str(exc))
            raise
        except Exception as exc:
            released = self._release_publish(
                job_id,
                token,
                int(self._row_value(row, "publish_attempts") or 0),
                str(exc),
            )
            if not released:
                raise DeliveryLeaseLost(
                    f"Queue delivery {job_id} lost its publish retry lease."
                ) from exc
            raise
        return settled

    def _claim_publish(self, job_id: str) -> tuple[Any, str] | None:
        """Lease one publication immediately before its broker I/O."""
        token = uuid.uuid4().hex
        now = pendulum.now("UTC")
        row = self._db().select_one(
            f"UPDATE {self.table} SET publish_status = %s, "
            "publish_lease_token = %s, publish_lease_expires_at = %s, "
            "updated_at = %s WHERE job_id = %s AND status = %s "
            "AND available_at <= %s AND publish_retry_at <= %s "
            "AND expires_at > %s AND (publish_status = %s OR "
            "(publish_status = %s AND (publish_lease_expires_at IS NULL OR "
            "publish_lease_expires_at <= %s))) "
            "RETURNING job_id, db_job_id, payload_sha256, signed_envelope, "
            "tenant_mode, tenant_id, queue, priority, publish_attempts",
            [
                self.PUBLISH_PROCESSING,
                token,
                now.add(seconds=self.publish_lease_seconds),
                now,
                job_id,
                self.STATUS_PENDING,
                now,
                now,
                now,
                self.PUBLISH_PENDING,
                self.PUBLISH_PROCESSING,
                now,
            ],
        )
        if row is None:
            self._expire_job_if_due(job_id, now)
            return None
        return row, token

    def _claim_next_publish(self) -> tuple[Any, str] | None:
        """Atomically lease the highest effective-priority due row.

        Each wait interval promotes a row by one tier. A bounded per-queue
        broker window keeps the backlog in this aging ledger instead of
        preloading RabbitMQ's strict-priority queue, where sustained critical
        traffic could otherwise starve low priority indefinitely. With the
        default five-minute interval, a low row reaches the critical lane
        after fifteen minutes.

        Relay replicas serialize this short reservation statement with a
        PostgreSQL advisory transaction lock. The publication itself remains
        outside the lock; ``publish_status=processing`` is the durable window
        reservation, so replicas cannot overfill RabbitMQ concurrently.
        """
        token = uuid.uuid4().hex
        now = pendulum.now("UTC")
        database = self._db()
        with database.transaction():
            database.select_one(
                "SELECT pg_advisory_xact_lock(%s) AS locked",
                [self._PUBLISH_CLAIM_ADVISORY_LOCK],
            )
            row = database.select_one(
                "WITH settings AS (SELECT %s::timestamptz AS now, "
                "%s::numeric AS aging_seconds, %s::integer AS broker_window), "
                "priority_lanes(priority, base_rank) AS (VALUES "
                "('critical', 0), ('high', 1), ('default', 2), ('low', 3)), "
                "eligible_queues AS (SELECT candidate.queue "
                "FROM unnest(%s::varchar[]) AS candidate(queue) "
                "CROSS JOIN settings WHERE (SELECT COUNT(*) "
                f"FROM {self.table} AS outstanding "
                "WHERE outstanding.queue = candidate.queue "
                "AND outstanding.status IN ('pending', 'processing') "
                "AND (outstanding.publish_status = 'published' OR "
                "(outstanding.publish_status = 'processing' AND "
                "outstanding.publish_lease_expires_at > settings.now))) "
                "< settings.broker_window), "
                "heads AS (SELECT head.job_id, head.available_at, "
                "head.created_at, priority_lanes.base_rank "
                "FROM eligible_queues CROSS JOIN settings "
                "CROSS JOIN priority_lanes CROSS JOIN LATERAL ("
                "SELECT delivery.job_id, delivery.available_at, "
                f"delivery.created_at FROM {self.table} AS delivery "
                "WHERE delivery.queue = eligible_queues.queue "
                "AND delivery.priority = priority_lanes.priority "
                "AND delivery.status = 'pending' "
                "AND delivery.available_at <= settings.now "
                "AND delivery.publish_retry_at <= settings.now "
                "AND delivery.expires_at > settings.now "
                "AND (delivery.publish_status = 'pending' OR "
                "(delivery.publish_status = 'processing' AND "
                "(delivery.publish_lease_expires_at IS NULL OR "
                "delivery.publish_lease_expires_at <= settings.now))) "
                "ORDER BY delivery.available_at, delivery.created_at "
                "FOR UPDATE OF delivery SKIP LOCKED LIMIT 1"
                ") AS head), "
                "candidate AS (SELECT heads.job_id "
                "FROM heads CROSS JOIN settings "
                "ORDER BY GREATEST(heads.base_rank - FLOOR(EXTRACT(EPOCH FROM "
                "(settings.now - heads.available_at)) / "
                "settings.aging_seconds), 0), heads.available_at, "
                "heads.created_at LIMIT 1) "
                f"UPDATE {self.table} AS delivery "
                "SET publish_status = 'processing', "
                "publish_lease_token = %s, publish_lease_expires_at = %s, "
                "updated_at = %s FROM candidate "
                "WHERE delivery.job_id = candidate.job_id "
                "RETURNING delivery.job_id, delivery.db_job_id, "
                "delivery.payload_sha256, delivery.signed_envelope, "
                "delivery.tenant_mode, delivery.tenant_id, delivery.queue, "
                "delivery.priority, delivery.publish_attempts",
                [
                    now,
                    self.priority_aging_seconds,
                    self.broker_window_per_queue,
                    list(self.canonical_queues),
                    token,
                    now.add(seconds=self.publish_lease_seconds),
                    now,
                ],
            )
        if row is None:
            return None
        return row, token

    def publish_due(self) -> dict[str, int]:
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
                    (
                        ""
                        if released
                        else " and its retry lease was concurrently lost"
                    ),
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

    def _publish_claimed(self, row: Any, token: str) -> bool:
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
            f"SELECT publish_status, published_at FROM {self.table} "
            "WHERE job_id = %s",
            [str(payload["job_id"])],
        )
        return (
            self._row_value(current, "publish_status")
            == self.PUBLISH_PUBLISHED
            and self._row_value(current, "published_at") is not None
        )

    def _release_publish(
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

    def _quarantine_publish(self, job_id: str, token: str, error: str) -> None:
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
                    self._safe_error(
                        f"publish_envelope_invalid:{safe_error}"
                    ),
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
                raise QueueException(
                    f"Queue delivery {job_id} lost its quarantine lease."
                )
            self._mark_tracker_failed(
                database,
                int(self._row_value(row, "db_job_id")),
                now,
            )

    def claim_execution(
        self,
        *,
        body: bytes | str | dict[str, Any],
        payload: dict[str, Any],
    ) -> DeliveryClaim:
        """Atomically validate identity and lease one delivery for execution."""
        digest = SignedJsonJobSerializer.canonical_envelope_sha256(body)
        job_id = str(payload["job_id"])
        timeout_seconds = self._bounded_int(
            payload.get("timeout_seconds"),
            minimum=1,
            maximum=self.execution_lease_seconds
            - self.execution_lease_grace_seconds,
            field="timeout_seconds",
        )
        lease_seconds = timeout_seconds + self.execution_lease_grace_seconds
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
                or self._row_value(row, "tenant_mode")
                != payload.get("_tenant_mode")
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
                        f"Queue delivery {job_id} early-publication recovery "
                        "was lost."
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
                lease_expiry = self._as_datetime(
                    self._row_value(row, "lease_expires_at")
                )
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
                    raise QueueException(
                        "Queue delivery expiry settlement was lost."
                    )
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
        return DeliveryClaim("claimed", token, reclaimed=reclaimed)

    def _reconcile_broker_receipt(
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

    def complete(self, job_id: str, lease_token: str) -> None:
        self._settle_with_retry(job_id, lease_token, self.STATUS_COMPLETED)

    def complete_with_tracker(
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

    def dead_letter(
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

    def dead_letter_with_tracker(
        self,
        job_id: str,
        lease_token: str,
        *,
        db_job_id: int,
        reason: str,
    ) -> None:
        self._settle_execution_with_tracker(
            job_id,
            lease_token,
            db_job_id=db_job_id,
            status=self.STATUS_DEAD_LETTERED,
            tracker_status="failed",
            reason=reason,
        )

    def _settle_execution_with_tracker(
        self,
        job_id: str,
        lease_token: str,
        *,
        db_job_id: int,
        status: str,
        tracker_status: str,
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
                database = self._db()
                with database.transaction():
                    self._settle(
                        job_id,
                        lease_token,
                        status,
                        db=database,
                        reason=reason,
                    )
                    self._set_tracker_status(
                        database,
                        db_job_id,
                        tracker_status,
                    )
                return
            except DeliveryLeaseLost:
                raise
            except Exception as exc:
                last_error = exc
                if attempt <= len(self._SETTLEMENT_BACKOFF_SECONDS):
                    Log.warning(
                        "Queue delivery %s atomic terminal settlement attempt "
                        "%s failed; retrying: %s",
                        job_id,
                        attempt,
                        exc,
                        category="cara.queue.delivery",
                    )
        raise QueueException(
            f"Queue delivery {job_id} atomic terminal settlement remained "
            "unavailable."
        ) from last_error

    def reconcile_terminal_tracker(
        self,
        job_id: str,
        *,
        db_job_id: int,
        delivery_status: str,
    ) -> None:
        tracker_status = (
            "completed"
            if delivery_status == self.STATUS_COMPLETED
            else "failed"
        )
        database = self._db()
        with database.transaction():
            row = database.select_one(
                f"SELECT status, db_job_id FROM {self.table} "
                "WHERE job_id = %s FOR UPDATE",
                [job_id],
            )
            if (
                row is None
                or self._row_value(row, "status") != delivery_status
                or self._row_value(row, "db_job_id") != db_job_id
            ):
                raise QueueException(
                    f"Queue delivery {job_id} terminal tracker recovery "
                    "does not match the ledger."
                )
            self._set_tracker_status(
                database,
                db_job_id,
                tracker_status,
            )

    def mark_retry_scheduled(
        self,
        job_id: str,
        lease_token: str,
        *,
        db: Any | None = None,
    ) -> None:
        database = db or self._db()
        self._settle(
            job_id,
            lease_token,
            self.STATUS_RETRY_SCHEDULED,
            db=database,
        )
        now = pendulum.now("UTC")
        affected = database.statement(
            f"UPDATE {self.table} SET post_hooks_completed_at = COALESCE("
            "post_hooks_completed_at, %s), updated_at = %s "
            "WHERE job_id = %s AND status = %s",
            [now, now, job_id, self.STATUS_RETRY_SCHEDULED],
        )
        if not self._affected(affected):
            raise QueueException(
                f"Queue delivery {job_id} retry hook bypass was not persisted."
            )

    def abandon_execution(self, job_id: str, lease_token: str) -> None:
        """Release one interrupted execution lease before broker redelivery."""
        now = pendulum.now("UTC")
        affected = self._db().statement(
            f"UPDATE {self.table} SET status = %s, lease_token = NULL, "
            "lease_expires_at = NULL, updated_at = %s "
            "WHERE job_id = %s AND status = %s AND lease_token = %s",
            [
                self.STATUS_PENDING,
                now,
                job_id,
                self.STATUS_PROCESSING,
                lease_token,
            ],
        )
        if not self._affected(affected):
            row = self._db().select_one(
                f"SELECT status, lease_token FROM {self.table} "
                "WHERE job_id = %s",
                [job_id],
            )
            if (
                self._row_value(row, "status") == self.STATUS_PENDING
                and self._row_value(row, "lease_token") is None
            ):
                return
            raise DeliveryLeaseLost(
                f"Queue delivery {job_id} lost its interrupted execution lease."
            )

    def claim_terminal_hooks(self, job_id: str) -> TerminalHookClaim:
        """CAS-claim one durable terminal-hook outbox row."""
        now = pendulum.now("UTC")
        database = self._db()
        with database.transaction():
            row = database.select_one(
                f"SELECT job_id, status, terminal_reason, signed_envelope, "
                "post_hooks_completed_at, post_hooks_lease_token, "
                "post_hooks_lease_expires_at, post_hooks_attempts, "
                f"post_hooks_quarantined_at FROM {self.table} "
                "WHERE job_id = %s FOR UPDATE",
                [job_id],
            )
            if row is None:
                raise QueueException(
                    f"Queue delivery {job_id} is absent during terminal hooks."
                )
            status = str(self._row_value(row, "status"))
            if status not in self.HOOK_TERMINAL_STATUSES:
                raise QueueException(
                    f"Queue delivery {job_id} is not hook-eligible."
                )
            if self._row_value(row, "post_hooks_completed_at") is not None:
                return TerminalHookClaim("completed", status=status)
            if self._row_value(row, "post_hooks_quarantined_at") is not None:
                return TerminalHookClaim("quarantined", status=status)

            lease_expiry = self._as_datetime(
                self._row_value(row, "post_hooks_lease_expires_at")
            )
            if (
                self._row_value(row, "post_hooks_lease_token") is not None
                and lease_expiry is not None
                and lease_expiry > now
            ):
                return TerminalHookClaim("live_lease", status=status)

            token = uuid.uuid4().hex
            affected = database.statement(
                f"UPDATE {self.table} SET post_hooks_lease_token = %s, "
                "post_hooks_lease_expires_at = %s, "
                "post_hooks_last_error = NULL, updated_at = %s "
                "WHERE job_id = %s AND post_hooks_completed_at IS NULL "
                "AND post_hooks_quarantined_at IS NULL "
                "AND (post_hooks_lease_token IS NULL OR "
                "post_hooks_lease_expires_at IS NULL OR "
                "post_hooks_lease_expires_at <= %s)",
                [
                    token,
                    now.add(seconds=self.POST_HOOK_LEASE_SECONDS),
                    now,
                    job_id,
                    now,
                ],
            )
            if not self._affected(affected):
                return TerminalHookClaim("live_lease", status=status)
            return TerminalHookClaim(
                "claimed",
                lease_token=token,
                signed_envelope=self._envelope_bytes(
                    self._row_value(row, "signed_envelope")
                ),
                status=status,
                terminal_reason=self._row_value(row, "terminal_reason"),
            )

    def process_terminal_hooks(
        self,
        job_id: str,
        *,
        instance: Any | None = None,
        message: dict[str, Any] | None = None,
        error: Exception | None = None,
    ) -> bool:
        """Run one claimed hook outbox row with a stable idempotency key."""
        claim = self.claim_terminal_hooks(job_id)
        if claim.outcome == "completed":
            return True
        if claim.outcome == "live_lease":
            return False
        if claim.outcome == "quarantined":
            raise QueueException(
                f"Queue delivery {job_id} terminal hooks are quarantined."
            )
        if claim.lease_token is None or claim.signed_envelope is None:
            raise QueueException("Terminal hook claim is missing its lease data.")

        try:
            payload = message
            job_instance = instance
            if payload is None or job_instance is None:
                envelope = SignedJsonJobSerializer.inspect_envelope(
                    claim.signed_envelope,
                    signing_keys=self.options.get("signing_keys", {}),
                    clock_skew_seconds=int(
                        self.options.get("clock_skew_seconds", 30)
                    ),
                    max_age_seconds=int(
                        self.options.get(
                            "envelope_max_age_seconds",
                            SignedJsonJobSerializer.DEFAULT_MAX_AGE_SECONDS,
                        )
                    ),
                    allow_not_before=True,
                    allow_expired=True,
                )
                payload = SignedJsonJobSerializer.deserialize_verified(
                    envelope["payload"],
                    allowed_prefixes=self.options.get("allowed_job_prefixes"),
                )
                from cara.queues.JobInstantiation import instantiate_job

                job_instance = instantiate_job(
                    self.application,
                    payload.get("obj"),
                    payload.get("args", ()),
                    payload.get("init_kwargs", {}),
                )
            if payload is None or job_instance is None:
                raise QueueException(
                    f"Queue delivery {job_id} terminal hook payload is invalid."
                )

            from cara.context import Tenancy

            mode = payload.get("_tenant_mode")
            if mode == "central":
                tenant_scope = Tenancy.central()
            elif mode == "tenant" and payload.get("_tenant") is not None:
                tenant_scope = Tenancy.as_tenant(payload["_tenant"])
            else:
                raise QueueException(
                    "Terminal hooks require verified tenant mode."
                )

            with tenant_scope:
                from cara.queues.contracts import UniqueJob

                async def _run_hooks() -> None:
                    if claim.status in {
                        self.STATUS_DEAD_LETTERED,
                        self.STATUS_EXPIRED,
                    } and hasattr(job_instance, "failed"):
                        failed_hook = job_instance.failed
                        if not inspect.iscoroutinefunction(failed_hook):
                            raise QueueException(
                                f"{type(job_instance).__name__}.failed must "
                                "be async and idempotency-aware."
                            )
                        hook_error = error or RuntimeError(
                            claim.terminal_reason or str(claim.status)
                        )
                        await failed_hook(
                            payload,
                            str(hook_error),
                            idempotency_key=(
                                f"queue-delivery:{job_id}:failed"
                            ),
                        )

                    if isinstance(job_instance, UniqueJob):
                        await asyncio.to_thread(
                            UniqueJob.release_unique_lock_strict,
                            job_instance.unique_id(),
                        )

                asyncio.run(
                    asyncio.wait_for(
                        _run_hooks(),
                        timeout=self.hook_timeout_seconds,
                    )
                )
        except Exception as exc:
            self.defer_terminal_hooks(
                job_id,
                claim.lease_token,
                error=str(exc),
            )
            raise

        self.complete_terminal_hooks(job_id, claim.lease_token)
        return True

    def complete_terminal_hooks(self, job_id: str, lease_token: str) -> None:
        now = pendulum.now("UTC")
        affected = self._db().statement(
            f"UPDATE {self.table} SET post_hooks_completed_at = %s, "
            "post_hooks_lease_token = NULL, "
            "post_hooks_lease_expires_at = NULL, "
            "post_hooks_last_error = NULL, updated_at = %s "
            "WHERE job_id = %s AND post_hooks_completed_at IS NULL "
            "AND post_hooks_lease_token = %s",
            [now, now, job_id, lease_token],
        )
        if not self._affected(affected):
            row = self._db().select_one(
                f"SELECT post_hooks_completed_at FROM {self.table} "
                "WHERE job_id = %s",
                [job_id],
            )
            if self._row_value(row, "post_hooks_completed_at") is not None:
                return
            raise DeliveryLeaseLost(
                f"Queue delivery {job_id} lost its terminal-hook lease."
            )

    def defer_terminal_hooks(
        self,
        job_id: str,
        lease_token: str,
        *,
        error: str,
    ) -> None:
        self._defer_terminal_hook(
            job_id,
            error=error,
            expected_lease_token=lease_token,
            skip_if_already_deferred=False,
        )

    def defer_terminal_hook_process_failure(
        self,
        job_id: str,
        *,
        error: str,
    ) -> str:
        """Back off a hook child that was killed or exited abnormally."""
        return self._defer_terminal_hook(
            job_id,
            error=error,
            expected_lease_token=None,
            skip_if_already_deferred=True,
        )

    def _defer_terminal_hook(
        self,
        job_id: str,
        *,
        error: str,
        expected_lease_token: str | None,
        skip_if_already_deferred: bool,
    ) -> str:
        now = pendulum.now("UTC")
        database = self._db()
        with database.transaction():
            row = database.select_one(
                f"SELECT post_hooks_completed_at, post_hooks_lease_token, "
                "post_hooks_attempts, post_hooks_quarantined_at, "
                "post_hooks_last_error "
                f"FROM {self.table} WHERE job_id = %s FOR UPDATE",
                [job_id],
            )
            if row is None:
                raise QueueException(
                    f"Queue delivery {job_id} is absent during hook deferral."
                )
            if self._row_value(row, "post_hooks_completed_at") is not None:
                return "completed"
            if self._row_value(row, "post_hooks_quarantined_at") is not None:
                return "quarantined"
            current_token = self._row_value(row, "post_hooks_lease_token")
            if current_token is None or (
                expected_lease_token is not None
                and current_token != expected_lease_token
            ):
                raise DeliveryLeaseLost(
                    f"Queue delivery {job_id} lost its terminal-hook retry lease."
                )
            if (
                skip_if_already_deferred
                and self._row_value(row, "post_hooks_last_error") is not None
            ):
                return "already_deferred"

            attempts = int(
                self._row_value(row, "post_hooks_attempts") or 0
            ) + 1
            safe_error = self._safe_error(error)
            if attempts >= self.hook_max_attempts:
                affected = database.statement(
                    f"UPDATE {self.table} SET post_hooks_attempts = %s, "
                    "post_hooks_quarantined_at = %s, "
                    "post_hooks_lease_token = NULL, "
                    "post_hooks_lease_expires_at = NULL, "
                    "post_hooks_last_error = %s, updated_at = %s "
                    "WHERE job_id = %s AND post_hooks_completed_at IS NULL "
                    "AND post_hooks_lease_token = %s",
                    [
                        attempts,
                        now,
                        safe_error,
                        now,
                        job_id,
                        current_token,
                    ],
                )
                outcome = "quarantined"
            else:
                index = min(
                    attempts - 1,
                    len(self._HOOK_BACKOFF_SECONDS) - 1,
                )
                affected = database.statement(
                    f"UPDATE {self.table} SET post_hooks_attempts = %s, "
                    "post_hooks_lease_expires_at = %s, "
                    "post_hooks_last_error = %s, updated_at = %s "
                    "WHERE job_id = %s AND post_hooks_completed_at IS NULL "
                    "AND post_hooks_lease_token = %s",
                    [
                        attempts,
                        now.add(seconds=self._HOOK_BACKOFF_SECONDS[index]),
                        safe_error,
                        now,
                        job_id,
                        current_token,
                    ],
                )
                outcome = "deferred"
            if not self._affected(affected):
                raise DeliveryLeaseLost(
                    f"Queue delivery {job_id} lost its terminal-hook retry lease."
                )
            return outcome

    def due_terminal_hook_ids(self, batch_size: int | None = None) -> list[str]:
        """List a bounded batch for isolated hook subprocess execution."""
        limit = self._bounded_int(
            batch_size or self.claim_batch,
            minimum=1,
            maximum=1000,
            field="delivery_hook_batch",
        )
        now = pendulum.now("UTC")
        rows = self._db().select(
            f"SELECT job_id FROM {self.table} "
            "WHERE status = ANY(%s) AND post_hooks_completed_at IS NULL "
            "AND post_hooks_quarantined_at IS NULL "
            "AND (post_hooks_lease_token IS NULL OR "
            "post_hooks_lease_expires_at IS NULL OR "
            "post_hooks_lease_expires_at <= %s) "
            "ORDER BY completed_at, created_at LIMIT %s",
            [list(self.HOOK_TERMINAL_STATUSES), now, limit],
        ) or []
        return [str(self._row_value(row, "job_id")) for row in rows]

    def retry_quarantined_terminal_hooks(
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
                raise QueueException(
                    f"Queue delivery {job_id} does not exist."
                )
            if str(self._row_value(row, "status")) not in (
                self.HOOK_TERMINAL_STATUSES
            ):
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
            if str(
                self._row_value(row, "terminal_reason") or ""
            ).startswith("publish_envelope_invalid:"):
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
                    self._safe_error(
                        self._row_value(row, "post_hooks_last_error") or ""
                    ),
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

    def _settle_with_retry(
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

    def _settle(
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

    def expire_due(self, batch_size: int = 5000) -> int:
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
            rows = database.select(
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
            ) or []
            for row in rows:
                job_id = str(self._row_value(row, "job_id"))
                db_job_id = int(self._row_value(row, "db_job_id"))
                tracker_status = str(self._row_value(row, "tracker_status"))
                recovered_completion = tracker_status in {
                    "completed",
                    "success",
                }
                target = (
                    self.STATUS_COMPLETED
                    if recovered_completion
                    else self.STATUS_EXPIRED
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

    def recover_stale_executions(
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
            rows = database.select(
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
            ) or []
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
                        "processed_at = NULL, completed_at = NULL, "
                        "finished_at = NULL, updated_at = %s WHERE id = %s "
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
                    reason = (
                        "unsupported_tracker_status_after_worker_crash"
                    )
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
                        f"Queue delivery {job_id} lost its terminal crash "
                        "recovery lease."
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

    def prune_terminal(self, batch_size: int = 5000) -> int:
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
        except (TypeError, ValueError):
            return 0

    def execution_timeout_for(self, job_or_class: Any) -> int:
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
            or timeout + self.execution_lease_grace_seconds
            > self.execution_lease_seconds
        ):
            raise QueueException(
                f"{job_class.__name__}.timeout must be positive and timeout + "
                f"lease grace ({self.execution_lease_grace_seconds}s) must not "
                f"exceed the delivery execution lease "
                f"({self.execution_lease_seconds}s)."
            )
        return timeout

    def _expire_job_if_due(
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

    def _expire_publish(self, job_id: str, token: str, reason: str) -> None:
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

    def backlog_metrics(self) -> dict[str, int | float]:
        row = self._db().select_one(
            f"SELECT COUNT(*) AS count, COALESCE(EXTRACT(EPOCH FROM "
            f"(NOW() - MIN(available_at))), 0) AS age FROM {self.table} "
            "WHERE status = %s AND publish_status != %s AND available_at <= NOW()",
            [self.STATUS_PENDING, self.PUBLISH_PUBLISHED],
        )
        return {
            "count": int(self._row_value(row, "count") or 0),
            "age": max(float(self._row_value(row, "age") or 0), 0.0),
        }

    def delivery_stats(
        self,
        queue_name: str,
        *,
        recent_hours: int,
    ) -> dict[str, Any]:
        """Return bounded operator stats from the canonical delivery ledger."""
        if queue_name not in self.canonical_queues:
            valid = ", ".join(self.canonical_queues)
            raise QueueException(
                f"Unknown canonical queue {queue_name!r}. Valid: {valid}."
            )
        hours = self._bounded_int(
            recent_hours,
            minimum=1,
            maximum=8760,
            field="recent_hours",
        )
        database = self._db()
        active = database.select_one(
            f"SELECT COUNT(*) AS active_total, "
            "COUNT(*) FILTER (WHERE status = %s) AS pending, "
            "COUNT(*) FILTER (WHERE status = %s) AS processing, "
            "COUNT(*) FILTER (WHERE status = %s AND publish_status != %s "
            "AND available_at <= NOW()) AS due_unpublished, "
            "COALESCE(EXTRACT(EPOCH FROM (NOW() - "
            "(MIN(available_at) FILTER (WHERE status = %s "
            "AND publish_status != %s AND available_at <= NOW())))), 0) "
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
                float(
                    self._row_value(active, "oldest_due_age")
                    or 0
                ),
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

    def delivery_metrics(self) -> dict[str, Any]:
        """Return one bounded aggregate snapshot for relay-owned metrics."""
        priority_columns: list[str] = []
        priority_params: list[Any] = []
        for priority in self._PRIORITY_RANKS:
            alias = priority.replace("-", "_")
            due_filter = (
                "status = %s AND publish_status != %s "
                "AND available_at <= NOW() AND priority = %s"
            )
            priority_columns.extend(
                (
                    f"COUNT(*) FILTER (WHERE {due_filter}) "
                    f"AS priority_{alias}_pending",
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
            "(MIN(available_at) FILTER (WHERE status = %s "
            "AND publish_status != %s AND available_at <= NOW())))), 0) "
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
                    "latency_budget": (
                        self._PRIORITY_RANKS[priority] + 1
                    )
                    * self.priority_aging_seconds,
                }
                for priority in self._PRIORITY_RANKS
            },
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

    def verify_schema(self) -> None:
        """Fail readiness if the durable queue schema/permissions are absent."""
        self._db().select_one(
            f"SELECT job_id, db_job_id, tenant_mode, tenant_id, status, "
            "publish_status, post_hooks_attempts, "
            "post_hooks_completed_at, post_hooks_lease_token, "
            "post_hooks_lease_expires_at, post_hooks_quarantined_at "
            f"FROM {self.table} LIMIT 0"
        )
        self._db().select_one(
            "SELECT id, status FROM job LIMIT 0"
        )
        self._db().select_one(
            "SELECT id, job_id, requested_by, requested_at "
            "FROM queue_job_delivery_hook_retry_audit LIMIT 0"
        )

    @staticmethod
    def _mark_tracker_failed(
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

    @staticmethod
    def _set_tracker_status(
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
            raise QueueException(
                f"Unsupported atomic tracker terminal status: {status}."
            )
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
            f"Tracked queue job {db_job_id} contradicts terminal "
            f"status {status!r}."
        )

    def _db(self) -> Any:
        if self.application is None or not self.application.has("DB"):
            raise QueueException(
                "Durable AMQP delivery requires the application DB binding."
            )
        return self.application.make("DB")

    @staticmethod
    def _tenant_scope(payload: Mapping[str, Any]) -> tuple[str, int | None]:
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
        raise QueueException(
            "Queue delivery requires a canonical signed tenant scope."
        )

    @staticmethod
    def _safe_error(error: Any) -> str:
        return QueueJobDeliveryStore._safe_persisted_text(
            error,
            maximum=2000,
        )

    @staticmethod
    def _safe_persisted_text(value: Any, *, maximum: int) -> str:
        from cara.support import redact_log_secrets

        return redact_log_secrets(value).replace("\x00", "")[:maximum]

    @staticmethod
    def _envelope_bytes(value: Any) -> bytes:
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

    @staticmethod
    def _row_value(row: Any, key: str) -> Any:
        if row is None:
            return None
        if isinstance(row, dict):
            return row.get(key)
        return getattr(row, key, None)

    @staticmethod
    def _affected(value: Any) -> bool:
        try:
            return int(value or 0) > 0
        except (TypeError, ValueError):
            return bool(value)

    @staticmethod
    def _as_datetime(value: Any) -> pendulum.DateTime | None:
        if value is None:
            return None
        if isinstance(value, pendulum.DateTime):
            return value.in_timezone("UTC")
        if isinstance(value, str):
            return pendulum.parse(value).in_timezone("UTC")
        if isinstance(value, datetime):
            return pendulum.instance(value).in_timezone("UTC")
        return pendulum.parse(str(value)).in_timezone("UTC")

    @staticmethod
    def _bounded_int(value: Any, *, minimum: int, maximum: int, field: str) -> int:
        if isinstance(value, bool):
            raise QueueException(f"{field} must be an integer.")
        try:
            parsed = int(value)
        except (TypeError, ValueError) as exc:
            raise QueueException(f"{field} must be an integer.") from exc
        if not minimum <= parsed <= maximum:
            raise QueueException(f"{field} must be between {minimum} and {maximum}.")
        return parsed

    @staticmethod
    def _bounded_text(value: Any, field: str, maximum: int) -> str:
        if not isinstance(value, str) or not value.strip():
            raise QueueException(f"{field} must be a non-empty string.")
        normalized = value.strip()
        if len(normalized) > maximum:
            raise QueueException(f"{field} exceeds {maximum} characters.")
        return normalized
