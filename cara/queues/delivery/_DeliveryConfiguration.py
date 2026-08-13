"""Delivery registration and publish settlement composed into the store."""

from __future__ import annotations

import uuid
from typing import Any

import pendulum

from cara.exceptions import QueueException
from cara.facades import Log
from cara.queues.serializers import SignedJsonJobSerializer

from .DeliveryEnvelopeExpired import DeliveryEnvelopeExpired
from .DeliveryEnvelopeMismatch import DeliveryEnvelopeMismatch
from .DeliveryLeaseLost import DeliveryLeaseLost
from .ReplayDelivery import ReplayDelivery
from .UniqueDeliveryConflict import UniqueDeliveryConflict

QueueJobDeliveryStore: type


def _bind_store(store_type: type) -> None:
    global QueueJobDeliveryStore
    QueueJobDeliveryStore = store_type


def _delivery_register(
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
    self._require_ledger_schema(database)
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
    unique_key = payload.get("unique_key")
    if unique_key is not None:
        unique_key = self._bounded_text(
            unique_key,
            "unique_key",
            500,
        )
    now = pendulum.now("UTC")
    available_at = pendulum.from_timestamp(
        int(envelope["not_before"]),
        tz="UTC",
    )
    expires_at = pendulum.from_timestamp(
        int(envelope["expires_at"]),
        tz="UTC",
    )
    insert_sql = (
        f"INSERT INTO {self.table} ("
        "job_id, db_job_id, replay_of, replay_requested_by, replay_reason, "
        "payload_sha256, signed_envelope, "
        "tenant_mode, tenant_id, unique_key, "
        "queue, priority, status, attempts, lease_token, lease_expires_at, "
        "completed_at, terminal_reason, post_hooks_completed_at, "
        "expires_at, available_at, publish_status, "
        "publish_attempts, publish_retry_at, publish_lease_token, "
        "publish_lease_expires_at, published_at, last_publish_error, "
        "created_at, updated_at"
        ") VALUES ("
        "%s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, "
        "%s, %s, %s, %s, 0, NULL, NULL, "
        "NULL, NULL, NULL, "
        "%s, %s, %s, 0, %s, NULL, NULL, NULL, NULL, %s, %s"
        ") ON CONFLICT DO NOTHING RETURNING job_id"
    )
    insert_bindings = [
        job_id,
        db_job_id,
        replay_of,
        replay_requested_by,
        replay_reason,
        digest,
        canonical.decode("utf-8"),
        tenant_mode,
        tenant_id,
        unique_key,
        payload["queue"],
        payload["priority"],
        self.STATUS_PENDING,
        expires_at,
        available_at,
        self.PUBLISH_PENDING,
        available_at,
        now,
        now,
    ]
    expected = {
        "payload_sha256": digest,
        "db_job_id": db_job_id,
        "replay_of": replay_of,
        "replay_requested_by": replay_requested_by,
        "replay_reason": replay_reason,
        "queue": payload["queue"],
        "priority": payload["priority"],
    }
    for attempt in range(3):
        row = database.select_one(insert_sql, insert_bindings)
        if row is not None:
            return True

        existing = database.select_one(
            f"SELECT job_id, db_job_id, replay_of, replay_requested_by, "
            f"replay_reason, payload_sha256, queue, priority, "
            f"expires_at, available_at FROM {self.table} WHERE job_id = %s",
            [job_id],
        )
        if existing is not None:
            for field, value in expected.items():
                if self._row_value(existing, field) != value:
                    raise QueueException(
                        f"Queue delivery id {job_id} conflicts on immutable {field}."
                    )
            return False

        if unique_key is not None:
            owner = database.select_one(
                f"SELECT job_id FROM {self.table} WHERE tenant_mode = %s "
                "AND tenant_id IS NOT DISTINCT FROM %s AND unique_key = %s "
                "AND status IN (%s, %s) ORDER BY created_at ASC LIMIT 1",
                [
                    tenant_mode,
                    tenant_id,
                    unique_key,
                    self.STATUS_PENDING,
                    self.STATUS_PROCESSING,
                ],
            )
            if owner is not None:
                raise UniqueDeliveryConflict(str(self._row_value(owner, "job_id")))
            if attempt < 2:
                continue
        break
    raise QueueException("Queue delivery ledger insert was not persisted.")


def _delivery_publish_after_commit(self, job_id: str) -> None:
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


def _delivery_replay_from_ledger(
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
                publish_status=str(self._row_value(existing_row, "publish_status")),
                expires_at=self._as_datetime(self._row_value(existing_row, "expires_at")),
            )
            if existing.is_accepted():
                return existing.job_id
            raise QueueException(
                f"Replay child {existing.job_id} is terminal "
                f"({existing.status}); replay that child delivery instead."
            )

        source_body = self._envelope_bytes(self._row_value(source, "signed_envelope"))
        source_envelope = SignedJsonJobSerializer.inspect_envelope(
            source_body,
            signing_keys=self.options.get("signing_keys", {}),
            clock_skew_seconds=int(self.options.get("clock_skew_seconds", 30)),
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
        source_digest = SignedJsonJobSerializer.canonical_envelope_sha256(source_body)
        if (
            str(source_payload["job_id"]) != str(source_job_id)
            or source_digest != self._row_value(source, "payload_sha256")
            or source_payload.get("db_job_id") != self._row_value(source, "db_job_id")
        ):
            raise DeliveryEnvelopeMismatch(
                "Ledger replay source does not match its signed envelope."
            )

        replay_job_id = str(uuid.uuid4())
        tracker = (
            self.application.make("JobTracker")
            if self.application is not None and self.application.has("JobTracker")
            else None
        )
        if tracker is None or getattr(tracker, "job_model", None) is None:
            raise QueueException("Queue replay requires a persistent JobTracker model.")
        descriptor = source_payload["job"]
        replay_db_job_id = tracker.create_job_record(
            job_name=str(descriptor["class"]),
            job_class=(f"{descriptor['module']}.{descriptor['class']}"),
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
            clock_skew_seconds=int(self.options.get("clock_skew_seconds", 30)),
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


def _delivery_publish_one(self, job_id: str) -> bool | None:
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


def _delivery_claim_publish(self, job_id: str) -> tuple[Any, str] | None:
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
