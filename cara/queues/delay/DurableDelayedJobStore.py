"""Delayed-job adapter backed by the unified queue delivery ledger."""

from __future__ import annotations

import hashlib
import uuid
from typing import Any

import pendulum

from cara.exceptions import QueueException
from cara.observability import Trace
from cara.queues.delivery import QueueJobDeliveryStore
from cara.queues.serializers import SignedJsonJobSerializer


class DurableDelayedJobStore:
    """Create due-at deliveries in the same outbox used by immediate jobs."""

    def __init__(
        self,
        application: Any,
        driver: Any,
        options: dict[str, Any],
        delivery_store: QueueJobDeliveryStore | None = None,
    ):
        self.application = application
        self.driver = driver
        self.options = options
        self.delivery_store = delivery_store or QueueJobDeliveryStore(
            application,
            driver,
            options,
        )

    def schedule(
        self,
        job: Any,
        when: Any,
        options: dict[str, Any],
    ) -> str:
        """Atomically create a new delayed delivery and settle its retry source."""
        now = pendulum.now("UTC")
        available_at = self._as_utc_datetime(when)
        timeout_seconds = self.delivery_store.execution_timeout_for(job)
        merged = {**self.options, **options}
        queue_name = merged.get("queue")
        if "queue" not in options:
            queue_name = getattr(job, "queue", None) or queue_name
        queue_name = self.driver.require_canonical_queue(queue_name)

        priority = self.driver._priority_name(job, merged)
        explicit_job_id = options.get("job_id")
        if explicit_job_id is not None:
            try:
                job_id = str(uuid.UUID(str(explicit_job_id)))
            except (ValueError, AttributeError, TypeError) as exc:
                raise QueueException("Delayed AMQP job_id must be a valid UUID.") from exc
        else:
            dedup_source = options.get("deduplication_key") or uuid.uuid4().hex
            if not isinstance(dedup_source, str) or not dedup_source.strip():
                raise QueueException("Delayed AMQP deduplication_key must be a string.")
            digest = hashlib.sha256(
                b"cara.queue.delivery.retry.v2\x00" + dedup_source.strip().encode("utf-8")
            ).hexdigest()
            job_id = str(uuid.UUID(digest[:32]))
        attempts = self._non_negative_int(options.get("attempts", 0), "attempts")
        dispatched_at = now.to_iso8601_string()
        tenant_fields = self.driver._tenant_payload(job, merged)
        from cara.queues.contracts import UniqueJob

        unique_key = options.get("unique_key")
        is_source_retry = (
            options.get("source_delivery_job_id") is not None
            or options.get("source_delivery_lease_token") is not None
        )
        if isinstance(job, UniqueJob):
            if explicit_job_id is None and not is_source_retry:
                raise QueueException("UniqueJob dispatch must go through Bus.dispatch().")
            if unique_key is None:
                raise QueueException(
                    "UniqueJob dispatch requires originating uniqueness metadata."
                )

        database = self._db()
        with database.transaction():
            explicit_db_job_id = self._optional_positive_int(options.get("db_job_id"))
            db_job_id = explicit_db_job_id or self.driver._create_job_record(
                job,
                job_id,
                merged,
            )
            payload = {
                "obj": job,
                "args": options.get("args", ()),
                "callback": options.get("callback", "handle"),
                "created": str(options.get("created") or now.to_iso8601_string()),
                "job_id": job_id,
                "db_job_id": db_job_id,
                "timeout_seconds": timeout_seconds,
                "attempts": attempts,
                "_otel": options.get("_otel") or Trace.inject({}),
                **tenant_fields,
                "queue": queue_name,
                "priority": priority,
                "dispatched_at": dispatched_at,
                "replay_of": None,
                "unique_key": unique_key,
            }
            body = SignedJsonJobSerializer.serialize(
                payload,
                signing_key_id=merged.get("signing_key_id", ""),
                signing_keys=merged.get("signing_keys", {}),
                allowed_prefixes=merged.get("allowed_job_prefixes"),
                issued_at=now,
                not_before=available_at,
                ttl_seconds=int(
                    merged.get(
                        "envelope_ttl_seconds",
                        SignedJsonJobSerializer.DEFAULT_TTL_SECONDS,
                    )
                ),
                max_age_seconds=int(
                    merged.get(
                        "envelope_max_age_seconds",
                        SignedJsonJobSerializer.DEFAULT_MAX_AGE_SECONDS,
                    )
                ),
            )
            envelope = SignedJsonJobSerializer.inspect_envelope(
                body,
                signing_keys=merged.get("signing_keys", {}),
                now=now,
                clock_skew_seconds=int(merged.get("clock_skew_seconds", 30)),
                max_age_seconds=int(
                    merged.get(
                        "envelope_max_age_seconds",
                        SignedJsonJobSerializer.DEFAULT_MAX_AGE_SECONDS,
                    )
                ),
                allow_not_before=True,
            )
            source_id = options.get("source_delivery_job_id")
            source_token = options.get("source_delivery_lease_token")
            if source_id is not None or source_token is not None:
                if not isinstance(source_id, str) or not isinstance(source_token, str):
                    raise QueueException(
                        "Retry scheduling requires source delivery id and lease token."
                    )
                self.delivery_store.mark_retry_scheduled(
                    source_id,
                    source_token,
                    db=database,
                )
            self.delivery_store.register(
                body=body,
                payload=envelope["payload"],
                envelope=envelope,
                db=database,
            )
            database.statement(
                "UPDATE job SET status = %s, available_at = %s, attempts = %s, "
                "updated_at = %s WHERE id = %s",
                ["retrying", available_at, attempts, now, db_job_id],
            )

        self.delivery_store.publish_after_commit(job_id)
        self._transition("scheduled")
        return job_id

    def dispatch_due(self) -> dict[str, int]:
        result = self.delivery_store.publish_due()
        for outcome, count in result.items():
            if count:
                self._transition(outcome, count=count)
        return result

    def refresh_metrics(self) -> None:
        try:
            backlog = self.delivery_store.backlog_metrics()
            from cara.observability.Metrics import MetricsBase

            MetricsBase.queue_delayed_jobs.labels(status="pending").set(backlog["count"])
            MetricsBase.queue_delayed_jobs.labels(status="processing").set(0)
            MetricsBase.queue_delayed_jobs.labels(status="failed").set(0)
            MetricsBase.queue_delayed_oldest_due_age_seconds.set(backlog["age"])
        except Exception:
            # Metrics must never make the durable publisher unavailable.
            return

    def _db(self) -> Any:
        if self.application is None or not self.application.has("DB"):
            raise QueueException(
                "Durable delayed AMQP dispatch requires the application DB binding."
            )
        return self.application.make("DB")

    @staticmethod
    def _as_utc_datetime(value: Any) -> pendulum.DateTime:
        if isinstance(value, pendulum.DateTime):
            parsed = value
        else:
            try:
                parsed = pendulum.parse(str(value))
            except (TypeError, ValueError) as exc:
                raise QueueException(
                    f"Invalid delayed AMQP timestamp: {value!r}"
                ) from exc
        if parsed.tzinfo is None:
            raise QueueException("Delayed AMQP timestamps must include a timezone.")
        return parsed.in_timezone("UTC")

    @staticmethod
    def _optional_positive_int(value: Any) -> int | None:
        if value is None:
            return None
        if isinstance(value, bool):
            raise QueueException("db_job_id must be a positive integer or null.")
        try:
            parsed = int(value)
        except (TypeError, ValueError) as exc:
            raise QueueException("db_job_id must be a positive integer or null.") from exc
        if parsed <= 0:
            raise QueueException("db_job_id must be a positive integer or null.")
        return parsed

    @staticmethod
    def _non_negative_int(value: Any, field: str) -> int:
        if isinstance(value, bool):
            raise QueueException(f"{field} must be a non-negative integer.")
        try:
            parsed = int(value)
        except (TypeError, ValueError) as exc:
            raise QueueException(f"{field} must be a non-negative integer.") from exc
        if parsed < 0:
            raise QueueException(f"{field} must be a non-negative integer.")
        return parsed

    @staticmethod
    def _transition(outcome: str, *, count: int = 1) -> None:
        try:
            from cara.observability.Metrics import MetricsBase

            MetricsBase.queue_delayed_transitions_total.labels(outcome=outcome).inc(count)
        except Exception:
            return
