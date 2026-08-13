"""Durable AMQP delivery registration, relay and retry behavior."""

from __future__ import annotations

import random
import time
import uuid
from typing import Any

import pendulum
import pendulum as pendulum_module

from cara.context import Tenancy
from cara.observability import MetricsBase
from cara.queues.Bus import Bus
from cara.queues.contracts import UniqueJob

try:
    import pika
except ImportError:  # pragma: no cover - optional queue extra
    pika = None  # type: ignore[assignment]

from cara.exceptions import QueueException
from cara.facades import Log
from cara.observability import Trace as _Trace
from cara.queues.delay import DurableDelayedJobStore
from cara.queues.delivery import UniqueDeliveryConflict
from cara.queues.serializers.SignedJsonJobSerializer import SignedJsonJobSerializer


def _amqp_delivery_push(self, *jobs: Any, options: dict[str, Any]) -> str | list[str]:
    """Durably accept jobs, then publish only after the DB commit."""
    merged_opts = {**self.options, **options}
    job_ids = []

    for job in jobs:
        self._delivery_store.execution_timeout_for(job)
        # Per-job queue resolution. Driver defaults must never override a
        # queue selected by the job/local router: the old merged-options
        # check always saw config's ``queue=default`` and silently routed
        # every direct dispatch there.
        job_opts = dict(merged_opts)
        if "queue" not in options:
            job_queue = getattr(job, "queue", None)
            if job_queue:
                job_opts["queue"] = job_queue
        job_opts["queue"] = self.require_canonical_queue(job_opts.get("queue"))

        explicit_job_id = options.get("job_id")

        if isinstance(job, UniqueJob) and explicit_job_id is None:
            raise QueueException("UniqueJob dispatch must go through Bus.dispatch().")
        if explicit_job_id is not None and len(jobs) != 1:
            raise QueueException("An explicit job_id requires exactly one job.")
        try:
            job_id = str(uuid.UUID(str(explicit_job_id or uuid.uuid4())))
        except (ValueError, AttributeError, TypeError) as exc:
            raise QueueException("AMQP job_id must be a valid UUID.") from exc
        accepted_job_id = self._register_immediate_delivery(
            job=job,
            job_id=job_id,
            merged_opts=merged_opts,
            job_opts=job_opts,
        )
        job_ids.append(accepted_job_id)

    return job_ids[0] if len(job_ids) == 1 else job_ids


def _amqp_delivery_register_immediate_delivery(
    self,
    *,
    job: Any,
    job_id: str,
    merged_opts: dict[str, Any],
    job_opts: dict[str, Any],
) -> str:
    """Atomically create the tracking fence and immutable delivery row."""

    database = self._delivery_store._db()
    try:
        with database.transaction():
            db_job_id = self._create_job_record(job, job_id, job_opts)
            timeout_seconds = self._delivery_store.execution_timeout_for(job)
            payload = {
                "obj": job,
                "args": merged_opts.get("args", ()),
                "callback": merged_opts.get("callback", "handle"),
                "created": pendulum.now(tz=merged_opts.get("tz", "UTC")),
                "job_id": job_id,
                "db_job_id": db_job_id,
                "timeout_seconds": timeout_seconds,
                "attempts": int(
                    merged_opts.get("attempts", 0)
                    if merged_opts.get("attempts", 0) is not None
                    else 0
                ),
            }
            is_unique = isinstance(job, UniqueJob)
            unique_key = merged_opts.get("unique_key")
            if is_unique and unique_key is None:
                raise QueueException(
                    "UniqueJob dispatch requires originating uniqueness metadata."
                )
            if not is_unique and unique_key is not None:
                raise QueueException("Non-unique jobs cannot carry uniqueness metadata.")
            payload["unique_key"] = unique_key
            payload["_otel"] = merged_opts.get("_otel") or _Trace.inject({})
            payload.update(self._tenant_payload(job, merged_opts))
            dispatched_at = pendulum.now("UTC").to_iso8601_string()
            payload["dispatched_at"] = dispatched_at
            payload["queue"] = self.require_canonical_queue(job_opts.get("queue"))
            payload["priority"] = self._priority_name(job, job_opts)
            payload["replay_of"] = None
            if hasattr(job, "__dict__"):
                job._dispatched_at = dispatched_at

            body = self._serialize_payload(payload, job_opts)
            envelope = SignedJsonJobSerializer.inspect_envelope(
                body,
                signing_keys=job_opts.get("signing_keys", {}),
                clock_skew_seconds=int(job_opts.get("clock_skew_seconds", 30)),
                max_age_seconds=int(
                    job_opts.get(
                        "envelope_max_age_seconds",
                        SignedJsonJobSerializer.DEFAULT_MAX_AGE_SECONDS,
                    )
                ),
                allow_not_before=True,
            )
            self._delivery_store.register(
                body=body,
                payload=envelope["payload"],
                envelope=envelope,
                db=database,
            )
            self._delivery_store.publish_after_commit(job_id)
    except UniqueDeliveryConflict as conflict:
        return conflict.job_id
    return job_id


def _amqp_delivery_serialize_payload(
    self,
    payload: dict[str, Any],
    opts: dict[str, Any],
    *,
    issued_at: Any | None = None,
    not_before: Any | None = None,
) -> bytes:
    return SignedJsonJobSerializer.serialize(
        payload,
        signing_key_id=opts.get("signing_key_id", ""),
        signing_keys=opts.get("signing_keys", {}),
        allowed_prefixes=opts.get("allowed_job_prefixes"),
        issued_at=issued_at,
        not_before=not_before,
        ttl_seconds=int(
            opts.get(
                "envelope_ttl_seconds",
                SignedJsonJobSerializer.DEFAULT_TTL_SECONDS,
            )
        ),
        max_age_seconds=int(
            opts.get(
                "envelope_max_age_seconds",
                SignedJsonJobSerializer.DEFAULT_MAX_AGE_SECONDS,
            )
        ),
    )


def _amqp_delivery_tenant_payload(job: Any, opts: dict[str, Any]) -> dict[str, Any]:
    """Derive the signed tenant mode; callers cannot silently go central."""

    is_central_job = bool(getattr(job, "central_job", False))
    explicit_tenant = "tenant_id" in opts
    if is_central_job:
        if explicit_tenant:
            raise QueueException("Central jobs cannot be dispatched with tenant_id.")
        if not Tenancy.is_central():
            raise QueueException(
                f"Central job {job.__class__.__name__} requires "
                "an explicit Tenancy.central() scope."
            )
        return {"_tenant_mode": "central", "_tenant": None}

    if not Tenancy.is_tenant():
        raise QueueException(
            f"Ordinary job {job.__class__.__name__} requires an active tenant."
        )
    tenant_id = Tenancy.id()
    if explicit_tenant and opts.get("tenant_id") != tenant_id:
        raise QueueException(
            "Explicit tenant_id must exactly match the active tenant scope."
        )
    return {"_tenant_mode": "tenant", "_tenant": tenant_id}


def _amqp_delivery_batch(self, *jobs: Any, options: dict[str, Any]) -> None:
    raise QueueException("AMQP batches require a durable JSON batch descriptor.")


def _amqp_delivery_chain(self, jobs: list, options: dict[str, Any]) -> None:
    raise QueueException("AMQP chains require a durable JSON chain descriptor.")


def _amqp_delivery_schedule(
    self, job: Any, when: Any, options: dict[str, Any]
) -> str | list[str]:
    """Persist a future AMQP dispatch in the durable database outbox.

    RabbitMQ itself is not a scheduler. The previous implementation merely
    attached an ``x-delay`` header while publishing to the default direct
    exchange; without the delayed-message exchange plugin that header is
    inert and every retry ran immediately. PostgreSQL now owns the clock,
    while a scheduler sweep publishes due signed envelopes with confirms.
    """
    target = DurableDelayedJobStore._as_utc_datetime(when)
    is_source_retry = (
        options.get("source_delivery_job_id") is not None
        or options.get("source_delivery_lease_token") is not None
    )
    if target <= pendulum.now("UTC") and not is_source_retry:
        return self.push(job, options=options)
    try:
        return self._delayed_store.schedule(job, target, options)
    except UniqueDeliveryConflict as conflict:
        if is_source_retry:
            raise
        return conflict.job_id


def _amqp_delivery_later(
    self, delay: int | pendulum.Duration, job: Any, options: dict[str, Any] = None
) -> str | list[str]:
    """
    Schedule a job to be executed after a delay.

    Uses the durable database delay outbox; no RabbitMQ plugin or
    TTL/dead-letter transfer is involved.

    Args:
        delay: Delay in seconds or pendulum Duration
        job: Job instance to schedule
        options: Queue options

    Returns:
        Job ID(s) — the real IDs assigned by push(), not fabricated ones.
    """

    if options is None:
        options = {}

    # Handle delay as Duration or int
    if isinstance(delay, int):
        delay_seconds = delay
    else:
        delay_seconds = (
            int(delay.total_seconds()) if hasattr(delay, "total_seconds") else delay
        )

    # Calculate when job should run
    when = pendulum_module.now(tz=options.get("tz", self.options.get("tz", "UTC"))).add(
        seconds=delay_seconds
    )

    return self.schedule(job, when, options)


def _amqp_delivery_dispatch_due_delayed_jobs(self) -> dict[str, int]:
    """Publish one bounded batch from the unified delivery outbox."""
    return self._delayed_store.dispatch_due()


def _amqp_delivery_wake_outbox_relay(self) -> None:
    """Best-effort in-process hint; durable polling remains authoritative."""
    self._relay_wakeup.set()


def _amqp_delivery_relay_publish_once(self) -> dict[str, int]:
    """Run one bounded broker-publication relay iteration."""
    self.verify_runtime_health()
    result = self._delivery_store.publish_due()
    if int(result.get("retried", 0) or 0) or int(result.get("settle_lost", 0) or 0):
        self.invalidate_runtime_health()
    self.refresh_delivery_metrics()
    return result


def _amqp_delivery_invalidate_runtime_health(self) -> None:
    """Force the next capability probe after a real relay failure."""
    self._runtime_health_cache.clear()


def _amqp_delivery_due_terminal_hook_ids(self) -> list[str]:
    self.verify_runtime_health()
    return self._delivery_store.due_terminal_hook_ids()


def _amqp_delivery_process_terminal_hook(self, job_id: str) -> bool:
    return self._delivery_store.process_terminal_hooks(job_id)


def _amqp_delivery_defer_terminal_hook_process_failure(
    self,
    job_id: str,
    *,
    error: str,
) -> str:
    return self._delivery_store.defer_terminal_hook_process_failure(
        job_id,
        error=error,
    )


def _amqp_delivery_retry_quarantined_terminal_hooks(
    self,
    job_id: str,
    *,
    operator: str,
    reason: str,
) -> None:
    self._delivery_store.retry_quarantined_terminal_hooks(
        job_id,
        operator=operator,
        reason=reason,
    )
    self.refresh_delivery_metrics()


def _amqp_delivery_refresh_delayed_job_metrics(self) -> None:
    """Refresh scheduler-owned delayed-outbox gauges."""
    self._delayed_store.refresh_metrics()


def _amqp_delivery_refresh_delivery_metrics(self) -> dict[str, Any]:
    """Refresh bounded ledger snapshots owned by relay processes."""
    snapshot = self._delivery_store.delivery_metrics()
    try:
        for status, count in snapshot["statuses"].items():
            MetricsBase.queue_delivery_ledger_jobs.labels(status=status).set(count)
        for kind, count in snapshot["stale_leases"].items():
            MetricsBase.queue_delivery_stale_leases.labels(kind=kind).set(count)
        for priority, backlog in snapshot["priority_backlog"].items():
            MetricsBase.queue_delivery_priority_pending.labels(priority=priority).set(
                backlog["pending"]
            )
            MetricsBase.queue_delivery_priority_oldest_due_age_seconds.labels(
                priority=priority
            ).set(backlog["oldest_due_age"])
            MetricsBase.queue_delivery_priority_latency_budget_seconds.labels(
                priority=priority
            ).set(backlog["latency_budget"])
        for queue, backlog in snapshot["lane_backlog"].items():
            MetricsBase.queue_delivery_lane_pending.labels(queue=queue).set(
                backlog["pending"]
            )
            MetricsBase.queue_delivery_lane_processing.labels(queue=queue).set(
                backlog["processing"]
            )
            MetricsBase.queue_delivery_lane_broker_outstanding.labels(queue=queue).set(
                backlog["broker_outstanding"]
            )
            MetricsBase.queue_delivery_lane_oldest_due_age_seconds.labels(
                queue=queue
            ).set(backlog["oldest_due_age"])
            MetricsBase.queue_delivery_lane_throughput_per_second.labels(queue=queue).set(
                backlog["throughput_per_second"]
            )
        MetricsBase.queue_delivery_broker_window_max_outstanding.set(
            snapshot["broker_window"]["max_outstanding"]
        )
        MetricsBase.queue_delivery_broker_window_limit.set(
            snapshot["broker_window"]["limit"]
        )
        for state, count in snapshot["hooks"].items():
            MetricsBase.queue_terminal_hooks.labels(state=state).set(count)
        MetricsBase.queue_delayed_jobs.labels(status="pending").set(
            snapshot["publish_pending"]
        )
        MetricsBase.queue_delayed_jobs.labels(status="processing").set(
            snapshot["publish_processing"]
        )
        MetricsBase.queue_delayed_jobs.labels(status="failed").set(
            snapshot["publish_quarantined"]
        )
        MetricsBase.queue_delayed_oldest_due_age_seconds.set(snapshot["oldest_due_age"])
        # LAST, and deliberately inside the try: every alert scoped to a
        # relay-published gauge is blind without a freshness anchor.
        # prometheus_client gauges are sticky — a wedged or half-failed
        # refresh leaves the previous values exported verbatim, so a
        # broken publisher is indistinguishable from a healthy queue and
        # `absent()` never sees anything missing. Writing the timestamp
        # only after every set above has succeeded means the swallowed
        # exception below strands it, and QueueDeliveryMetricsStale fires
        # instead of the whole family silently freezing.
        MetricsBase.queue_delivery_metrics_timestamp_seconds.set(time.time())
    except Exception as exc:
        # The stale timestamp remains the alert signal, while this report
        # preserves the concrete publisher failure for diagnosis.
        Log.warning(
            "Queue delivery metrics refresh failed: %s",
            exc,
            category="cara.queue.delivery",
        )
    return snapshot


def _amqp_delivery_publish_registered_envelope(
    self,
    body: bytes,
    payload: dict[str, Any],
    *,
    capability: Any,
) -> None:
    if capability is not self._delivery_store:
        raise QueueException(
            "AMQP broker publication requires a claimed delivery-ledger row."
        )
    opts = self.options
    url = self._build_url(opts)
    self._acquire_thread_connection(url, opts)
    try:
        queue_name = str(payload["queue"])
        self.require_canonical_queue(queue_name)
        message_priority = self._message_priority(
            None,
            {**opts, "priority": payload["priority"]},
        )
        self.channel.basic_publish(
            exchange=opts.get("exchange", ""),
            routing_key=queue_name,
            body=body,
            properties=pika.BasicProperties(
                content_encoding="utf-8",
                content_type="application/json",
                delivery_mode=2,
                message_id=str(payload.get("job_id") or ""),
                priority=message_priority,
                type=f"cara.job.v{SignedJsonJobSerializer.VERSION}",
            ),
            mandatory=True,
        )
    except Exception:
        self._discard_thread_connection()
        raise
    else:
        self._return_thread_connection(url)


def _amqp_delivery_apply_retry_jitter(self, base_delay: int, instance: Any) -> int:
    """Add full-jitter spread to a retry delay.

    A job class can override the spread with a
    ``retry_jitter_fraction`` attribute (0 disables jitter).
    Floor is always ``1s`` so callers don't accidentally retry
    immediately when ``base_delay`` is small and the jitter swing
    rounds the result down to zero. Floor is also bounded ABOVE
    by ``base_delay + jitter_max`` so a misconfigured fraction
    never inflates the wait time beyond the schedule's intent.
    """
    if base_delay <= 0:
        return 0
    fraction = getattr(
        instance, "retry_jitter_fraction", self.DEFAULT_RETRY_JITTER_FRACTION
    )
    try:
        fraction = float(fraction)
    except TypeError, ValueError:
        fraction = self.DEFAULT_RETRY_JITTER_FRACTION
    if fraction <= 0:
        return base_delay
    # Clamp the spread so a bad config (1.0+) doesn't double the
    # base delay or push the lower end below zero.
    fraction = min(fraction, 0.9)
    swing = base_delay * fraction
    jitter = random.uniform(-swing, swing)
    return max(1, int(round(base_delay + jitter)))


def _amqp_delivery_create_job_record(self, job, job_id: str, opts: dict[str, Any]) -> int:
    """Create job record via JobTracker for consistent tracking."""
    tracker = self._resolve_job_tracker()
    if tracker is None or getattr(tracker, "job_model", None) is None:
        raise QueueException(
            "Durable AMQP dispatch requires a persistent JobTracker model."
        )

    queue_name = job.queue if hasattr(job, "queue") and job.queue else opts.get("queue")
    queue_name = self.require_canonical_queue(queue_name)
    job_name = job.__class__.__name__
    job_class = f"{job.__class__.__module__}.{job.__class__.__name__}"

    payload = Bus.get_dispatch_params(job)
    db_job_id = tracker.create_job_record(
        job_name=job_name,
        job_class=job_class,
        queue=queue_name,
        execution_mode="queued",
        payload=payload,
        metadata={"job_id": job_id, "driver": "amqp"},
    )
    if isinstance(db_job_id, bool) or not isinstance(db_job_id, int) or db_job_id <= 0:
        raise QueueException("JobTracker did not persist a positive AMQP db_job_id.")
    return db_job_id


def _amqp_delivery_resolve_job_tracker(self):
    """Resolve JobTracker from container."""
    if self.application and self.application.has("JobTracker"):
        return self.application.make("JobTracker")
    return None
