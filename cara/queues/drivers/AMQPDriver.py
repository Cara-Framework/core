"""
AMQP Queue Driver for the Cara framework.

Modern, clean implementation for RabbitMQ-based job queue management.
"""

from __future__ import annotations

import logging
import random
import ssl
import threading
import time
import uuid
from typing import Any

import pendulum

try:
    # ``pika`` is the optional 'queue' extra (cara[queue]). Import it at module
    # top WHEN PRESENT so the hot publish/consume paths reference ``pika.*``
    # with no per-call import cost — but degrade to ``None`` when absent so a
    # service that never runs a queue worker (e.g. a DB-less HTTP/render app)
    # can still import ``cara.queues`` and its command package. Every code path
    # that actually opens an AMQP connection re-checks and raises
    # ``QueueDriverLibraryNotFoundException`` with an install hint (see the
    # guarded ``import pika`` in the connection methods), so a missing pika
    # fails LOUD at use, never silently.
    import pika
except ImportError:  # pragma: no cover - exercised only without the extra
    pika = None  # type: ignore[assignment]

import contextlib

from cara.exceptions import QueueDriverLibraryNotFoundException, QueueException
from cara.facades import Log
from cara.observability import Trace as _Trace
from cara.queues.contracts.Queue import Queue
from cara.queues.delay import DurableDelayedJobStore
from cara.queues.delivery import QueueJobDeliveryStore, UniqueDeliveryConflict
from cara.queues.retry.Policy import (
    DEFAULT_MAX_ATTEMPTS as _RETRY_DEFAULT_MAX_ATTEMPTS,
)
from cara.queues.retry.Policy import (
    DEFAULT_RETRY_BACKOFF_SECONDS as _RETRY_DEFAULT_BACKOFF_SECONDS,
)
from cara.queues.retry.Policy import (
    DEFAULT_RETRY_JITTER_FRACTION as _RETRY_DEFAULT_JITTER_FRACTION,
)
from cara.queues.serializers.SignedJsonJobSerializer import (
    SignedJsonJobSerializer,
)
from cara.support import HasColoredOutput

# Connection/stream errors that warrant one publish retry. Built at module
# level so the ``except`` clause in push() never dereferences
# ``pika.exceptions`` when the extra isn't installed — doing so raised
# AttributeError mid-handling and masked the install-hint exception.
_RETRYABLE_PUBLISH_ERRORS: tuple[type[BaseException], ...] = (
    BrokenPipeError,
    ConnectionResetError,
    ConnectionRefusedError,
    OSError,
)
if pika is not None:
    _RETRYABLE_PUBLISH_ERRORS = (
        pika.exceptions.AMQPConnectionError,
        pika.exceptions.StreamLostError,
    ) + _RETRYABLE_PUBLISH_ERRORS


class AMQPDriver(HasColoredOutput, Queue):
    """
    AMQP-based queue driver for RabbitMQ.

    Features:
    - Reliable message delivery with publisher confirms
    - HMAC-authenticated JSON-only job envelopes
    - Broker-native priority queues
    - Job tracking with unique IDs
    - Integration with JobTracker for status updates
    - Persistent messages and durable queues
    - Bounded automatic retry in QueueWorkCommand
    """

    driver_name = "amqp"
    durable_transactional_outbox = True

    # Framework-level default retry policy — SINGLE-SOURCED in
    # ``cara.queues.retry.Policy`` (the rationale for 1/5/30 + 25% jitter
    # lives there) so this driver, the production worker
    # (``QueueWorkCommand``) and the publisher-side ``retry`` can never
    # silently drift. A job class still overrides per-job by declaring
    # ``max_attempts`` / ``retry_backoff`` at the class level.
    DEFAULT_MAX_ATTEMPTS = _RETRY_DEFAULT_MAX_ATTEMPTS
    DEFAULT_RETRY_BACKOFF_SECONDS = _RETRY_DEFAULT_BACKOFF_SECONDS
    DEFAULT_RETRY_JITTER_FRACTION = _RETRY_DEFAULT_JITTER_FRACTION

    def __init__(self, application, options: dict[str, Any]):
        super().__init__(module="queue.amqp")
        self.application = application
        self.options = options
        canonical = options.get("canonical_queues") or ()
        self._canonical_queues = frozenset(str(name) for name in canonical)
        if not self._canonical_queues:
            raise QueueException("AMQP canonical_queues must not be empty.")
        # ``connection`` / ``channel`` were instance attributes shared
        # across all threads. They're now thread-local so each thread
        # owns its own pika connection/channel — pika's BlockingConnection
        # is not thread-safe, and the previous global lock pattern
        # serialised every publish across the whole worker process.
        # With per-thread state, parallel publishes from different
        # threads run truly concurrently against separate sockets,
        # while a single thread's publishes stay ordered through its
        # own channel.
        self._tls = threading.local()
        self._relay_wakeup = threading.Event()
        self._runtime_health_cache: dict[tuple[str, tuple[str, ...]], float] = {}

        self._delivery_store = QueueJobDeliveryStore(
            application=self.application,
            driver=self,
            options=self.options,
        )
        self._delayed_store = DurableDelayedJobStore(
            application=self.application,
            driver=self,
            options=self.options,
            delivery_store=self._delivery_store,
        )

        # Suppress verbose pika logs
        logging.getLogger("pika").setLevel(logging.WARNING)

    # ── Thread-local connection / channel handles ─────────────────
    # Existing call sites read/write ``self.connection`` and
    # ``self.channel`` directly. Routing through a ``threading.local``
    # via these properties preserves the call-site shape while
    # making the state per-thread.
    @property
    def connection(self):
        return getattr(self._tls, "connection", None)

    @connection.setter
    def connection(self, value):
        self._tls.connection = value

    @property
    def channel(self):
        return getattr(self._tls, "channel", None)

    @channel.setter
    def channel(self, value):
        self._tls.channel = value

    def ping(self, timeout_ms: int = 1000) -> None:
        """Perform an isolated AMQP handshake without touching publish state.

        A driver's thread-local or pooled connection only proves that an old
        connection existed. A fresh handshake verifies DNS/TCP, TLS (when
        configured), authentication, vhost access, and channel creation. The
        probe connection is never placed in the publisher pool.
        """
        if pika is None:
            raise QueueDriverLibraryNotFoundException(
                "pika is required for AMQPDriver. Install with: pip install pika"
            )

        timeout_seconds = max(int(timeout_ms), 1) / 1000
        parameters = self._connection_parameters(self.options)
        parameters.connection_attempts = 1
        parameters.retry_delay = 0
        parameters.socket_timeout = timeout_seconds
        parameters.stack_timeout = timeout_seconds
        parameters.blocked_connection_timeout = timeout_seconds

        connection = None
        channel = None
        try:
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
        finally:
            if channel is not None:
                with contextlib.suppress(
                    OSError,
                    ConnectionError,
                    RuntimeError,
                    AttributeError,
                    pika.exceptions.AMQPError,
                ):
                    channel.close()
            if connection is not None:
                with contextlib.suppress(
                    OSError,
                    ConnectionError,
                    RuntimeError,
                    AttributeError,
                    pika.exceptions.AMQPError,
                ):
                    connection.close()

    def open_topology_connection(self) -> tuple[Any, Any]:
        """Open a dedicated, unpooled connection for broker topology changes.

        Topology reconciliation is an operator/deploy operation, not a publish
        hot path. Returning an isolated connection prevents queue/exchange
        declarations (and broker-closed passive-declare channels) from
        poisoning the driver's thread-local publisher pool. The caller owns
        and must close both returned handles.
        """
        return self._open_new_connection(self.options)

    def verify_runtime_health(
        self,
        queue_names: Any | None = None,
        *,
        force: bool = False,
    ) -> None:
        """Verify only the resources allowed by this process capability."""
        access = str(self.options.get("broker_access") or "full").strip().lower()
        if access not in {"none", "consume", "publish", "topology", "full"}:
            raise QueueException(
                f"Unsupported AMQP broker_access capability: {access!r}."
            )
        names = tuple(
            sorted(
                {
                    self.require_canonical_queue(name)
                    for name in (queue_names or self._canonical_queues)
                }
            )
        )
        cache_key = (access, names)
        now = time.monotonic()
        last_check = self._runtime_health_cache.get(cache_key, 0.0)
        if not force and now - last_check < 10:
            return
        self._delivery_store.verify_schema()
        if access == "none":
            self._runtime_health_cache[cache_key] = now
            return

        connection, bootstrap = self.open_topology_connection()
        with contextlib.suppress(Exception):
            bootstrap.close()
        try:
            if access == "publish":
                # Prove write authorization without leaving a message behind.
                # The default exchange routes only to an exactly named queue;
                # a random nonexistent route with mandatory confirms must
                # therefore return UnroutableError after Rabbit accepts the
                # publish. AccessRefused/Nack/transport errors still propagate.
                channel = connection.channel()
                try:
                    channel.confirm_delivery()
                    try:
                        channel.basic_publish(
                            exchange="",
                            routing_key=(f"__cara_write_probe__.{uuid.uuid4().hex}"),
                            body=b"",
                            mandatory=True,
                            properties=pika.BasicProperties(
                                content_type="application/octet-stream",
                                delivery_mode=1,
                                expiration="1",
                                type="cara.queue.write-probe",
                            ),
                        )
                    except pika.exceptions.UnroutableError:
                        pass
                    else:
                        raise QueueException(
                            "RabbitMQ write probe unexpectedly routed; "
                            "the reserved health queue namespace is occupied."
                        )
                finally:
                    with contextlib.suppress(Exception):
                        channel.close()
                self._runtime_health_cache[cache_key] = now
                return
            if access == "consume":
                resources = [("queue", name) for name in names]
            else:
                resources = [
                    ("exchange", "dead.letter.dlx"),
                    ("queue", "dead.letter.queue"),
                    *(("queue", name) for name in names),
                ]
            for kind, name in resources:
                channel = connection.channel()
                try:
                    if kind == "exchange":
                        channel.exchange_declare(
                            exchange=name,
                            exchange_type="topic",
                            passive=True,
                        )
                    else:
                        channel.queue_declare(queue=name, passive=True)
                finally:
                    with contextlib.suppress(Exception):
                        channel.close()
        finally:
            with contextlib.suppress(Exception):
                connection.close()
        self._runtime_health_cache[cache_key] = now

    def push(self, *jobs: Any, options: dict[str, Any]) -> str | list[str]:
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
            from cara.queues.contracts import UniqueJob

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

    def _register_immediate_delivery(
        self,
        *,
        job: Any,
        job_id: str,
        merged_opts: dict[str, Any],
        job_opts: dict[str, Any],
    ) -> str:
        """Atomically create the tracking fence and immutable delivery row."""
        from cara.queues.contracts import UniqueJob

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
                    raise QueueException(
                        "Non-unique jobs cannot carry uniqueness metadata."
                    )
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

    @property
    def delivery_store(self) -> QueueJobDeliveryStore:
        return self._delivery_store

    def require_canonical_queue(self, queue_name: Any) -> str:
        """Return a configured consumed queue or fail before persistence."""
        if not isinstance(queue_name, str) or not queue_name:
            raise QueueException("AMQP jobs must declare an explicit canonical queue.")
        if queue_name not in self._canonical_queues:
            valid = ", ".join(sorted(self._canonical_queues))
            raise QueueException(
                f"AMQP queue {queue_name!r} is not consumed. Valid: {valid}."
            )
        return queue_name

    def _serialize_payload(
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

    @staticmethod
    def _tenant_payload(job: Any, opts: dict[str, Any]) -> dict[str, Any]:
        """Derive the signed tenant mode; callers cannot silently go central."""
        from cara.context import Tenancy

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

    def batch(self, *jobs: Any, options: dict[str, Any]) -> None:
        raise QueueException("AMQP batches require a durable JSON batch descriptor.")

    def chain(self, jobs: list, options: dict[str, Any]) -> None:
        raise QueueException("AMQP chains require a durable JSON chain descriptor.")

    def schedule(self, job: Any, when: Any, options: dict[str, Any]) -> str | list[str]:
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

    def later(
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
        import pendulum as pendulum_module

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
        when = pendulum_module.now(
            tz=options.get("tz", self.options.get("tz", "UTC"))
        ).add(seconds=delay_seconds)

        return self.schedule(job, when, options)

    def dispatch_due_delayed_jobs(self) -> dict[str, int]:
        """Publish one bounded batch from the unified delivery outbox."""
        return self._delayed_store.dispatch_due()

    def wake_outbox_relay(self) -> None:
        """Best-effort in-process hint; durable polling remains authoritative."""
        self._relay_wakeup.set()

    def relay_publish_once(self) -> dict[str, int]:
        """Run one bounded broker-publication relay iteration."""
        self.verify_runtime_health()
        result = self._delivery_store.publish_due()
        if int(result.get("retried", 0) or 0) or int(result.get("settle_lost", 0) or 0):
            self.invalidate_runtime_health()
        self.refresh_delivery_metrics()
        return result

    def invalidate_runtime_health(self) -> None:
        """Force the next capability probe after a real relay failure."""
        self._runtime_health_cache.clear()

    def due_terminal_hook_ids(self) -> list[str]:
        self.verify_runtime_health()
        return self._delivery_store.due_terminal_hook_ids()

    def process_terminal_hook(self, job_id: str) -> bool:
        return self._delivery_store.process_terminal_hooks(job_id)

    def defer_terminal_hook_process_failure(
        self,
        job_id: str,
        *,
        error: str,
    ) -> str:
        return self._delivery_store.defer_terminal_hook_process_failure(
            job_id,
            error=error,
        )

    def retry_quarantined_terminal_hooks(
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

    def refresh_delayed_job_metrics(self) -> None:
        """Refresh scheduler-owned delayed-outbox gauges."""
        self._delayed_store.refresh_metrics()

    def refresh_delivery_metrics(self) -> dict[str, Any]:
        """Refresh bounded ledger snapshots owned by relay processes."""
        snapshot = self._delivery_store.delivery_metrics()
        try:
            from cara.observability.Metrics import MetricsBase

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
                MetricsBase.queue_delivery_lane_broker_outstanding.labels(
                    queue=queue
                ).set(backlog["broker_outstanding"])
                MetricsBase.queue_delivery_lane_oldest_due_age_seconds.labels(
                    queue=queue
                ).set(backlog["oldest_due_age"])
                MetricsBase.queue_delivery_lane_throughput_per_second.labels(
                    queue=queue
                ).set(backlog["throughput_per_second"])
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
            MetricsBase.queue_delayed_oldest_due_age_seconds.set(
                snapshot["oldest_due_age"]
            )
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
        except Exception:
            pass
        return snapshot

    def _publish_registered_envelope(
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

    def retry(self, options: dict[str, Any]) -> None:
        """Protocol stub — AMQP failed jobs live in the dead-letter queue.

        The worker's failure path
        republishes retryable jobs with backoff via ``later()`` and routes
        exhausted ones to the DLX. Requeuing FROM the DLX is a broker-side
        operation, not something this driver can do here. The previous
        implementation took ``(job, options, attempts, backoff)`` — a
        signature the ``Queue.retry(options=...)`` manager call could never
        satisfy, so it TypeError'd on every invocation and had no callers.
        """
        raise QueueException(
            "AMQPDriver does not support retry(): failed jobs are routed to "
            "the dead-letter exchange. Requeue them from the DLQ (broker "
            "shovel or the recovery cron) instead."
        )

    def _apply_retry_jitter(self, base_delay: int, instance: Any) -> int:
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

    def consume(self, options: dict[str, Any]) -> None:
        """Reject the removed duplicate consumer path.

        ``queue:work`` is the sole AMQP consumer implementation. Keeping two
        loops caused acknowledgement, retry, tenancy and tracing semantics to
        drift, so this driver-level entry point intentionally has no fallback.
        """
        _ = options
        raise QueueException(
            "Use `queue:work`; the duplicate AMQPDriver.consume() path was removed."
        )

    def _create_job_record(self, job, job_id: str, opts: dict[str, Any]) -> int:
        """Create job record via JobTracker for consistent tracking."""
        tracker = self._resolve_job_tracker()
        if tracker is None or getattr(tracker, "job_model", None) is None:
            raise QueueException(
                "Durable AMQP dispatch requires a persistent JobTracker model."
            )

        queue_name = (
            job.queue if hasattr(job, "queue") and job.queue else opts.get("queue")
        )
        queue_name = self.require_canonical_queue(queue_name)
        job_name = job.__class__.__name__
        job_class = f"{job.__class__.__module__}.{job.__class__.__name__}"

        from cara.queues import Bus

        payload = Bus.get_dispatch_params(job)
        db_job_id = tracker.create_job_record(
            job_name=job_name,
            job_class=job_class,
            queue=queue_name,
            execution_mode="queued",
            payload=payload,
            metadata={"job_id": job_id, "driver": "amqp"},
        )
        if (
            isinstance(db_job_id, bool)
            or not isinstance(db_job_id, int)
            or db_job_id <= 0
        ):
            raise QueueException("JobTracker did not persist a positive AMQP db_job_id.")
        return db_job_id

    def _resolve_job_tracker(self):
        """Resolve JobTracker from container."""
        if self.application and self.application.has("JobTracker"):
            return self.application.make("JobTracker")
        return None

    def get_dead_letter_messages(
        self, queue_name: str = "dead.letter.queue", limit: int = 100
    ) -> list[dict[str, Any]]:
        """
        Peek at dead letter queue messages without consuming them.

        Args:
            queue_name: Dead letter queue name
            limit: Maximum messages to retrieve

        Returns:
            List of message details (headers, body, routing_key)
        """
        messages = []
        try:
            self._connect(self.options)

            # Use basic_get to peek at messages without consuming
            for _ in range(limit):
                method, properties, body = self.channel.basic_get(
                    queue_name, auto_ack=False
                )

                if method is None:
                    break

                # Verify the signed JSON envelope without importing the job
                # class. A forged/corrupt DLQ record remains visible as raw
                # metadata but is never dynamically imported.
                try:
                    envelope = SignedJsonJobSerializer.inspect_envelope(
                        body,
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
                    payload = envelope["payload"]
                    signature_valid = True
                    temporal_status = SignedJsonJobSerializer.temporal_status(
                        envelope,
                        clock_skew_seconds=int(
                            self.options.get("clock_skew_seconds", 30)
                        ),
                    )
                except QueueException as exc:
                    payload = {
                        "error": str(exc),
                        "raw": body.decode("utf-8", errors="replace"),
                    }
                    signature_valid = False
                    temporal_status = "invalid"

                messages.append(
                    {
                        "delivery_tag": method.delivery_tag,
                        "routing_key": method.routing_key,
                        "redelivered": method.redelivered,
                        "exchange": method.exchange,
                        "headers": dict(properties.headers or {}),
                        "priority": properties.priority,
                        "signature_valid": signature_valid,
                        "temporal_status": temporal_status,
                        "timestamp": properties.timestamp,
                        "payload": payload,
                    }
                )

                # Don't consume - requeue the message
                self.channel.basic_nack(method.delivery_tag, requeue=True)

        except Exception as e:
            Log.error("Failed to get dead letter messages: %s", e, exc_info=True)
            raise
        finally:
            try:
                if self.channel is not None:
                    self.channel.close()
            except OSError, ConnectionError, RuntimeError, AttributeError:
                pass
            try:
                if self.connection is not None:
                    self.connection.close()
            except OSError, ConnectionError, RuntimeError, AttributeError:
                pass
            self.channel = None
            self.connection = None

        return messages

    def replay_delivery(
        self,
        job_id: str,
        *,
        operator: str,
        reason: str,
    ) -> str:
        """Replay one audited expired/dead delivery directly from PostgreSQL."""
        return self._delivery_store.replay_from_ledger(
            job_id,
            operator=operator,
            reason=reason,
        )

    def canonical_queue_arguments(
        self,
        queue_name: str,
        options: dict[str, Any] | None = None,
    ) -> dict[str, object]:
        """Return the one canonical declaration contract for AMQP queues."""
        opts = {**self.options, **(options or {})}
        exchange_name = opts.get("exchange", "")
        arguments: dict[str, object] = {
            "x-queue-type": "quorum",
            "x-delivery-limit": self._bounded_queue_argument(
                opts.get("delivery_limit", 20),
                field="delivery_limit",
                minimum=1,
                maximum=1000,
            ),
            "x-dead-letter-exchange": (
                f"{exchange_name}.dlx" if exchange_name else "dead.letter.dlx"
            ),
            "x-dead-letter-routing-key": f"dead.{queue_name}",
            "x-dead-letter-strategy": "at-least-once",
            "x-overflow": "reject-publish",
        }

        max_priority = opts.get("max_priority")
        if isinstance(max_priority, bool) or not isinstance(max_priority, int):
            raise QueueException("AMQP max_priority must be an integer.")
        if not 1 <= max_priority <= 31:
            raise QueueException("AMQP max_priority must be between 1 and 31.")
        # RabbitMQ 4.3 quorum queues provide strict 0-31 priorities without
        # x-max-priority (that argument applies only to classic queues).

        for field, argument, default in (
            ("max_length", "x-max-length", 100000),
            ("max_length_bytes", "x-max-length-bytes", 1073741824),
        ):
            value = opts.get(field, default)
            if isinstance(value, bool) or not isinstance(value, int):
                raise QueueException(f"AMQP {field} must be an integer.")
            if value <= 0:
                raise QueueException(f"AMQP {field} must be positive.")
            arguments[argument] = value

        message_ttl = opts.get("message_ttl")
        if message_ttl is not None:
            if isinstance(message_ttl, bool) or not isinstance(message_ttl, int):
                raise QueueException("AMQP message_ttl must be an integer.")
            if message_ttl <= 0:
                raise QueueException("AMQP message_ttl must be positive.")
            arguments["x-message-ttl"] = message_ttl
        return arguments

    def dead_letter_queue_arguments(
        self,
        options: dict[str, Any] | None = None,
    ) -> dict[str, object]:
        """Return the bounded quorum contract for untrusted broker quarantine."""
        opts = {**self.options, **(options or {})}
        return {
            "x-queue-type": "quorum",
            "x-delivery-limit": self._bounded_queue_argument(
                opts.get("delivery_limit", 20),
                field="delivery_limit",
                minimum=1,
                maximum=1000,
            ),
            "x-overflow": "reject-publish",
            "x-max-length": self._bounded_queue_argument(
                opts.get("max_length", 100000),
                field="max_length",
                minimum=1,
                maximum=2_147_483_647,
            ),
            "x-max-length-bytes": self._bounded_queue_argument(
                opts.get("max_length_bytes", 1073741824),
                field="max_length_bytes",
                minimum=1,
                maximum=9_223_372_036_854_775_807,
            ),
        }

    @staticmethod
    def _bounded_queue_argument(
        value: Any,
        *,
        field: str,
        minimum: int,
        maximum: int,
    ) -> int:
        if isinstance(value, bool) or not isinstance(value, int):
            raise QueueException(f"AMQP {field} must be an integer.")
        if not minimum <= value <= maximum:
            raise QueueException(f"AMQP {field} must be between {minimum} and {maximum}.")
        return value

    def _priority_name(self, job: Any, options: dict[str, Any]) -> str:
        explicit = options.get("priority")
        job_priority = getattr(job, "priority", None)
        if not isinstance(job_priority, (str, int)):
            job_priority = getattr(job, "job_priority", None)
        value = explicit if explicit is not None else job_priority
        if value is None:
            value = "default"
        if isinstance(value, bool) or not isinstance(value, (str, int)):
            raise QueueException(f"Invalid AMQP job priority: {value!r}")
        if isinstance(value, int):
            return str(value)

        levels = options.get("priority_levels") or {}
        if value not in levels:
            valid = ", ".join(sorted(str(level) for level in levels))
            raise QueueException(f"Unknown AMQP job priority {value!r}. Valid: {valid}")
        return value

    def _message_priority(self, job: Any, options: dict[str, Any]) -> int:
        name = self._priority_name(job, options)
        max_priority = int(options.get("max_priority"))
        if name.isdigit():
            value = int(name)
        else:
            value = (options.get("priority_levels") or {}).get(name)
        if isinstance(value, bool) or not isinstance(value, int):
            raise QueueException(f"AMQP priority {name!r} has no integer mapping.")
        if not 0 <= value <= max_priority:
            raise QueueException(
                f"AMQP priority {name!r}={value} exceeds queue max {max_priority}."
            )
        return value

    # ── Pool helpers ───────────────────────────────────────────────
    def _open_new_connection(self, opts: dict[str, Any]) -> tuple:
        """Open a brand-new connection + channel pair."""
        try:
            import pika
        except ImportError:
            raise QueueDriverLibraryNotFoundException(
                "pika is required for AMQPDriver. Install with: pip install pika"
            )

        connection = pika.BlockingConnection(self._connection_parameters(opts))
        channel = connection.channel()
        channel.confirm_delivery()
        return connection, channel

    def _acquire_thread_connection(self, url: str, opts: dict[str, Any]) -> None:
        """Bind a connection + channel to this thread for the publish.

        Reuse only the current thread's own connection. Pika
        BlockingConnection objects are owner-affine and must never cross
        thread boundaries.
        """
        if self.connection is not None and self.channel is not None:
            # Already bound on this thread (typical case for hot
            # publishers reusing the same pika channel).
            try:
                if self.connection.is_open and self.channel.is_open:
                    return
            except OSError, ConnectionError, RuntimeError, AttributeError:
                pass
            # Stale handle — drop it and fall through.
            self._discard_thread_connection()

        # No healthy owner-local handle — open a fresh connection.
        self.connection, self.channel = self._open_new_connection(opts)

    def _return_thread_connection(self, url: str) -> None:
        """Keep the healthy connection bound to its owner thread."""
        return

    def _discard_thread_connection(self) -> None:
        """Drop the thread-local connection without returning it to
        the pool. Used after a publish error."""
        conn, chan = self.connection, self.channel
        self.connection = None
        self.channel = None
        for handle in (chan, conn):
            try:
                if handle is not None:
                    handle.close()
            except (
                OSError,
                ConnectionError,
                RuntimeError,
                AttributeError,
                pika.exceptions.AMQPError,
            ):
                # Best-effort discard: closing an ALREADY-closed channel /
                # connection makes pika raise ``ChannelWrongStateError`` /
                # ``ConnectionWrongStateError`` (both ``AMQPError`` subclasses).
                # The whole point here is to drop a dead handle, so a
                # "already closed" close is success, not a failure to surface.
                pass

    def _connect(self, opts: dict[str, Any]) -> None:
        """Bind a connection + channel to this thread.

        Kept for read-only DLQ inspection. Runtime topology mutation belongs
        exclusively to the deploy-time ``queue:reconcile`` command.
        """
        if self.connection is not None and self.channel is not None:
            try:
                if self.connection.is_open and self.channel.is_open:
                    return
            except OSError, ConnectionError, RuntimeError, AttributeError:
                pass
        self.connection, self.channel = self._open_new_connection(opts)

        # NOTE: Queue declaration is intentionally NOT done here.
        # Each caller (_connect_and_publish, setup_dead_letter_exchange, etc.)
        # declares its target queue with the correct arguments (x-message-ttl,
        # x-dead-letter-exchange, ...). Declaring here without arguments
        # conflicted with existing queues and caused PRECONDITION_FAILED
        # (inequivalent arg 'x-message-ttl') on reconnects.

    def _build_url(self, opts: dict[str, Any]) -> str:
        """Build AMQP connection URL with proper encoding."""
        from urllib.parse import quote_plus

        connection_params = {
            "username": opts.get("username", ""),
            "password": opts.get("password", ""),
            "host": opts.get("host", "localhost"),
            "port": opts.get("port", 5672),
            "vhost": opts.get("vhost", "/"),
        }

        # URL encode username and password (handles special characters like *, #, %, etc.)
        encoded_username = quote_plus(connection_params["username"])
        encoded_password = quote_plus(connection_params["password"])

        # Encode vhost (/ becomes %2F)
        encoded_vhost = (
            "%2F"
            if not connection_params["vhost"] or connection_params["vhost"] == "/"
            else connection_params["vhost"].replace("/", "%2F")
        )

        scheme = str(opts.get("scheme", "amqp") or "amqp").lower()
        if scheme not in {"amqp", "amqps"}:
            raise QueueException("AMQP scheme must be 'amqp' or 'amqps'.")

        base_url = (
            f"{scheme}://{encoded_username}:{encoded_password}"
            f"@{connection_params['host']}:{connection_params['port']}/{encoded_vhost}"
        )

        # Append connection options if present
        connection_options = opts.get("connection_options")
        if connection_options:
            from urllib.parse import urlencode

            return f"{base_url}?{urlencode(connection_options)}"

        return base_url

    def _connection_parameters(self, opts: dict[str, Any]):
        """Build pika parameters, including verified TLS and optional mTLS."""
        if pika is None:
            raise QueueDriverLibraryNotFoundException(
                "pika is required for AMQPDriver. Install with: pip install pika"
            )

        parameters = pika.URLParameters(self._build_url(opts))
        parameters.connection_attempts = 1
        parameters.retry_delay = 0
        parameters.socket_timeout = float(opts.get("socket_timeout_seconds", 5))
        parameters.stack_timeout = float(opts.get("stack_timeout_seconds", 10))
        parameters.blocked_connection_timeout = float(
            opts.get("blocked_connection_timeout_seconds", 10)
        )
        parameters.heartbeat = int(opts.get("heartbeat_seconds", 60))
        scheme = str(opts.get("scheme", "amqp") or "amqp").lower()
        if scheme != "amqps":
            return parameters

        context = ssl.create_default_context(cafile=opts.get("ssl_ca_certs") or None)
        certfile = opts.get("ssl_certfile")
        keyfile = opts.get("ssl_keyfile")
        if bool(certfile) != bool(keyfile):
            raise QueueException(
                "RABBIT_SSL_CERTFILE and RABBIT_SSL_KEYFILE must be configured together."
            )
        if certfile and keyfile:
            context.load_cert_chain(certfile=certfile, keyfile=keyfile)
        context.check_hostname = True
        context.verify_mode = ssl.CERT_REQUIRED
        parameters.ssl_options = pika.SSLOptions(
            context,
            str(opts.get("host", "localhost")),
        )
        return parameters
