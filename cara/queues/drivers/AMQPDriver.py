"""
AMQP Queue Driver for the Cara framework.

Modern, clean implementation for RabbitMQ-based job queue management.
"""

from __future__ import annotations

import logging
import threading
import time
import uuid
from typing import Any

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
from cara.queues.contracts.QueueContract import QueueContract as _QueueContract
from cara.queues.delay import DurableDelayedJobStore
from cara.queues.delivery import QueueJobDeliveryStore
from cara.queues.QueueState import DEAD_LETTER_EXCHANGE, DEAD_LETTER_QUEUE
from cara.queues.retry.Policy import (
    DEFAULT_MAX_ATTEMPTS as _RETRY_DEFAULT_MAX_ATTEMPTS,
)
from cara.queues.retry.Policy import (
    DEFAULT_RETRY_BACKOFF_SECONDS as _RETRY_DEFAULT_BACKOFF_SECONDS,
)
from cara.queues.retry.Policy import (
    DEFAULT_RETRY_JITTER_FRACTION as _RETRY_DEFAULT_JITTER_FRACTION,
)
from cara.support import HasColoredOutput

from . import _AMQPBroker, _AMQPDelivery

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


class AMQPDriver(HasColoredOutput, _QueueContract):
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
                    ("exchange", DEAD_LETTER_EXCHANGE),
                    ("queue", DEAD_LETTER_QUEUE),
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

    @property
    def delivery_store(self) -> QueueJobDeliveryStore:
        return self._delivery_store

    @property
    def canonical_queues(self) -> frozenset[str]:
        """The queue inventory this driver accepts dispatches for.

        The same SSOT ``require_canonical_queue`` validates against, exposed so
        broker maintenance (reconcile, flush) reads one inventory instead of
        re-deriving its own from configuration and drifting from it.
        """
        return self._canonical_queues

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

    # ── Pool helpers ───────────────────────────────────────────────

    # NOTE: Queue declaration is intentionally NOT done here.
    # Each caller (_connect_and_publish, and the topology helpers in
    # ``cara.queues.QueueState`` that ``queue:topology`` drives)
    # declares its target queue with the correct arguments (x-message-ttl,
    # x-dead-letter-exchange, ...). Declaring here without arguments
    # conflicted with existing queues and caused PRECONDITION_FAILED
    # (inequivalent arg 'x-message-ttl') on reconnects.

    _apply_retry_jitter = _AMQPDelivery._amqp_delivery_apply_retry_jitter
    _create_job_record = _AMQPDelivery._amqp_delivery_create_job_record
    _publish_registered_envelope = (
        _AMQPDelivery._amqp_delivery_publish_registered_envelope
    )
    _register_immediate_delivery = (
        _AMQPDelivery._amqp_delivery_register_immediate_delivery
    )
    _resolve_job_tracker = _AMQPDelivery._amqp_delivery_resolve_job_tracker
    _serialize_payload = _AMQPDelivery._amqp_delivery_serialize_payload
    _tenant_payload = staticmethod(_AMQPDelivery._amqp_delivery_tenant_payload)
    batch = _AMQPDelivery._amqp_delivery_batch
    chain = _AMQPDelivery._amqp_delivery_chain
    defer_terminal_hook_process_failure = (
        _AMQPDelivery._amqp_delivery_defer_terminal_hook_process_failure
    )
    dispatch_due_delayed_jobs = _AMQPDelivery._amqp_delivery_dispatch_due_delayed_jobs
    due_terminal_hook_ids = _AMQPDelivery._amqp_delivery_due_terminal_hook_ids
    invalidate_runtime_health = _AMQPDelivery._amqp_delivery_invalidate_runtime_health
    later = _AMQPDelivery._amqp_delivery_later
    process_terminal_hook = _AMQPDelivery._amqp_delivery_process_terminal_hook
    push = _AMQPDelivery._amqp_delivery_push
    refresh_delayed_job_metrics = _AMQPDelivery._amqp_delivery_refresh_delayed_job_metrics
    refresh_delivery_metrics = _AMQPDelivery._amqp_delivery_refresh_delivery_metrics
    relay_publish_once = _AMQPDelivery._amqp_delivery_relay_publish_once
    retry_quarantined_terminal_hooks = (
        _AMQPDelivery._amqp_delivery_retry_quarantined_terminal_hooks
    )
    schedule = _AMQPDelivery._amqp_delivery_schedule
    wake_outbox_relay = _AMQPDelivery._amqp_delivery_wake_outbox_relay

    _acquire_thread_connection = _AMQPBroker._amqp_broker_acquire_thread_connection
    _bounded_queue_argument = staticmethod(
        _AMQPBroker._amqp_broker_bounded_queue_argument
    )
    _build_url = _AMQPBroker._amqp_broker_build_url
    _connect = _AMQPBroker._amqp_broker_connect
    _connection_parameters = _AMQPBroker._amqp_broker_connection_parameters
    _discard_thread_connection = _AMQPBroker._amqp_broker_discard_thread_connection
    _message_priority = _AMQPBroker._amqp_broker_message_priority
    maintain_publisher_connection = _AMQPBroker._amqp_broker_maintain_publisher_connection
    _open_new_connection = _AMQPBroker._amqp_broker_open_new_connection
    _priority_name = _AMQPBroker._amqp_broker_priority_name
    _return_thread_connection = _AMQPBroker._amqp_broker_return_thread_connection
    canonical_queue_arguments = _AMQPBroker._amqp_broker_canonical_queue_arguments
    dead_letter_queue_arguments = _AMQPBroker._amqp_broker_dead_letter_queue_arguments
    get_dead_letter_messages = _AMQPBroker._amqp_broker_get_dead_letter_messages
    replay_delivery = _AMQPBroker._amqp_broker_replay_delivery
