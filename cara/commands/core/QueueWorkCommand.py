"""
Queue Worker Command for the Cara framework.

This module provides a CLI command to process jobs from the queue with enhanced UX.
"""

from __future__ import annotations

import asyncio
import atexit
import builtins
import concurrent.futures
import contextlib
import inspect
import logging
import os
import signal
import sys
import threading
import time
import traceback
from collections import deque
from typing import Any

from cara.commands import CommandBase
from cara.commands.MakesAutoReload import MakesAutoReload
from cara.configuration import config
from cara.decorators import command
from cara.exceptions import (
    CaraException,
    ConfigurationException,
    InvalidArgumentException,
    QueueException,
)
from cara.facades import Log, Queue

# These live under ``cara.queues``. The package is import-safe WITHOUT the
# optional 'queue' extra (pika) — ``AMQPDriver`` degrades its pika import to
# ``None`` and re-checks at connection time — so importing them at module top
# no longer forces every service to install pika just to load the command
# package. A worker that actually runs still needs pika and fails LOUD when it
# opens a connection.
from cara.queues.JobInstantiation import instantiate_job
from cara.queues.PayloadLimits import MAX_AMQP_JOB_PAYLOAD_BYTES
from cara.queues.retry.Policy import (
    DEFAULT_MAX_ATTEMPTS as _RETRY_DEFAULT_MAX_ATTEMPTS,
)
from cara.queues.retry.Policy import (
    DEFAULT_RETRY_BACKOFF_SECONDS as _RETRY_DEFAULT_BACKOFF_SECONDS,
)
from cara.queues.serializers.SignedJsonJobSerializer import (
    SignedJsonJobSerializer,
)

# Prometheus metrics — framework-owned MetricsBase carries the queue/worker
# metrics. Guarded so a partial import never breaks the worker.
try:
    from cara.observability.Metrics import MetricsBase
except (ImportError, RuntimeError):  # pragma: no cover
    MetricsBase = None  # type: ignore[assignment]

_logger = logging.getLogger("cara.queue.worker")


class DeliverySettlementError(RuntimeError):
    """Execution finished but durable delivery settlement did not."""


def _queue_label(
    msg: dict | None, instance: Any = None, queue_name: str | None = None
) -> str:
    """Best-effort queue label for the current message (bounded cardinality).

    Resolution order:
      1. ``queue_name`` arg — the queue the worker just polled from
         (highest fidelity — this is exactly where the message was consumed).
      2. ``msg["queue"]`` / ``msg["routing_key"]`` — the producer-side hint.
      3. ``instance.queue`` — the job's own class-level queue attribute.
    """
    if queue_name:
        return str(queue_name)
    if isinstance(msg, dict):
        q = msg.get("queue") or msg.get("routing_key")
        if q:
            return str(q)
    if instance is not None and hasattr(instance, "queue"):
        q = getattr(instance, "queue", None)
        if q:
            return str(q)
    return "unknown"


def _job_label(instance: Any, msg: dict | None) -> str:
    """Class-name label for the running job."""
    if instance is not None:
        return instance.__class__.__name__
    if isinstance(msg, dict):
        obj_ref = msg.get("obj")
        if isinstance(obj_ref, str):
            return obj_ref.rsplit(".", 1)[-1] or "unknown"
    return "unknown"


# Silence pika's remote Channel.Close (404) warnings — worker polls a
# superset of queue names via wildcards, so "queue doesn't exist" on a
# passive declare is expected for empty queues. The worker already caches
# the miss in ``_missing_queues``; pika still logs each channel close at
# WARNING level on the underlying logger, which spams the console every
# retry tick. Silencing here keeps the worker's own log line ("No job
# found" / job output) readable.
for _pika_logger in (
    "pika",
    "pika.channel",
    "pika.connection",
    "pika.adapters.blocking_connection",
    "pika.adapters.utils.connection_workflow",
    "pika.adapters.utils.io_services_utils",
):
    logging.getLogger(_pika_logger).setLevel(logging.CRITICAL)


class AMQPConnectionManager:
    """Manages AMQP connections for queue workers (Single Responsibility)."""

    def __init__(self, config_func, driver=None):
        self.config = config_func
        self.driver = driver
        self.connection = None

    def ensure_connection(self) -> bool:
        """Ensure AMQP connection is alive.

        Treats any prior operational failure (``StreamLostError``,
        ``ConnectionClosedByBroker``, TCP RST during a long job)
        as "connection is dead" even when ``is_closed`` still reports
        False. pika's BlockingConnection occasionally keeps a zombie
        connection object after the underlying stream dies; the next
        ``channel()`` call then explodes with the original
        ``StreamLostError`` instead of transparently reconnecting.
        A fresh heartbeat probe rules that out.
        """
        try:
            if self.connection is not None and not self.connection.is_closed:
                try:
                    # Cheap liveness probe — pika doesn't expose a
                    # dedicated ``ping``; dispatching data events
                    # triggers a heartbeat exchange and surfaces a
                    # stale connection as an exception here rather
                    # than much later in the consumer loop.
                    self.connection.process_data_events(time_limit=0)
                except Exception:
                    with contextlib.suppress(
                        OSError, RuntimeError, AttributeError, ConnectionError
                    ):
                        self.connection.close()
                    self.connection = None

            if self.connection is None or self.connection.is_closed:
                self.connection = self._create_connection()
            return True
        except Exception as e:
            try:
                from cara.facades import Log

                Log.error("Failed to connect to RabbitMQ: %s", e, exc_info=True)
            except (ImportError, RuntimeError):
                import sys

                print(
                    f"[QueueWorkCommand] Failed to connect to RabbitMQ: {e}",
                    file=sys.stderr,
                )
            self.connection = None
            return False

    def _create_connection(self):
        """Create new AMQP connection."""
        import pika

        if self.driver is not None and hasattr(self.driver, "_connection_parameters"):
            parameters = self.driver._connection_parameters(self.driver.options)
        else:
            credentials = pika.PlainCredentials(
                self.config("queue.drivers.amqp.username"),
                self.config("queue.drivers.amqp.password"),
            )
            parameters = pika.ConnectionParameters(
                host=self.config("queue.drivers.amqp.host"),
                port=self.config("queue.drivers.amqp.port", 5672),
                virtual_host=self.config("queue.drivers.amqp.vhost", "/"),
                credentials=credentials,
                heartbeat=int(
                    self.config(
                        "queue.drivers.amqp.heartbeat_seconds",
                        60,
                    )
                ),
                blocked_connection_timeout=float(
                    self.config(
                        "queue.drivers.amqp.blocked_connection_timeout_seconds",
                        10,
                    )
                ),
                socket_timeout=float(
                    self.config(
                        "queue.drivers.amqp.socket_timeout_seconds",
                        5,
                    )
                ),
                stack_timeout=float(
                    self.config(
                        "queue.drivers.amqp.stack_timeout_seconds",
                        10,
                    )
                ),
            )
        return pika.BlockingConnection(parameters)

    def create_channel(self):
        """Create fresh channel for queue operations."""
        if not self.ensure_connection():
            return None
        return self.connection.channel()

    def close(self):
        """Clean up connection."""
        if self.connection and not self.connection.is_closed:
            with contextlib.suppress(ImportError, RuntimeError, AttributeError, OSError):
                self.connection.close()


class ThreadSafeAMQPAckChannel:
    """Expose ACK/NACK to job threads without touching pika off its I/O thread."""

    def __init__(
        self,
        connection,
        channel,
        timeout_seconds: int = 30,
        on_settled=None,
    ):
        self._connection = connection
        self._channel = channel
        self._timeout_seconds = timeout_seconds
        self._on_settled = on_settled
        self._settled = False
        self._settled_lock = threading.Lock()

    def basic_ack(self, *, delivery_tag) -> None:
        self._schedule(
            lambda: self._channel.basic_ack(delivery_tag=delivery_tag),
            operation="ACK",
        )

    def basic_nack(self, *, delivery_tag, requeue: bool) -> None:
        self._schedule(
            lambda: self._channel.basic_nack(
                delivery_tag=delivery_tag,
                requeue=requeue,
            ),
            operation="NACK",
        )

    def _schedule(self, callback, *, operation: str) -> None:
        completed = threading.Event()
        errors: list[BaseException] = []

        def _run() -> None:
            try:
                callback()
                with self._settled_lock:
                    if self._settled:
                        raise RuntimeError(
                            "RabbitMQ delivery was settled more than once."
                        )
                    self._settled = True
                if self._on_settled is not None:
                    self._on_settled()
            except BaseException as exc:
                errors.append(exc)
            finally:
                completed.set()

        if self._connection is None or self._connection.is_closed:
            raise ConnectionError(f"RabbitMQ connection closed before {operation}")
        self._connection.add_callback_threadsafe(_run)
        if not completed.wait(self._timeout_seconds):
            raise TimeoutError(
                f"RabbitMQ {operation} was not processed within "
                f"{self._timeout_seconds} seconds"
            )
        if errors:
            raise errors[0]


class ActiveJobCancellationRegistry:
    """Thread-safe registry of async jobs that can be cancelled on shutdown.

    A queue worker owns one registry and shares it with every consumer slot.
    The main thread never touches a consumer's event loop directly; it asks
    that loop to cancel its registered task with ``call_soon_threadsafe``.
    Synchronous handlers are intentionally absent because Python cannot safely
    interrupt a running thread — the worker's bounded hard-exit path handles
    those by letting the broker redeliver their unacknowledged messages.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._tasks: dict[int, tuple[asyncio.AbstractEventLoop, asyncio.Task]] = {}

    def register_current(self) -> int:
        """Register the current asyncio task and return its removal token."""
        loop = asyncio.get_running_loop()
        task = asyncio.current_task()
        if task is None:  # pragma: no cover - asyncio always supplies one here
            raise RuntimeError("No current asyncio task to register")
        token = id(task)
        with self._lock:
            self._tasks[token] = (loop, task)
        return token

    def unregister(self, token: int) -> None:
        with self._lock:
            self._tasks.pop(token, None)

    def cancel_all(self) -> int:
        """Request cancellation on every active job's owning event loop."""
        with self._lock:
            tasks = list(self._tasks.values())

        requested = 0
        for loop, task in tasks:
            if task.done():
                continue
            try:
                loop.call_soon_threadsafe(task.cancel)
                requested += 1
            except RuntimeError:
                # The consumer completed and closed its loop between the
                # snapshot and this call. Its registry ``finally`` will remove
                # the stale entry; there is nothing left to cancel.
                continue
        return requested


class JobProcessor:
    """Processes individual jobs from queue messages (Single Responsibility)."""

    # Class-level constants for job execution
    DEFAULT_JOB_TIMEOUT = 300
    MAX_PAYLOAD_SIZE = MAX_AMQP_JOB_PAYLOAD_BYTES
    _SETTLEMENT_BACKOFF_SECONDS = (0.05, 0.25, 1.0, 2.0, 5.0)

    @staticmethod
    def _broker_ack(channel, delivery_tag) -> None:
        try:
            channel.basic_ack(delivery_tag=delivery_tag)
        except Exception as exc:
            raise DeliverySettlementError("Broker ACK outcome is unknown.") from exc

    @staticmethod
    def _broker_nack(channel, delivery_tag, *, requeue: bool) -> None:
        try:
            channel.basic_nack(delivery_tag=delivery_tag, requeue=requeue)
        except Exception as exc:
            raise DeliverySettlementError("Broker NACK outcome is unknown.") from exc

    @staticmethod
    def _retry_settlement_step(description: str, callback) -> None:
        last_error: Exception | None = None
        for attempt, delay in enumerate(
            (0.0, *JobProcessor._SETTLEMENT_BACKOFF_SECONDS),
            start=1,
        ):
            if delay:
                time.sleep(delay)
            try:
                callback()
                return
            except Exception as exc:
                last_error = exc
                if attempt <= len(JobProcessor._SETTLEMENT_BACKOFF_SECONDS):
                    Log.warning(
                        "%s failed on settlement attempt %s; retrying: %s",
                        description,
                        attempt,
                        exc,
                        category="cara.queue.delivery",
                    )
        raise DeliverySettlementError(
            f"{description} remained unavailable."
        ) from last_error

    def __init__(
        self,
        cancellation_registry: ActiveJobCancellationRegistry | None = None,
    ) -> None:
        self.cancellation_registry = cancellation_registry

    @staticmethod
    def _execute_async_job_with_timeout(
        method_to_call,
        init_args,
        timeout_seconds,
        cancellation_registry: ActiveJobCancellationRegistry | None = None,
    ):
        """Execute async job with timeout enforcement.

        Wraps the coroutine in ``asyncio.wait_for(...)`` so a hung
        ``await`` (a stuck HTTP call without its own timeout, a DB
        operation behind a dead pool) actually surfaces as a
        ``TimeoutError`` after ``timeout_seconds``. Pre-fix this
        helper accepted ``timeout_seconds`` but only used it in the
        exception message — ``asyncio.run(method_to_call(...))`` ran
        without a cap, so an async job that genuinely hung held its
        worker slot indefinitely. The active worker dispatch path
        (``handle`` body) already wraps in ``wait_for``; this static
        is here so anyone reaching for it (debug shim, alternative
        worker mode, unit test) gets the same contract.
        """

        async def _run_registered():
            token = (
                cancellation_registry.register_current()
                if cancellation_registry is not None
                else None
            )
            try:
                return await asyncio.wait_for(
                    method_to_call(*init_args), timeout=timeout_seconds
                )
            finally:
                if token is not None:
                    cancellation_registry.unregister(token)

        hard_kill = threading.Timer(
            float(timeout_seconds) + 5.0,
            JobProcessor._hard_kill_uncooperative_timeout,
            kwargs={"timeout_seconds": timeout_seconds},
        )
        hard_kill.daemon = True
        hard_kill.start()
        try:
            return asyncio.run(_run_registered())
        except TimeoutError as e:
            raise TimeoutError(f"Async job exceeded timeout of {timeout_seconds}s") from e
        finally:
            hard_kill.cancel()

    @staticmethod
    def _hard_kill_uncooperative_timeout(*, timeout_seconds: float) -> None:
        """Kill the worker if coroutine cancellation cannot stop the handler."""
        Log.error(
            "Queue handler ignored cancellation after its %ss timeout; "
            "terminating the worker before its DB lease can be recovered.",
            timeout_seconds,
            category="cara.queue.delivery",
        )
        os._exit(getattr(os, "EX_TEMPFAIL", 75))

    # Framework-default retry policy used when the failing job does not
    # declare its own ``max_attempts`` / ``retry_backoff``. SINGLE-SOURCED
    # from ``cara.queues.retry.Policy`` so this production worker and
    # ``AMQPDriver`` can no longer drift apart — they previously kept
    # hand-copied constants "in lockstep" by comment only.
    DEFAULT_MAX_ATTEMPTS = _RETRY_DEFAULT_MAX_ATTEMPTS
    DEFAULT_RETRY_BACKOFF_SECONDS = _RETRY_DEFAULT_BACKOFF_SECONDS

    @staticmethod
    def _should_retry_job(msg, instance) -> bool:
        """Decide whether a failed message should be republished with a delay.

        ``msg["attempts"]`` is the *attempts-already-made* counter
        (AMQPDriver.push stamps it 0; each retry republish bumps it).
        The cap is whatever the job class declares via ``max_attempts``
        (default :data:`DEFAULT_MAX_ATTEMPTS`).

        Pre-fix this read ``msg["attempt"]`` (singular — a key nothing
        ever set) and compared it to ``msg["attempts"]`` as if that
        held the cap, so the comparison was always ``1 < 0`` → False
        and every first failure was ACKed straight to the DLQ, bypassing
        the worker's retry schedule.
        """
        if not msg:
            return False
        try:
            attempts_done = int(
                msg.get("attempts", 0) if msg.get("attempts", 0) is not None else 0
            )
        except (TypeError, ValueError):
            attempts_done = 0
        max_attempts = int(
            getattr(instance, "max_attempts", None) or JobProcessor.DEFAULT_MAX_ATTEMPTS
        )
        # ``do_not_retry`` on the failing exception is honoured one
        # level up (see _requeue_with_delay) — we only answer the
        # "budget remaining" question here.
        return attempts_done + 1 < max_attempts

    @staticmethod
    def _requeue_with_delay(
        channel,
        method_frame,
        msg,
        instance,
        exc: Exception,
        queue_name: str | None,
        delivery_lease_token: str | None,
        tracker,
        db_job_id: int,
    ) -> None:
        """Atomically accept a new delayed delivery, then ACK the source.

        ``basic_nack(requeue=True)`` puts the message back on the queue
        head immediately. With ``prefetch=1`` the same worker thread
        re-claims it on the very next iteration — a poison message
        loops at 100% CPU. The Cara contract is ``republish-with-
        backoff`` (1s / 5s / 30s by default, jittered), which only
        works when we persist the retry in the durable delayed-job
        outbox. We ACK the original delivery only after that database
        commit and stamp the new message with
        ``attempts = attempts_done + 1`` so the next failure can decide
        budget correctly.
        """
        # The delivery ledger commits the new row and the source
        # ``retry_scheduled`` terminal transition in ONE DB transaction. Only
        # after that durable acceptance may the broker source be ACKed.
        attempts_done = int(
            msg.get("attempts", 0) if msg.get("attempts", 0) is not None else 0
        )
        # Throttle-class exceptions (``ConcurrencyExceeded`` raised by
        # the ``ConcurrencyLimited`` middleware, future per-host rate-
        # limit middleware) signal "the job never got a slot, try again
        # later" — not a job failure. Bumping ``attempts`` for these
        # would DLQ healthy jobs purely from losing the slot lottery.
        # The middleware contract is the source of truth (it declares
        # ``is_throttle = True`` on the exception class); we read it
        # via ``getattr`` so the check stays loose-coupled and future
        # throttle classes opt in for free.
        is_throttle = bool(getattr(exc, "is_throttle", False))
        next_attempt = attempts_done if is_throttle else attempts_done + 1

        backoff_schedule = getattr(
            instance,
            "retry_backoff",
            JobProcessor.DEFAULT_RETRY_BACKOFF_SECONDS,
        )
        if not isinstance(backoff_schedule, (list, tuple)) or not backoff_schedule:
            backoff_schedule = JobProcessor.DEFAULT_RETRY_BACKOFF_SECONDS
        idx = min(attempts_done, len(backoff_schedule) - 1)
        base_delay = int(backoff_schedule[idx])

        try:
            from cara.facades import Queue as _Queue

            driver = _Queue.driver()
            # ``AMQPDriver`` exposes ``_apply_retry_jitter`` for the
            # full-jitter spread; fall back to the unjittered delay if
            # we're running against a different driver.
            delay_seconds = base_delay
            apply_jitter = getattr(driver, "_apply_retry_jitter", None)
            if callable(apply_jitter):
                try:
                    delay_seconds = apply_jitter(base_delay, instance)
                except Exception:
                    delay_seconds = base_delay

            retry_options = {
                "queue": queue_name or msg.get("queue") or "default",
                "attempts": next_attempt,
                "_otel": msg.get("_otel") or {},
                "db_job_id": msg.get("db_job_id"),
                "source_delivery_job_id": msg.get("job_id"),
                "source_delivery_lease_token": delivery_lease_token,
                "deduplication_key": (f"retry:{msg.get('job_id')}:{next_attempt}"),
                "unique_key": msg.get("unique_key"),
            }
            if msg.get("_tenant_mode") == "tenant":
                retry_options["tenant_id"] = msg.get("_tenant")
            # Carry the original ``callback`` / ``args`` through to the
            # republished payload. Pre-fix the retry options ONLY held
            # ``queue`` and ``attempts``, so AMQPDriver.push fell back
            # to defaults (``callback="handle"``, ``args=()``). Any job
            # dispatched with ``Bus.dispatch(job, callback="custom",
            # args=(123,))`` retried against ``handle()`` with no args
            # — silent semantic drift on every retry path. Only
            # propagate keys the original payload actually set so we
            # don't override driver defaults with empty values.
            if "callback" in msg:
                retry_options["callback"] = msg["callback"]
            if "args" in msg:
                retry_options["args"] = msg["args"]
            if "created" in msg:
                retry_options["created"] = msg["created"]
            # ``later`` is the Laravel-compatible delay entry point. The AMQP
            # driver commits a signed row to PostgreSQL; only then may this
            # worker acknowledge the failed source delivery.
            if msg.get("_tenant_mode") == "central":
                from cara.context import Tenancy

                with Tenancy.central():
                    _Queue.later(delay_seconds, instance, **retry_options)
            else:
                from cara.context import Tenancy

                with Tenancy.as_tenant(msg.get("_tenant")):
                    _Queue.later(delay_seconds, instance, **retry_options)
            Log.info(
                "↻ Durable retry scheduled for %s (attempt %s, +%ss, reason=%s)",
                instance.__class__.__name__,
                next_attempt,
                delay_seconds,
                type(exc).__name__,
            )
            JobProcessor._retry_settlement_step(
                f"Tracked queue job {db_job_id} retry settlement",
                lambda: tracker.require_job_status_strict(
                    db_job_id,
                    "retrying",
                ),
            )
            # Retry is durably accepted in the DB outbox and source settlement
            # committed atomically — ACK the original now. Broker publication
            # may happen later via the reconciler without changing semantics.
            JobProcessor._broker_ack(channel, method_frame.delivery_tag)
        except Exception as republish_err:
            Log.error(
                "Retry republish failed for %s: %s. "
                "Leaving the original delivery unacknowledged for broker "
                "redelivery.",
                instance.__class__.__name__,
                republish_err,
                exc_info=True,
            )
            raise

    @staticmethod
    def _route_failed_message(
        *,
        channel,
        method_frame,
        msg,
        instance,
        exc: Exception,
        queue_name: str | None,
        delivery_store,
        delivery_lease_token: str | None,
        tracker,
        db_job_id: int,
    ) -> str:
        """Single failure router: retry-with-delay OR dead-letter.

        Centralises three rules that the two ``except`` branches
        previously duplicated and routinely diverged on:

        * ``do_not_retry`` exceptions skip straight to DLQ — no point
          burning the backoff budget on a 404 that won't come back.
        * Retry settlement transactionally transfers the database uniqueness
          fence from the processing source to its delayed child.
        * Terminal failure removes the row from the open-delivery index, so a
          later legitimate dispatch can proceed.
        """
        do_not_retry = bool(getattr(exc, "do_not_retry", False))
        can_retry = (
            msg
            and instance is not None
            and not do_not_retry
            and JobProcessor._should_retry_job(msg, instance)
        )

        if can_retry:
            if delivery_store is None or delivery_lease_token is None:
                raise QueueException(
                    "Cannot retry a queue delivery without a durable execution lease."
                )
            JobProcessor._requeue_with_delay(
                channel=channel,
                method_frame=method_frame,
                msg=msg,
                instance=instance,
                exc=exc,
                queue_name=queue_name,
                delivery_lease_token=delivery_lease_token,
                tracker=tracker,
                db_job_id=db_job_id,
            )
            # The ledger transaction moved the uniqueness fence from the
            # processing source to the delayed retry child.
            return "retry_scheduled"

        # Terminal — give up the slot.
        JobProcessor._ack_to_dlq(
            channel,
            method_frame,
            msg,
            str(exc),
            instance=instance,
            delivery_store=delivery_store,
            delivery_lease_token=delivery_lease_token,
            tracker=tracker,
            db_job_id=db_job_id,
        )
        return "dead_lettered"

    @staticmethod
    def _ack_to_dlq(
        channel,
        method_frame,
        msg,
        error_msg,
        *,
        instance=None,
        delivery_store=None,
        delivery_lease_token: str | None = None,
        tracker=None,
        db_job_id: int | None = None,
    ):
        """Settle trusted failures in PostgreSQL; quarantine untrusted bytes."""
        queue_name = msg.get("queue", "unknown") if msg else "unknown"
        job_id = msg.get("job_id", "unknown") if msg else "unknown"
        Log.error(
            "Job dead-lettered: %s | Queue: %s | Error: %s",
            job_id,
            queue_name,
            error_msg,
        )
        if delivery_store is not None and delivery_lease_token is not None:
            if tracker is None or db_job_id is None:
                raise DeliverySettlementError(
                    "Terminal queue settlement requires a persistent tracker."
                )
            try:
                delivery_store.dead_letter_with_tracker(
                    str(job_id),
                    delivery_lease_token,
                    db_job_id=db_job_id,
                    reason=str(error_msg),
                )
            except Exception as exc:
                raise DeliverySettlementError(
                    "Could not atomically persist terminal queue failure."
                ) from exc
            JobProcessor._broker_ack(channel, method_frame.delivery_tag)
            return
        JobProcessor._broker_nack(channel, method_frame.delivery_tag, requeue=False)

    @staticmethod
    def process_message(
        channel,
        method_frame,
        body,
        queue_name: str | None = None,
        cancellation_registry: ActiveJobCancellationRegistry | None = None,
    ) -> bool:
        """Process a single queue message and return success status.

        ``queue_name`` is the queue the worker dequeued from. Used as
        the highest-fidelity label for Prometheus metrics — otherwise
        we'd have to infer the queue from producer payload metadata,
        which is less reliable than the broker delivery source.
        """
        # Start of job window — used across all exit paths below.
        _mx_start = time.time()
        _mx_queue = str(queue_name) if queue_name else "unknown"
        _mx_job = "unknown"
        _mx_inflight_entered = False

        def _mx_record(outcome: str) -> None:
            """Emit metrics for this job exit. Safe to call multiple times
            (we only set ``_mx_recorded`` once inside the closure)."""
            if MetricsBase is None:
                return
            nonlocal _mx_recorded
            if _mx_recorded:
                return
            _mx_recorded = True
            try:
                MetricsBase.queue_jobs_consumed_total.labels(
                    queue=_mx_queue,
                    job_class=_mx_job,
                    outcome=outcome,
                ).inc()
                MetricsBase.queue_job_duration_seconds.labels(
                    queue=_mx_queue,
                    job_class=_mx_job,
                ).observe(time.time() - _mx_start)
                if _mx_inflight_entered:
                    MetricsBase.queue_jobs_in_flight.labels(
                        queue=_mx_queue,
                        job_class=_mx_job,
                    ).dec()
            except (ImportError, RuntimeError, AttributeError, OSError):
                pass

        _mx_recorded = False

        # Bound parser work before JSON/signature verification.
        if len(body) > JobProcessor.MAX_PAYLOAD_SIZE:
            Log.error(
                "❌ Payload exceeds max size (%s > %s)",
                len(body),
                JobProcessor.MAX_PAYLOAD_SIZE,
            )
            JobProcessor._broker_nack(
                channel,
                method_frame.delivery_tag,
                requeue=False,
            )
            _mx_record("oversized")
            return False

        # Resolve app and tracker outside try block for exception handler access
        app_instance = builtins.app() if hasattr(builtins, "app") else None
        tracker = None
        if app_instance and app_instance.has("JobTracker"):
            tracker = app_instance.make("JobTracker")

        msg = None
        instance = None
        db_job_id = None
        delivery_store = None
        delivery_lease_token = None
        terminal_outcome = None

        try:
            envelope = SignedJsonJobSerializer.inspect_envelope(
                body,
                signing_keys=config("queue.drivers.amqp.signing_keys", {}),
                clock_skew_seconds=int(
                    config("queue.drivers.amqp.clock_skew_seconds", 30)
                ),
                max_age_seconds=int(
                    config(
                        "queue.drivers.amqp.envelope_max_age_seconds",
                        SignedJsonJobSerializer.DEFAULT_MAX_AGE_SECONDS,
                    )
                ),
                allow_not_before=True,
                allow_expired=True,
            )
            verified_payload = envelope["payload"]
            queue_driver = Queue.driver("amqp")
            delivery_store = queue_driver.delivery_store
            claim = delivery_store.claim_execution(
                body=body,
                payload=verified_payload,
            )
            if claim.outcome == "retry_scheduled":
                retry_db_job_id = verified_payload.get("db_job_id")
                if tracker is None or retry_db_job_id is None:
                    raise DeliverySettlementError(
                        "Retry settlement requires a persistent JobTracker fence."
                    )
                JobProcessor._retry_settlement_step(
                    f"Tracked queue job {retry_db_job_id} retry recovery",
                    lambda: tracker.ensure_retry_progress_strict(retry_db_job_id),
                )
                JobProcessor._broker_ack(channel, method_frame.delivery_tag)
                _mx_record(claim.outcome)
                return True
            if claim.outcome in {"completed", "dead_lettered", "expired"}:
                terminal_outcome = claim.outcome
            elif claim.outcome in {"live_lease", "not_ready"}:
                # PostgreSQL owns crash/early-publication recovery. Repeatedly
                # closing a quorum-queue channel would burn RabbitMQ's delivery
                # limit while the DB lease is still live, eventually DLQ'ing the
                # only broker copy. ``live_lease`` is recovered and republished
                # by the relay once stale; ``not_ready`` was reset to its DB
                # outbox timestamp inside claim_execution().
                JobProcessor._broker_ack(channel, method_frame.delivery_tag)
                _mx_record(claim.outcome)
                return True
            elif claim.outcome in {"unknown", "mismatch"}:
                JobProcessor._broker_nack(
                    channel,
                    method_frame.delivery_tag,
                    requeue=False,
                )
                _mx_record(f"ledger_{claim.outcome}")
                return False
            elif claim.outcome == "claimed" and claim.lease_token:
                delivery_lease_token = claim.lease_token
            elif terminal_outcome is None:
                raise QueueException(
                    f"Unsupported delivery ledger claim outcome: {claim.outcome}."
                )

            # Keep the verified primitives available to the failure router if
            # class resolution/constructor validation fails after the lease.
            msg = dict(verified_payload)
            if queue_name and verified_payload.get("queue") != queue_name:
                if terminal_outcome is None:
                    JobProcessor._ack_to_dlq(
                        channel,
                        method_frame,
                        msg,
                        (
                            f"signed queue {verified_payload.get('queue')!r} "
                            f"does not match delivery queue {queue_name!r}"
                        ),
                        delivery_store=delivery_store,
                        delivery_lease_token=delivery_lease_token,
                        tracker=tracker,
                        db_job_id=verified_payload.get("db_job_id"),
                    )
                else:
                    JobProcessor._broker_ack(channel, method_frame.delivery_tag)
                _mx_record("queue_mismatch")
                return False

            if terminal_outcome is not None:
                terminal_db_job_id = verified_payload.get("db_job_id")
                if tracker is None or terminal_db_job_id is None:
                    raise DeliverySettlementError(
                        "Terminal queue recovery requires a persistent JobTracker fence."
                    )
                JobProcessor._retry_settlement_step(
                    f"Tracked queue job {terminal_db_job_id} terminal recovery",
                    lambda: delivery_store.reconcile_terminal_tracker(
                        str(verified_payload["job_id"]),
                        db_job_id=terminal_db_job_id,
                        delivery_status=terminal_outcome,
                    ),
                )
                JobProcessor._broker_ack(channel, method_frame.delivery_tag)
                _mx_record(terminal_outcome)
                return terminal_outcome == "completed"

            msg = SignedJsonJobSerializer.deserialize_verified(
                verified_payload,
                allowed_prefixes=config(
                    "queue.drivers.amqp.allowed_job_prefixes",
                    (),
                ),
            )
            instance = instantiate_job(
                app_instance,
                msg.get("obj"),
                msg.get("args", ()),
                msg.get("init_kwargs", {}),
            )
            if instance is not None:
                # Carry the dispatcher's trace context onto the job so
                # BaseJob.handle re-parents its span (Obs-4 propagation).
                instance._otel_carrier = msg.get("_otel")
                # Dispatcher's tenant scope — armed around the job body
                # by run_through_middleware_async.
                instance._tenant_id = msg.get("_tenant")
                instance._tenant_mode = msg.get("_tenant_mode")
                instance._dispatched_at = msg.get("dispatched_at")
            callback = msg.get("callback", "handle")
            init_args = msg.get("args", ())
            db_job_id = msg.get("db_job_id")

            # A payload with no ``obj`` (or ``obj=None``) is malformed —
            # the worker has no class to call and no failed() hook to
            # invoke. Pre-fix the
            # ``callable(getattr(None, callback))`` check below was
            # False, the block was skipped, and the success branch
            # ACKed the message + emitted ``outcome="success"``
            # metrics on work that never ran. Producers can hit this
            # by accident — a script pushing a raw dict, a JSON
            # serializer where ``obj`` resolves to None — and the
            # only operator-visible symptom is silently-missing work.
            # Route straight to the DLQ with an explicit error so
            # the trail exists.
            if instance is None:
                Log.error(
                    "❌ Malformed payload (missing 'obj'): job_id=%s keys=%s — routing to DLQ",
                    msg.get("job_id"),
                    sorted(msg.keys()),
                )
                JobProcessor._ack_to_dlq(
                    channel,
                    method_frame,
                    msg,
                    "payload missing 'obj'",
                    delivery_store=delivery_store,
                    delivery_lease_token=delivery_lease_token,
                    tracker=tracker,
                    db_job_id=db_job_id,
                )
                _mx_queue = _queue_label(msg, queue_name=queue_name)
                _mx_job = _job_label(None, msg)
                _mx_record("malformed")
                return "failure"

            signed_timeout = int(msg["timeout_seconds"])
            current_timeout = delivery_store.execution_timeout_for(type(instance))
            job_timeout = min(signed_timeout, current_timeout)

            # Queue wait time — measure dispatched_at → now.
            _dispatched_at = getattr(instance, "_dispatched_at", None)
            if _dispatched_at and isinstance(_dispatched_at, str):
                try:
                    import pendulum

                    dt = pendulum.parse(_dispatched_at)
                    wait_secs = max((pendulum.now("UTC") - dt).total_seconds(), 0)
                    if hasattr(instance, "__dict__"):
                        instance._queue_wait_seconds = wait_secs
                except Exception:
                    wait_secs = None
            else:
                wait_secs = None

            # Metric labels — now that we have a resolved job instance.
            # ``queue_name`` (the queue this worker actually polled) is the
            # highest-fidelity label; dropping it here collapsed most
            # consumed jobs onto the producer-side hint or "unknown".
            _mx_queue = _queue_label(msg, instance, queue_name=queue_name)
            _mx_job = _job_label(instance, msg)
            if MetricsBase is not None:
                try:
                    MetricsBase.queue_jobs_in_flight.labels(
                        queue=_mx_queue,
                        job_class=_mx_job,
                    ).inc()
                    _mx_inflight_entered = True
                except (ImportError, RuntimeError, AttributeError, OSError):
                    pass
                if wait_secs is not None:
                    with contextlib.suppress(
                        OSError, RuntimeError, AttributeError, ConnectionError
                    ):
                        MetricsBase.queue_wait_seconds.labels(
                            queue=_mx_queue,
                            job_class=_mx_job,
                        ).observe(wait_secs)

            # Set up job tracking
            job_id = msg.get("job_id")
            if hasattr(instance, "set_tracking_id") and job_id:
                instance.set_tracking_id(job_id)

            if db_job_id and hasattr(instance, "__dict__"):
                instance._db_job_id = db_job_id

            if tracker is None or db_job_id is None:
                raise DeliverySettlementError(
                    "Durable AMQP execution requires a persistent JobTracker fence."
                )

            if claim.reclaimed:
                completed: list[bool] = []
                JobProcessor._retry_settlement_step(
                    f"Tracked queue job {db_job_id} completion lookup",
                    lambda: completed.append(tracker.is_job_completed(db_job_id)),
                )
                if completed[-1]:
                    try:
                        delivery_store.complete_with_tracker(
                            str(msg["job_id"]),
                            delivery_lease_token,
                            db_job_id=db_job_id,
                        )
                    except Exception as exc:
                        raise DeliverySettlementError(
                            "Could not recover completed queue delivery state."
                        ) from exc
                    JobProcessor._broker_ack(
                        channel,
                        method_frame.delivery_tag,
                    )
                    _mx_record("tracker_completed")
                    return True

            # Update job table status to processing
            JobProcessor._retry_settlement_step(
                f"Tracked queue job {db_job_id} processing",
                lambda: tracker.update_job_status_strict(
                    db_job_id,
                    "processing",
                ),
            )

            # Stamp container on job so BaseJob and method-level DI can use it.
            # Only set on the INSTANCE — never on type(instance) — to avoid
            # thread-safety issues where concurrent workers overwrite each
            # other's container binding on the shared job class.
            if app_instance is not None and hasattr(instance, "__dict__"):
                instance._app = app_instance

            # Execute job — auto-inject type-hinted deps via container.call()
            #
            # Job middleware (RateLimited, WithoutOverlapping,
            # ThrottlesExceptions, etc.) used to apply only when a job was
            # dispatched via Bus.dispatch() in the sync context. Jobs
            # arriving here through RabbitMQ → queue:work skipped the
            # middleware pipeline entirely, so a job declaring a
            # ``middleware()`` list got it for sync calls but silently
            # lost the protection on the production async path. Routing
            # the call through ``run_through_middleware_async`` closes
            # that gap; if the job has no middleware the helper is
            # effectively a passthrough.
            method_to_call = getattr(instance, callback, None)
            if not callable(method_to_call):
                raise AttributeError(
                    f"Job {instance.__class__.__name__} has no callable "
                    f"'{callback}' method — treating as terminal failure"
                )

            if callable(method_to_call):
                from cara.queues.middleware import run_through_middleware_async

                if inspect.iscoroutinefunction(method_to_call):

                    async def _async_handler(_job, _m=method_to_call, _args=init_args):
                        if app_instance is not None:
                            return await app_instance.call(_m, *_args)
                        return await _m(*_args)

                    try:

                        async def _call_with_middleware():
                            return await run_through_middleware_async(
                                instance, _async_handler
                            )

                        JobProcessor._execute_async_job_with_timeout(
                            _call_with_middleware,
                            (),
                            job_timeout,
                            cancellation_registry=cancellation_registry,
                        )
                    except TimeoutError as e:
                        raise TimeoutError(
                            f"Job exceeded timeout of {job_timeout}s"
                        ) from e
                else:
                    raise QueueException(
                        f"AMQP job {instance.__class__.__name__}.handle must be async."
                    )

            # Durable terminal state MUST commit before the broker ACK. A crash
            # in the gap redelivers, but the ledger answers ``completed`` and
            # the worker ACK-skips without re-running side effects.
            if delivery_store is None or delivery_lease_token is None:
                raise DeliverySettlementError(
                    "Queue delivery has no execution lease at completion."
                )
            try:
                delivery_store.complete_with_tracker(
                    str(msg["job_id"]),
                    delivery_lease_token,
                    db_job_id=db_job_id,
                )
            except Exception as exc:
                raise DeliverySettlementError(
                    "Could not atomically persist completed queue delivery."
                ) from exc
            JobProcessor._broker_ack(channel, method_frame.delivery_tag)

            _mx_record("success")
            return "success"

        except DeliverySettlementError:
            # The handler may already have committed external side effects.
            # Never reinterpret a ledger outage as a business failure/retry;
            # leave the delivery unacknowledged and reconnect. The processing
            # lease prevents concurrent execution until stale recovery.
            _mx_record("settlement_error")
            raise

        except asyncio.CancelledError:
            # Worker shutdown is not a job failure. Leave the AMQP delivery
            # UNACKNOWLEDGED so closing this consumer's own channel requeues it
            # atomically. Release the exact durable execution lease first so
            # the next consumer can claim immediately instead of waiting for
            # timeout + grace. Never call ``failed()``, burn retry attempts or
            # route to the DLQ.
            Log.info(
                "Job %s interrupted by worker shutdown; delivery will redeliver",
                _mx_job,
            )
            if (
                delivery_store is None
                or delivery_lease_token is None
                or msg is None
                or tracker is None
                or db_job_id is None
            ):
                raise DeliverySettlementError(
                    "Interrupted queue job is missing its durable lease fence."
                )
            JobProcessor._retry_settlement_step(
                f"Queue delivery {msg['job_id']} interruption release",
                lambda: delivery_store.abandon_execution(
                    str(msg["job_id"]),
                    delivery_lease_token,
                ),
            )
            JobProcessor._retry_settlement_step(
                f"Tracked queue job {db_job_id} interruption reset",
                lambda: tracker.update_job_status_strict(
                    db_job_id,
                    "pending",
                ),
            )
            _mx_record("interrupted")
            raise

        except TimeoutError as timeout_error:
            Log.error("Job timeout: %s", timeout_error, exc_info=True)

            JobProcessor._route_failed_message(
                channel=channel,
                method_frame=method_frame,
                msg=msg,
                instance=instance,
                exc=timeout_error,
                queue_name=queue_name,
                delivery_store=delivery_store,
                delivery_lease_token=delivery_lease_token,
                tracker=tracker,
                db_job_id=db_job_id or (msg or {}).get("db_job_id"),
            )

            _mx_record("timeout")
            return "failure"

        except Exception as job_error:
            Log.error("Job failed: %s", job_error, exc_info=True)

            JobProcessor._route_failed_message(
                channel=channel,
                method_frame=method_frame,
                msg=msg,
                instance=instance,
                exc=job_error,
                queue_name=queue_name,
                delivery_store=delivery_store,
                delivery_lease_token=delivery_lease_token,
                tracker=tracker,
                db_job_id=db_job_id or (msg or {}).get("db_job_id"),
            )

            _mx_record("failed")
            return "failure"  # Still processed (failed gracefully)


@command(
    name="queue:work",
    help=(
        "CONSUMER: take jobs off RabbitMQ and run them. "
        "Not sufficient on its own — see below.\n"
        "\n"
        "This process only CONSUMES. It never publishes. `Bus.dispatch` does "
        "not write to RabbitMQ either: it commits a row to the "
        "`queue_job_delivery` outbox, and `craft queue:relay` is the ONLY "
        "thing that turns those rows into broker messages.\n"
        "\n"
        "So `queue:work` alone gets you a worker listening to an empty "
        "broker: every dispatch reports success, nothing ever runs, and "
        "nothing else complains. Run `craft queue:relay` alongside it. Add "
        "`craft schedule:work` if you also want scheduled jobs to fire.\n"
        "\n"
        "This command warns at startup if it finds an aged, undrained "
        "outbox — but it will still start, so read the banner."
    ),
    options={
        "--driver=?": "Queue driver to use (overrides default configuration)",
        "--queue=?": "Queue name(s) to process (comma-separated)",
        "--pool=?": "Worker pool name from config/queue.py WORKER_POOLS (e.g. pipeline, enrichment, background, realtime). Overrides --queue and --concurrency with pool config.",
        "--timeout=?": "Reconnect backoff in seconds after a broker disconnect (default: 5)",
        "--max-jobs=?": "Maximum number of jobs to process before stopping",
        "--max-time=?": "Maximum runtime in seconds before stopping",
        "--concurrency=?": "Number of parallel consumer threads inside this worker process (default: 1). Each thread keeps its own AMQP connection/channel, so --concurrency=5 is roughly equivalent to starting 5 worker processes but shares the Python heap, the Cara DB connection pool, and the HTTP clients — much lower memory and cleaner lifecycle.",
        "--reload": "Enable auto-reload on file changes",
    },
)
class QueueWorkCommand(MakesAutoReload, CommandBase):
    """Run queue worker with enhanced monitoring and graceful shutdown."""

    def __init__(self, application=None):
        super().__init__(application)
        self.start_time = None
        self.jobs_processed = 0
        self.jobs_failed = 0
        # Memory ceiling for the worker process — configurable via
        # WORKER_MEMORY_LIMIT_MB env. The default doubles when
        # ``--concurrency`` is in use (each consumer thread carries its
        # own ORM pool + HTTP clients + parser state) so multi-threaded
        # workers don't hit the limit after a handful of scrapes.
        try:
            from cara.configuration import config

            limit_mb = int(config("queue.worker_memory_limit_mb", 512))
        except Exception:
            limit_mb = 512
        # Bumped minimum so multi-thread heavy workloads (browser pools +
        # extractor pipelines + HTTP clients per thread) breathe without
        # bouncing. Override with the env if 2 GB is too aggressive for
        # the deploy box.
        limit_mb = max(limit_mb, 2048)
        self.memory_limit_bytes = limit_mb * 1024 * 1024
        self._signal_handlers_installed = False
        self._atexit_registered = False
        self._shutdown_signal: int | None = None
        self._consumer_threads: list[threading.Thread] = []
        self._consumer_state_lock = threading.Lock()
        self._active_consumer_slots = 0
        self._active_job_cancellations = ActiveJobCancellationRegistry()
        self._resource_shutdown_lock = threading.Lock()
        self._worker_resources_shutdown = False
        self._reload_requested = False

    def _setup_worker_lifecycle_hooks(self) -> None:
        """Install graceful-shutdown hooks once per worker process.

        Without SIGINT/SIGTERM handlers the worker only stopped when the
        poll loop happened to check ``shutdown_requested`` — Ctrl+C could
        abort mid-job and skip ``_shutdown_worker_resources``. ``atexit``
        covers abrupt ``sys.exit`` / supervisor SIGKILL-followup paths that
        still run interpreter teardown.
        """
        if not self._signal_handlers_installed:

            def _graceful_stop(signum, _frame):
                # First signal requests a bounded graceful drain. A second
                # signal is an explicit operator request to stop immediately;
                # hard process exit is the only safe way to interrupt arbitrary
                # Python threads, and lets RabbitMQ redeliver every unacked job.
                if self.shutdown_requested:
                    self._force_terminate_for_redelivery(
                        reason="second shutdown signal",
                        signal_number=signum,
                    )
                    return

                self._shutdown_signal = signum
                self.shutdown_requested = True
                Log.info(
                    "Queue worker received %s — draining current job then exiting",
                    signal.Signals(signum).name if hasattr(signal, "Signals") else signum,
                )

            signal.signal(signal.SIGINT, _graceful_stop)
            signal.signal(signal.SIGTERM, _graceful_stop)
            self._signal_handlers_installed = True

        if not self._atexit_registered:
            atexit.register(self._shutdown_worker_resources)
            self._atexit_registered = True

    def handle(
        self,
        driver: str | None = None,
        queue: str | None = None,
        pool: str | None = None,
        timeout: str | None = None,
        max_jobs: str | None = None,
        max_time: str | None = None,
        concurrency: str | None = None,
    ):
        """Handle queue worker execution with enhanced monitoring."""
        # ── Pool resolution ────────────────────────────────────────
        # --pool=<name> reads WORKER_POOLS from config/queue.py and
        # overrides --queue, --concurrency, and --timeout with pool
        # values. Explicit flags still take precedence.
        if pool:
            pool_cfg = self._resolve_pool(pool)
            if pool_cfg is None:
                raise InvalidArgumentException(f"Invalid worker pool: {pool}")
            if not queue:
                queue = ",".join(pool_cfg["queues"])
            if not concurrency:
                concurrency = str(pool_cfg.get("concurrency", 1))
            if not timeout:
                timeout = str(pool_cfg.get("timeout", 5))

        self.console.print()  # Empty line for spacing
        self.console.print("[bold #e5c07b]╭─ Queue Worker ─╮[/bold #e5c07b]")
        self.console.print()

        # Stand up /metrics on a side-thread HTTP server so Prometheus
        # can scrape the worker. Opt out with METRICS_PORT=0.
        try:
            from cara.observability import MetricsBase as _Metrics
            from cara.observability import start_http_server as _start_metrics

            _Metrics.queue_worker_ready.set(0)
            _Metrics.queue_worker_configured_queues.set(0)
            _port = _start_metrics(role="worker")
            if _port:
                Log.info("📈 Metrics server on :%s/metrics", _port)
        except Exception as e:
            if str(config("app.env", "local")).lower() in {"production", "prod"}:
                raise CaraException("Worker metrics server failed to start") from e
            Log.warning("metrics server startup failed: %s", e)

        # Is anything actually PUBLISHING the work this worker consumes?
        # ``queue:work`` and ``queue:relay`` read like a matched pair but
        # only one of them is a consumer; starting this one alone leaves a
        # worker listening to an empty broker while dispatches pile up in
        # the outbox (2026-07-20: 1250 jobs, zero complaints). Advisory
        # only — see PublicationBacklogProbe on why this must never be
        # able to stop a worker from starting.
        self._warn_when_nothing_is_publishing()

        # Worker-startup hooks — declared by the app in
        # config/queue.py::WORKER_STARTUP_HOOKS (dotted paths to sync
        # callables, e.g. a domain metrics sampler). Kept out of the
        # framework so cara carries no app/domain startup logic.
        self._run_worker_startup_hooks()

        # Parse concurrency early so we can use it to gate the reload path
        # (auto-reload restarts the whole worker — fine with 1 thread, but
        # with N parallel consumer threads we want to drain them first).
        concurrency_val = 1
        if concurrency:
            try:
                concurrency_val = max(1, int(concurrency))
            except ValueError:
                raise InvalidArgumentException(
                    f"Invalid --concurrency value: {concurrency!r}"
                )
        self._concurrency = concurrency_val

        # Store parameters for restart
        self.store_restart_params(driver, queue, timeout, max_jobs, max_time)

        # Auto-reload only when explicitly requested — module purging
        # invalidates IoC container bindings (contract→implementation
        # identity is lost after re-import), causing resolution failures
        # like "Can't instantiate abstract class …Contract".
        if self.option("reload"):
            self.enable_auto_reload()

        self._setup_worker_lifecycle_hooks()

        # Start main worker loop
        try:
            self._run_main_loop(driver, queue, timeout, max_jobs, max_time)
        except Exception as e:
            self.error(f"× Worker error: {e}")
            self.error(f"× Stack trace: {traceback.format_exc()}")
            raise
        finally:
            with contextlib.suppress(Exception):
                from cara.observability import MetricsBase as _Metrics

                _Metrics.queue_worker_ready.set(0)
            self.cleanup_auto_reload()
            self._show_final_stats()
        if self._reload_requested:
            self._restart_worker_process()

    def _warn_when_nothing_is_publishing(self) -> str | None:
        """Print a loud banner when the outbox is aged and undrained.

        Returns the advisory text (for tests), or ``None`` when there is
        nothing to say. NEVER raises and NEVER exits: an operator whose
        relay is down still needs their worker to come up, and a
        diagnostic that kills its host process is worse than the silence
        it replaces.
        """
        from cara.queues.delivery import PublicationBacklogProbe

        def _emit(message: str) -> None:
            self.console.print()
            self.console.print(
                "[bold #e06c75]⚠ NOTHING IS PUBLISHING TO THE BROKER[/bold #e06c75]"
            )
            for line in message.splitlines():
                self.console.print(f"[#e5c07b]  {line}[/#e5c07b]")
            self.console.print()

        try:
            return PublicationBacklogProbe.announce(emit=_emit)
        except Exception:  # noqa: BLE001 — belt and braces; see docstring
            return None

    def _trigger_auto_reload(self) -> None:
        """Drain the worker, then replace the process for code reload.

        The generic in-process reload purges app modules and resource pools
        after a fixed 500ms sleep. That is unsafe for queue consumers whose
        jobs may still be executing in other threads. A process replacement
        after the normal drain gives every consumer the same shutdown contract
        as SIGTERM and starts with a coherent module/container graph.
        """
        if not self._auto_reload_enabled or self._reload_requested:
            return
        self.info("🔄 File changed — draining worker before process reload")
        self._reload_requested = True
        self.shutdown_requested = True

    @staticmethod
    def _restart_worker_process() -> None:
        """Replace the drained worker with the same interpreter/arguments."""
        os.execv(sys.executable, [sys.executable, *sys.argv])

    def _prepare_config(
        self,
        driver: str | None,
        queue: str | None,
        timeout: str | None,
        max_jobs: str | None,
        max_time: str | None,
    ) -> dict[str, Any]:
        """Prepare and validate worker configuration."""
        # Determine driver
        driver_name = driver or config("queue.default")
        if not driver_name:
            raise ConfigurationException(
                "No driver specified and no default 'queue.default' configured"
            )

        drivers = config("queue.drivers", {})
        if driver_name not in drivers:
            raise ConfigurationException(f"Driver '{driver_name}' is not configured")

        # Parse timeout
        timeout_val = 5
        if timeout:
            try:
                timeout_val = int(timeout)
                if timeout_val < 1:
                    raise InvalidArgumentException("Timeout must be at least 1 second")
            except ValueError as e:
                raise InvalidArgumentException(f"Invalid timeout value: {e}") from e
        else:
            # Get from driver config
            timeout_val = config(f"queue.drivers.{driver_name}.poll", 5)

        # Parse limits
        max_jobs_val = None
        if max_jobs:
            try:
                max_jobs_val = int(max_jobs)
                if max_jobs_val <= 0:
                    raise InvalidArgumentException("max-jobs must be positive")
            except ValueError as e:
                raise InvalidArgumentException(f"Invalid max-jobs value: {e}") from e

        max_time_val = None
        if max_time:
            try:
                max_time_val = int(max_time)
                if max_time_val <= 0:
                    raise InvalidArgumentException("max-time must be positive")
            except ValueError as e:
                raise InvalidArgumentException(f"Invalid max-time value: {e}") from e

        return {
            "driver_name": driver_name,
            "queue_names": self._parse_queue_names(queue),
            "timeout": timeout_val,
            "max_jobs": max_jobs_val,
            "max_time": max_time_val,
        }

    def _resolve_pool(self, pool_name: str) -> dict[str, Any] | None:
        """Resolve a named worker pool from config/queue.py WORKER_POOLS.

        Returns the pool dict on success, or None after printing an error.
        """
        pools = config("queue.worker_pools", None)
        if not pools:
            self.error("× No WORKER_POOLS defined in config/queue.py")
            return None
        if pool_name not in pools:
            available = ", ".join(sorted(pools.keys()))
            self.error(f"× Pool '{pool_name}' not found. Available: {available}")
            return None
        pool_cfg = pools[pool_name]
        if not pool_cfg.get("queues"):
            self.error(f"× Pool '{pool_name}' has no queues defined")
            return None
        self.console.print(
            f"  [bold #30e047]Pool:[/bold #30e047] [white]{pool_name}[/white] "
            f"[dim]({len(pool_cfg['queues'])} queues, "
            f"concurrency={pool_cfg.get('concurrency', 1)}, "
            f"timeout={pool_cfg.get('timeout', 5)}s)[/dim]"
        )
        return pool_cfg

    def _parse_queue_names(self, queue: str | None) -> list:
        """Parse queue names from comma-separated string with wildcard support."""
        if not queue:
            return ["default"]

        # Split by comma and clean up
        queue_patterns = [q.strip() for q in queue.split(",")]
        queue_patterns = [q for q in queue_patterns if q]  # Remove empty strings

        if not queue_patterns:
            return ["default"]

        # Expand wildcard patterns
        expanded_queues = []
        for pattern in queue_patterns:
            # Trailing-dot prefix shorthand: "discovery." means "every
            # priority sub-queue of discovery" — i.e. discovery.{critical,
            # high,default,low} (plus any nested queues the management API
            # reports, e.g. notification.email.default). This is the form
            # the operator-facing docs and the e2e queue:work command use.
            #
            # Without this normalisation a bare "discovery." fell through
            # the ``"*" in pattern`` check and was polled as a LITERAL queue
            # name. RabbitMQ has no queue called "discovery.", so the worker
            # lazily created an empty one and consumed from it forever while
            # the real discovery.default (where a discovery job actually
            # lands) was never read — the whole pipeline stalled at dispatch.
            # Mapping "prefix." → "prefix.*" routes it through the same
            # expansion the wildcard form already uses.
            if pattern.endswith(".") and "*" not in pattern:
                pattern = f"{pattern}*"

            if "*" in pattern:
                expanded_queues.extend(self._expand_wildcard_pattern(pattern))
            else:
                expanded_queues.append(pattern)

        # De-duplicate while preserving the operator/config sequence so a
        # queue named by two overlapping patterns (e.g. "discovery." and
        # "discovery.high") isn't polled twice per cycle.
        seen: set[str] = set()
        deduped = [q for q in expanded_queues if not (q in seen or seen.add(q))]

        return deduped if deduped else ["default"]

    def _expand_wildcard_pattern(self, pattern: str) -> list:
        """Expand wildcard pattern to actual queue names.

        Two-phase expansion:
        1. Try to discover real queues from RabbitMQ Management API and
           match with fnmatch. This catches nested prefixes like
           ``notification.email.default`` when the user passes
           ``notification.*``.
        2. Merge canonical queue names from configured bindings so the worker
           starts correctly even when RabbitMQ management is unavailable.
        """
        import fnmatch as _fnmatch

        if pattern.endswith(".*"):
            static: set[str] = set()
        elif pattern.endswith("*"):
            static = set()
        else:
            return [pattern]

        # Merge with any extra queues discovered from RabbitMQ
        # (e.g. notification.email.default) that the static set misses.
        discovered = self._discover_rabbitmq_queues()
        if discovered:
            matched = {q for q in discovered if _fnmatch.fnmatch(q, pattern)}
            static |= matched

        # Also merge canonical queue names declared in the process-local
        # routing rules that match this pattern. Live RabbitMQ discovery only
        # sees queues that ALREADY exist at worker startup; a queue first
        # created mid-run — e.g. ``notification.email`` the moment the first
        # email job is dispatched after the worker booted — would otherwise
        # never be polled, so those messages pile up with no consumer. The
        # rules are the declarative source of truth for canonical queue
        # names (notification.email/sms/push, etc.), so consulting them makes
        # a ``notification.*`` worker pick up those channel queues regardless
        # of broker timing. Missing queues are handled gracefully by the
        # per-queue declare path, so adding a not-yet-created name is safe.
        try:
            from cara.configuration import config as _config

            bindings = _config("queue.queue_routing_rules", []) or []
            bound = {
                name for name, _routing in bindings if _fnmatch.fnmatch(name, pattern)
            }
            static |= bound
        except (ImportError, RuntimeError, AttributeError, OSError):
            pass

        return sorted(static)

    def _discover_rabbitmq_queues(self) -> list:
        """Fetch existing queue names from RabbitMQ Management API.

        Returns an empty list on any failure so the caller can
        fall back to static expansion.
        """
        if hasattr(self, "_rabbitmq_queues_cache"):
            return self._rabbitmq_queues_cache

        try:
            import json
            import urllib.request

            from cara.configuration import config

            # The AMQP config lives under queue.drivers.amqp.* (see
            # QueueProvider) — the old queue.connections.amqp.* paths never
            # existed, so discovery always probed guest@127.0.0.1.
            host = config("queue.drivers.amqp.host", "127.0.0.1")
            mgmt_port = config("queue.drivers.amqp.management_port", 15672)
            user = config("queue.drivers.amqp.username", "guest")
            password = config("queue.drivers.amqp.password", "guest")
            vhost = config("queue.drivers.amqp.vhost", "/")

            import urllib.parse

            encoded_vhost = urllib.parse.quote(vhost, safe="")
            url = f"http://{host}:{mgmt_port}/api/queues/{encoded_vhost}"

            req = urllib.request.Request(url)
            credentials = f"{user}:{password}"
            import base64

            auth = base64.b64encode(credentials.encode()).decode()
            req.add_header("Authorization", f"Basic {auth}")

            with urllib.request.urlopen(req, timeout=3) as resp:
                data = json.loads(resp.read())
                queues = [q["name"] for q in data if isinstance(q, dict) and "name" in q]
                self._rabbitmq_queues_cache = queues
                return queues
        except Exception:
            self._rabbitmq_queues_cache = []
            return []

    def _show_config(self, config: dict[str, Any]):
        """Display worker configuration in ServeCommand style."""
        self.console.print("[bold #e5c07b]┌─ Configuration[/bold #e5c07b]")

        # Driver info
        self.console.print(
            f"[#e5c07b]│[/#e5c07b] [white]Driver:[/white] [bold white]{config['driver_name'].upper()}[/bold white]"
        )

        # Queue info
        queue_names = config["queue_names"]
        if len(queue_names) > 1:
            self.console.print(
                f"[#e5c07b]│[/#e5c07b] [white]Queues:[/white] [dim]{len(queue_names)} canonical queues[/dim]"
            )
            for i, queue in enumerate(queue_names, 1):  # Show all queues
                priority_color = (
                    "#E21102"
                    if "critical" in queue
                    else "#e5c07b"
                    if "high" in queue
                    else "#30e047"
                    if "default" in queue
                    else "dim"
                )
                self.console.print(
                    f"[#e5c07b]│[/#e5c07b]   [white]{i}.[/white] [{priority_color}]{queue}[/{priority_color}]"
                )
        else:
            queue_color = (
                "#E21102"
                if "critical" in queue_names[0]
                else "#e5c07b"
                if "high" in queue_names[0]
                else "#30e047"
            )
            self.console.print(
                f"[#e5c07b]│[/#e5c07b] [white]Queue:[/white] [{queue_color}]{queue_names[0]}[/{queue_color}]"
            )

        # Timing and limits
        self.console.print(
            f"[#e5c07b]│[/#e5c07b] [white]Reconnect Backoff:[/white] [dim]{config['timeout']}s[/dim]"
        )

        if config.get("max_jobs"):
            self.console.print(
                f"[#e5c07b]│[/#e5c07b] [white]Max Jobs:[/white] [dim]{config['max_jobs']}[/dim]"
            )
        if config.get("max_time"):
            self.console.print(
                f"[#e5c07b]│[/#e5c07b] [white]Max Runtime:[/white] [dim]{config['max_time']}s[/dim]"
            )

        # Auto-reload status (default: enabled in development)

        auto_reload = bool(self.option("reload"))
        self.console.print(
            f"[#e5c07b]│[/#e5c07b] [white]Auto-reload:[/white] [{'#30e047' if auto_reload else '#E21102'}]{'✓' if auto_reload else '×'}[/{'#30e047' if auto_reload else '#E21102'}]"
        )

        self.console.print("[#e5c07b]└─[/#e5c07b]")
        self.console.print()

    def _run_worker(self, config: dict[str, Any]) -> None:
        """Run long-lived AMQP consumers with bounded parallelism.

        We spin up N independent consumer threads, including when N=1, each
        with its own AMQP connection + broker-side subscription. Keeping the command/signal
        loop on the main thread is what makes the bounded shutdown and async
        cancellation contract enforceable for every concurrency setting. pika's
        BlockingConnection is not thread-safe across threads, so each
        thread keeps its own manager. The threads share:

        * The job processor (stateless, so safe to share).
        * The Cara DB connection pool + in-flight semaphore (module-level,
          built for multi-thread access from the start).
        * ``jobs_processed`` / ``jobs_failed`` counters — incremented
          under a lock below (would otherwise race and undercount).

        The number of in-flight jobs is bounded by ``concurrency``; each
        channel uses ``prefetch_count=1`` and handles one delivery at a time.
        RabbitMQ pushes jobs immediately; there is no ``basic_get`` polling,
        idle sleep, or per-cycle channel churn.
        """
        queue_names = config["queue_names"]
        concurrency = getattr(self, "_concurrency", 1)
        if concurrency < len(queue_names):
            raise ConfigurationException(
                "Quorum queue workers require at least one consumer slot per "
                f"configured queue ({concurrency} slots for "
                f"{len(queue_names)} queues)."
            )

        self._show_worker_startup_info(queue_names, concurrency)
        self.start_time = time.time()
        self._active_job_cancellations = ActiveJobCancellationRegistry()
        self._consumer_threads = []
        self._consumer_state_lock = threading.Lock()
        self._active_consumer_slots = 0

        # Lock protecting shared counters + shutdown flag read-modify-writes.
        # shutdown_requested itself is a bool (atomic) so we read it
        # unlocked; counters genuinely need a lock.
        self._stats_lock = threading.Lock()

        # Consumer-thread mode. Even concurrency=1 stays off the main thread so
        # SIGTERM can cancel/force-redeliver a job that exceeds the drain budget.
        from cara.configuration import config as global_config

        job_processor = JobProcessor(self._active_job_cancellations)
        queue_driver = Queue.driver(config["driver_name"])
        if not hasattr(queue_driver, "_connection_parameters"):
            raise ConfigurationException(
                "queue:work requires the AMQP driver for durable subscriptions"
            )
        threads: list[threading.Thread] = []
        self._consumer_threads = threads

        def _consumer_loop(slot_idx: int) -> None:
            """One durable consumer slot with bounded reconnect backoff."""
            reconnect_delay = min(max(int(config.get("timeout", 5)), 1), 10)
            assigned_queue = queue_names[(slot_idx - 1) % len(queue_names)]
            while not self.shutdown_requested:
                mgr = AMQPConnectionManager(global_config, queue_driver)
                try:
                    self._consume_queue_stream(
                        queue_names=[assigned_queue],
                        connection_manager=mgr,
                        job_processor=job_processor,
                        config=config,
                    )
                except Exception as exc:
                    if not self.shutdown_requested:
                        Log.warning(
                            "[worker-%s] AMQP consumer disconnected: %s",
                            slot_idx,
                            exc,
                        )
                finally:
                    mgr.close()
                if not self.shutdown_requested:
                    time.sleep(reconnect_delay)

        try:
            for i in range(concurrency):
                t = threading.Thread(
                    target=_consumer_loop,
                    args=(i + 1,),
                    name=f"queue-worker-{i + 1}",
                    daemon=True,
                )
                t.start()
                threads.append(t)

            # Main thread just waits for shutdown. Poll for the signal
            # rather than join() because join() on daemon threads would
            # block forever if one thread deadlocks. Also poll the
            # configured stop conditions so --max-time fires even if
            # every consumer is blocked on a slow job (otherwise a
            # poison-message that hangs forever would never trip the
            # cap).
            next_broker_probe_at = 0.0
            while not self.shutdown_requested:
                now = time.monotonic()
                if now >= next_broker_probe_at:
                    from cara.observability import MetricsBase as _Metrics

                    try:
                        Queue.driver(config["driver_name"]).verify_runtime_health(
                            queue_names,
                            force=True,
                        )
                    except Exception as exc:
                        _Metrics.queue_worker_ready.set(0)
                        Log.warning("Queue worker readiness probe failed: %s", exc)
                    else:
                        with self._consumer_state_lock:
                            active_slots = self._active_consumer_slots
                        _Metrics.queue_worker_ready.set(
                            1 if active_slots == concurrency else 0
                        )
                    next_broker_probe_at = now + 10.0
                if self._should_stop(config):
                    self.shutdown_requested = True
                    break
                time.sleep(1)
        finally:
            self.shutdown_requested = True
            drained = self._drain_consumer_threads(threads)
            # Production cannot observe ``False`` because the escalation path
            # calls ``os._exit``. The condition lets tests replace hard-exit
            # without accidentally running hooks while fake consumers remain.
            if drained:
                self._shutdown_worker_resources()

    def _drain_consumer_threads(self, threads: list[threading.Thread]) -> bool:
        """Drain, then cancel, parallel consumers without tearing resources down.

        Phase 1 gives in-flight work the configured graceful budget. Phase 2
        cooperatively cancels registered asyncio jobs; their deliveries remain
        unacknowledged and each consumer closes its own AMQP connection before
        returning. Python cannot safely interrupt a running synchronous thread,
        so anything still alive after the cancellation grace forces a process
        exit. The OS then closes sockets and DB connections atomically, which
        makes RabbitMQ redeliver instead of fabricating job failures.
        """
        drain_budget = self._shutdown_drain_seconds()
        self._join_threads_until(threads, time.monotonic() + drain_budget)
        still_alive = [thread for thread in threads if thread.is_alive()]
        if not still_alive:
            return True

        cancelled = self._active_job_cancellations.cancel_all()
        cancel_budget = self._shutdown_cancel_seconds()
        Log.warning(
            "Worker shutdown: %s consumer thread(s) exceeded the %ss drain; "
            "requested cancellation for %s async job(s)",
            len(still_alive),
            drain_budget,
            cancelled,
        )
        self._join_threads_until(still_alive, time.monotonic() + cancel_budget)
        still_alive = [thread for thread in still_alive if thread.is_alive()]
        if not still_alive:
            return True

        self._force_terminate_for_redelivery(
            reason=(
                f"{len(still_alive)} consumer thread(s) remained active after "
                f"{drain_budget + cancel_budget:g}s shutdown budget"
            ),
            signal_number=self._shutdown_signal,
        )
        return False

    @staticmethod
    def _join_threads_until(threads: list[threading.Thread], deadline: float) -> None:
        """Join multiple threads against one shared deadline."""
        for thread in threads:
            remaining = max(0.0, deadline - time.monotonic())
            with contextlib.suppress(ImportError, RuntimeError, AttributeError, OSError):
                thread.join(timeout=remaining)

    @staticmethod
    def _force_terminate_for_redelivery(
        *, reason: str, signal_number: int | None = None
    ) -> None:
        """Terminate without cleanup so the broker redelivers unacked work.

        Running resource hooks or closing pika connections from the main thread
        would race live consumers and turn shutdown into ordinary job errors.
        ``os._exit`` deliberately skips those callbacks; the kernel closes the
        process' sockets and open DB transactions, giving RabbitMQ/DB their
        native redelivery/rollback semantics.
        """
        Log.error("Worker forced shutdown: %s; unacked jobs will redeliver", reason)
        exit_code = (
            128 + int(signal_number)
            if signal_number is not None
            else getattr(os, "EX_TEMPFAIL", 75)
        )
        os._exit(exit_code)

    @staticmethod
    def _shutdown_drain_seconds() -> float:
        """Graceful-shutdown drain budget (seconds) for in-flight jobs.

        Configurable via ``queue.shutdown_drain_seconds``. Defaults GENEROUS
        (120s) so a normal scrape/enrich/consolidation job finishes cleanly on
        SIGTERM instead of being killed mid-transaction at the old flat 10s cap,
        while still bounding how long a wedged poison-thread can delay a deploy.
        Operators can raise it (long batch jobs) or lower it (fast-only workers).
        """
        try:
            return max(0.0, float(config("queue.shutdown_drain_seconds", 120.0)))
        except (TypeError, ValueError):
            return 120.0

    @staticmethod
    def _shutdown_cancel_seconds() -> float:
        """Grace after async cancellation before forced process exit."""
        try:
            return max(0.0, float(config("queue.shutdown_cancel_seconds", 5.0)))
        except (TypeError, ValueError):
            return 5.0

    @staticmethod
    def _run_worker_startup_hooks() -> None:
        """Run app-declared worker-startup hooks.

        Hooks live in the APP (``config/queue.py::WORKER_STARTUP_HOOKS``) as
        dotted paths to sync, non-blocking module-level callables (they should
        spawn their own background threads). Keeping them in config means the
        framework worker holds no app/domain startup logic (e.g. a metrics
        sampler that queries product tables).
        """
        import importlib

        try:
            hooks = config("queue.worker_startup_hooks", []) or []
        except Exception:
            hooks = []
        for path in hooks:
            try:
                module_path, attr = path.rsplit(".", 1)
                fn = getattr(importlib.import_module(module_path), attr)
                fn()
            except Exception as exc:
                Log.warning("worker startup hook %s failed: %s", path, exc)

    def _shutdown_worker_resources(self) -> bool:
        """Run app-declared worker-shutdown hooks (release pooled resources).

        Hooks live in the APP (``config/queue.py::WORKER_SHUTDOWN_HOOKS``) as
        dotted paths to callables (sync or async); coroutine results are
        awaited. Domain teardown — browser pools, fetch drivers that leak OS
        handles (semaphores / Playwright children) on abrupt exit — registers
        here, so the framework worker holds no app/domain teardown logic.
        """
        import asyncio
        import importlib

        active_consumers = [
            thread
            for thread in getattr(self, "_consumer_threads", [])
            if thread.is_alive()
        ]
        if active_consumers:
            Log.warning(
                "Worker resource shutdown deferred: %s consumer thread(s) "
                "are still active",
                len(active_consumers),
            )
            return False

        resource_lock = getattr(self, "_resource_shutdown_lock", None)
        if resource_lock is None:
            resource_lock = threading.Lock()
            self._resource_shutdown_lock = resource_lock

        with resource_lock:
            if getattr(self, "_worker_resources_shutdown", False):
                return True
            # Mark before invoking hooks so atexit and the normal finally path
            # cannot race the same browser/executor shutdown twice.
            self._worker_resources_shutdown = True

        try:
            hooks = config("queue.worker_shutdown_hooks", []) or []
        except Exception:
            hooks = []
        if not hooks:
            return True

        async def _close_all() -> None:
            for path in hooks:
                try:
                    module_path, attr = path.rsplit(".", 1)
                    fn = getattr(importlib.import_module(module_path), attr)
                    result = fn()
                    if asyncio.iscoroutine(result):
                        await result
                except Exception as exc:
                    Log.debug(
                        "[QueueWorkCommand] shutdown hook %s skipped: %s", path, exc
                    )

        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                loop.create_task(_close_all())
            else:
                loop.run_until_complete(_close_all())
        except RuntimeError:
            asyncio.run(_close_all())
        return True

    def _check_memory_usage(self) -> bool:
        """
        Check worker memory usage and exit gracefully if limit exceeded.
        CRITICAL FIX #3: Enforce memory limit to prevent unbounded growth.
        Returns True if memory is within limits, False if exceeded.
        """
        try:
            import psutil

            process = psutil.Process(os.getpid())
            rss_bytes = process.memory_info().rss

            if rss_bytes > self.memory_limit_bytes:
                limit_mb = self.memory_limit_bytes / (1024 * 1024)
                current_mb = rss_bytes / (1024 * 1024)
                Log.warning(
                    "⚠️ Memory limit exceeded: %.1fMB > %.1fMB. "
                    "Initiating graceful shutdown for supervisor restart.",
                    current_mb,
                    limit_mb,
                )
                self.shutdown_requested = True
                return False

            return True
        except ImportError:
            # psutil not available, fall back to /proc on Linux
            try:
                with open(f"/proc/{os.getpid()}/status", "r") as f:
                    for line in f:
                        if line.startswith("VmRSS:"):
                            rss_kb = int(line.split()[1])
                            rss_bytes = rss_kb * 1024

                            if rss_bytes > self.memory_limit_bytes:
                                limit_mb = self.memory_limit_bytes / (1024 * 1024)
                                current_mb = rss_bytes / (1024 * 1024)
                                Log.warning(
                                    "⚠️ Memory limit exceeded: %.1fMB > %.1fMB. "
                                    "Initiating graceful shutdown for supervisor restart.",
                                    current_mb,
                                    limit_mb,
                                )
                                self.shutdown_requested = True
                                return False
                            break
            except (ImportError, RuntimeError, AttributeError, OSError):
                pass

            return True

    def _show_worker_startup_info(self, queue_names: list, concurrency: int = 1) -> None:
        """Display worker startup information in ServeCommand style."""
        self.console.print("[bold #e5c07b]┌─ Worker Status[/bold #e5c07b]")

        if len(queue_names) > 1:
            self.console.print(
                f"[#e5c07b]│[/#e5c07b] [white]Processing:[/white] [dim]{len(queue_names)} canonical queues[/dim]"
            )
        else:
            queue_color = (
                "#E21102"
                if "critical" in queue_names[0]
                else "#e5c07b"
                if "high" in queue_names[0]
                else "#30e047"
            )
            self.console.print(
                f"[#e5c07b]│[/#e5c07b] [white]Monitoring:[/white] [{queue_color}]{queue_names[0]}[/{queue_color}]"
            )

        if concurrency > 1:
            self.console.print(
                f"[#e5c07b]│[/#e5c07b] [white]Concurrency:[/white] "
                f"[#30e047]{concurrency} parallel consumer threads[/#30e047]"
            )

        self.console.print(
            "[#e5c07b]│[/#e5c07b] [white]Status:[/white] [#30e047]✓ Active - Waiting for jobs[/#30e047]"
        )

        self.console.print("[#e5c07b]└─[/#e5c07b]")
        self.console.print()

        # Simple ready message
        self.console.print("[dim]Press Ctrl+C to stop the worker[/dim]")
        self.console.print()

    @staticmethod
    def _verify_consumer_queue(
        connection_manager: AMQPConnectionManager,
        queue_name: str,
    ) -> None:
        """Passively require one deploy-reconciled canonical queue."""
        channel = connection_manager.create_channel()
        if channel is None:
            raise ConnectionError("RabbitMQ channel could not be created")
        try:
            channel.queue_declare(
                queue=queue_name,
                passive=True,
            )
        finally:
            with contextlib.suppress(
                ImportError,
                RuntimeError,
                AttributeError,
                OSError,
            ):
                channel.close()

    def _record_worker_outcome(
        self,
        outcome: bool | str,
        config: dict[str, Any],
    ) -> None:
        if outcome:
            with self._stats_lock:
                if outcome == "success":
                    self.jobs_processed += 1
                elif outcome == "failure":
                    self.jobs_failed += 1
        if not self._check_memory_usage() or self._should_stop(config):
            self.shutdown_requested = True

    def _mark_consumer_slot(self, delta: int) -> None:
        """Track subscriptions so readiness reflects actual consuming ability."""
        state_lock = getattr(self, "_consumer_state_lock", None)
        if state_lock is None:
            state_lock = threading.Lock()
            self._consumer_state_lock = state_lock
        with state_lock:
            current = int(getattr(self, "_active_consumer_slots", 0))
            self._active_consumer_slots = max(0, current + delta)

    def _consume_queue_stream(
        self,
        *,
        queue_names: list[str],
        connection_manager: AMQPConnectionManager,
        job_processor: JobProcessor,
        config: dict[str, Any],
    ) -> None:
        """Register durable subscriptions and service broker deliveries."""
        if len(queue_names) != 1:
            raise ConfigurationException(
                "Each quorum-queue consumer channel must own exactly one queue."
            )
        for queue_name in queue_names:
            self._verify_consumer_queue(connection_manager, queue_name)

        channel = connection_manager.create_channel()
        if channel is None:
            raise ConnectionError("RabbitMQ consumer channel could not be created")
        # RabbitMQ quorum queues do not support global QoS. Each channel owns
        # exactly one consumer, so per-consumer prefetch=1 is also the exact
        # one-job-per-worker-slot bound.
        channel.basic_qos(prefetch_count=1, global_qos=False)

        consumer_tags: list[str] = []
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        in_flight: concurrent.futures.Future | None = None
        settled_futures: deque[concurrent.futures.Future] = deque()
        subscribed = False
        try:
            for queue_name in queue_names:

                def _on_message(
                    ch,
                    method_frame,
                    _header_frame,
                    body,
                    *,
                    consumed_queue: str = queue_name,
                ) -> None:
                    nonlocal in_flight
                    if in_flight is not None:
                        raise RuntimeError(
                            "RabbitMQ delivered more than prefetch_count=1"
                        )

                    def _release_settled_slot() -> None:
                        nonlocal in_flight
                        if in_flight is None:
                            raise RuntimeError(
                                "RabbitMQ settled a delivery without an "
                                "in-flight worker future."
                            )
                        settled_futures.append(in_flight)
                        in_flight = None

                    ack_channel = ThreadSafeAMQPAckChannel(
                        connection_manager.connection,
                        ch,
                        on_settled=_release_settled_slot,
                    )
                    start_gate = threading.Event()

                    def _process_delivery():
                        start_gate.wait()
                        return job_processor.process_message(
                            ack_channel,
                            method_frame,
                            body,
                            queue_name=consumed_queue,
                            cancellation_registry=(job_processor.cancellation_registry),
                        )

                    in_flight = executor.submit(_process_delivery)
                    start_gate.set()

                consumer_tags.append(
                    channel.basic_consume(
                        queue=queue_name,
                        on_message_callback=_on_message,
                        auto_ack=False,
                    )
                )

            self._mark_consumer_slot(1)
            subscribed = True
            Log.info(
                "AMQP worker subscribed to %s",
                ", ".join(queue_names),
            )
            while (
                not self.shutdown_requested
                or (in_flight is not None and not in_flight.done())
                or any(not future.done() for future in settled_futures)
            ):
                connection = connection_manager.connection
                if connection is None or connection.is_closed:
                    pending = [
                        future
                        for future in (in_flight, *settled_futures)
                        if future is not None and not future.done()
                    ]
                    if pending:
                        self.shutdown_requested = True
                        time.sleep(0.1)
                        continue
                    raise ConnectionError("RabbitMQ consumer connection closed")
                try:
                    connection.process_data_events(time_limit=0.25)
                except Exception:
                    pending = [
                        future
                        for future in (in_flight, *settled_futures)
                        if future is not None and not future.done()
                    ]
                    if pending:
                        self.shutdown_requested = True
                        while any(not future.done() for future in pending):
                            time.sleep(0.1)
                    raise
                pending_settlements = [
                    future
                    for future in (in_flight, *settled_futures)
                    if future is not None and not future.done()
                ]
                if pending_settlements:
                    # Real pika blocks for ``time_limit`` while servicing the
                    # I/O loop. Test/fallback connections may return
                    # immediately; yield briefly so a handler that just
                    # settled can finish without a hot process_data_events
                    # spin. The short timeout cannot deadlock a handler waiting
                    # for its thread-safe ACK callback because the I/O loop is
                    # serviced again on the next iteration.
                    concurrent.futures.wait(
                        pending_settlements,
                        timeout=0.01,
                        return_when=concurrent.futures.FIRST_COMPLETED,
                    )
                while settled_futures and settled_futures[0].done():
                    outcome = settled_futures.popleft().result()
                    self._record_worker_outcome(outcome, config)
                if in_flight is not None and in_flight.done():
                    outcome = in_flight.result()
                    in_flight = None
                    self._record_worker_outcome(outcome, config)
                if not self.shutdown_requested and self._should_stop(config):
                    self.shutdown_requested = True
        finally:
            executor.shutdown(wait=False, cancel_futures=True)
            if subscribed:
                self._mark_consumer_slot(-1)
            if getattr(channel, "is_open", False):
                for consumer_tag in consumer_tags:
                    with contextlib.suppress(Exception):
                        channel.basic_cancel(consumer_tag)
                with contextlib.suppress(Exception):
                    channel.close()

    def _should_stop(self, config: dict[str, Any]) -> bool:
        """Check if worker should stop due to configured limits.

        ``--max-jobs`` is a *terminal-attempt* cap, not a *successful-job*
        cap. Under a failure storm (poison-message stream, DB outage,
        misconfigured retention, etc.) every dequeue increments
        ``jobs_failed`` while ``jobs_processed`` stays at 0 — and the
        cap was never tripped, so the worker drained an unbounded
        number of jobs into the DLQ before --max-time eventually
        kicked in. Counting both completed and failed terminal
        attempts gives operators the safety bound they expect when
        load-testing or running short triage workers.
        """
        terminal_jobs = self.jobs_processed + self.jobs_failed
        max_jobs = config.get("max_jobs")
        if max_jobs and terminal_jobs >= max_jobs:
            self.info(
                f"🎯 Reached maximum job limit ({max_jobs}) "
                f"[processed={self.jobs_processed} failed={self.jobs_failed}]"
            )
            return True

        max_time = config.get("max_time")
        if max_time and (time.time() - self.start_time) >= max_time:
            self.info(f"⏰ Reached maximum runtime ({max_time} seconds)")
            return True

        return False

    def _get_runtime(self) -> str:
        """Get formatted runtime duration."""
        if not self.start_time:
            return "00:00:00"

        runtime_seconds = int(time.time() - self.start_time)
        hours, remainder = divmod(runtime_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

    def _show_final_stats(self):
        """Show enhanced worker statistics with job status breakdown."""
        total_jobs = self.jobs_processed + self.jobs_failed
        success_rate = (
            (self.jobs_processed / total_jobs * 100) if total_jobs > 0 else 100.0
        )

        self.info("\n📊 Final Worker Statistics:")
        self.info(f"   Runtime: {self._get_runtime()}")
        self.info(f"   Jobs Processed: {self.jobs_processed}")
        self.info(f"   Jobs Failed: {self.jobs_failed}")
        self.info(f"   Success Rate: {success_rate:.1f}%")

        # Show enhanced queue stats if available
        try:
            # Try to resolve Job model from container (framework agnostic)
            job_model = self._resolve_job_model()
            if job_model and hasattr(job_model, "get_queue_stats"):
                queue_display = getattr(self, "_queue_names_display", "default")
                stats = job_model.get_queue_stats(queue_display)
                self.info(f"\n📈 Current Queue Status ({queue_display}):")
                self.info(f"   Pending: {stats.get('pending_jobs', 0)}")
                self.info(f"   Processing: {stats.get('processing_jobs', 0)}")
                self.info(f"   Completed: {stats.get('completed_jobs', 0)}")
                self.info(f"   Cancelled: {stats.get('cancelled_jobs', 0)}")
                self.info(f"   Failed: {stats.get('failed_jobs', 0)}")
        except Exception as exc:
            _logger.debug("enhanced stats unavailable: %s", exc)

    def _resolve_job_model(self):
        """Resolve Job model from JobTracker."""
        import builtins

        if hasattr(builtins, "app"):
            app_instance = builtins.app()
            if app_instance and app_instance.has("JobTracker"):
                tracker = app_instance.make("JobTracker")
                return getattr(tracker, "job_model", None)
        return None

    def _run_main_loop(self, *args, **kwargs):
        """Main worker loop - called by MakesAutoReload on restart."""
        # Use stored parameters from store_restart_params
        if hasattr(self, "_restart_params") and self._restart_params:
            driver, queue, timeout, max_jobs, max_time = self._restart_params
        else:
            driver, queue, timeout, max_jobs, max_time = (
                args if args else (None, None, None, None, None)
            )

        # Prepare config with current parameters
        try:
            worker_config = self._prepare_config(
                driver, queue, timeout, max_jobs, max_time
            )
        except Exception as e:
            self.error(f"❌ Configuration error: {e}")
            raise

        # Show worker configuration
        self._show_config(worker_config)

        # Clean up connections before starting
        self._cleanup_connections_for_restart()

        # Reset counters for fresh start
        self.jobs_processed = 0
        self.jobs_failed = 0

        queue_driver = Queue.driver(worker_config["driver_name"])
        queue_driver.verify_runtime_health(
            worker_config["queue_names"],
            force=True,
        )
        from cara.observability import MetricsBase as _Metrics

        _Metrics.queue_worker_configured_queues.set(len(worker_config["queue_names"]))
        _Metrics.queue_worker_ready.set(0)

        # Run the worker
        self._run_worker(worker_config)

    def _cleanup_connections_for_restart(self):
        """Clean up connections specifically for restart - simple and effective."""
        try:
            from cara.facades.Queue import Queue

            # Simple approach: Just clear all references without trying to close broken connections
            drivers = config("queue.drivers", {})
            for driver_name in drivers:
                try:
                    driver = Queue.driver(driver_name)

                    # Just clear references - don't try to close broken connections
                    if hasattr(driver, "channel"):
                        driver.channel = None

                    if hasattr(driver, "connection"):
                        driver.connection = None

                    # Reset driver state
                    if hasattr(driver, "_connected"):
                        driver._connected = False

                except Exception:
                    continue

            # Force a small delay to let any pending operations complete
            import time

            time.sleep(0.1)

        except (ImportError, RuntimeError, AttributeError, OSError):
            pass

    def _cleanup_watching(self):
        """Cleanup file watching resources."""
        if hasattr(self, "command_watcher") and self.command_watcher:
            with contextlib.suppress(ImportError, RuntimeError, AttributeError, OSError):
                self.command_watcher.shutdown()
