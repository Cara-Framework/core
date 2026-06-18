"""
AMQP Queue Driver for the Cara framework.

Modern, clean implementation for RabbitMQ-based job queue management.
"""

from __future__ import annotations

import concurrent.futures
import json
import logging
import pickle
import random
import threading
import uuid
from typing import Any

import pendulum
import pika

from cara.exceptions import QueueDriverLibraryNotFoundException
from cara.facades import Log
from cara.observability import Trace as _Trace
from cara.queues.contracts.Queue import Queue
from cara.queues.job_instantiation import instantiate_job
from cara.queues.serializers.PickleJobSerializer import restricted_pickle_loads
from cara.queues.retry.policy import (
    DEFAULT_MAX_ATTEMPTS as _RETRY_DEFAULT_MAX_ATTEMPTS,
)
from cara.queues.retry.policy import (
    DEFAULT_RETRY_BACKOFF_SECONDS as _RETRY_DEFAULT_BACKOFF_SECONDS,
)
from cara.queues.retry.policy import (
    DEFAULT_RETRY_JITTER_FRACTION as _RETRY_DEFAULT_JITTER_FRACTION,
)
from cara.support.Console import HasColoredOutput


class AMQPDriver(HasColoredOutput, Queue):
    """
    AMQP-based queue driver for RabbitMQ.

    Features:
    - Reliable message delivery with publisher confirms
    - Job tracking with unique IDs
    - Integration with JobTracker for status updates
    - Persistent messages and durable queues
    - Bounded automatic retry on consumer-side failure (see
      ``_handle_failed_message``)
    """

    driver_name = "amqp"

    # Framework-level default retry policy — SINGLE-SOURCED in
    # ``cara.queues.retry.policy`` (the rationale for 1/5/30 + 25% jitter
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

        # Connection pool: idle (host:port → list[connection]) so a
        # thread that finished publishing can hand its connection
        # back instead of dropping the TCP socket. The pool is
        # process-global; thread-locals only point at a connection
        # while that thread is using one. Bounded so we don't
        # accumulate idle sockets on a load spike.
        self._pool: dict[str, list[Any]] = {}
        self._pool_lock = threading.Lock()
        self._publish_lock = threading.Lock()
        self._max_pool_per_url = int(options.get("amqp_pool_size", 8))

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

    def push(self, *jobs: Any, options: dict[str, Any]) -> str | list[str]:
        """Push jobs to queue and return job ID(s) for tracking."""
        merged_opts = {**self.options, **options}
        job_ids = []

        for job in jobs:
            # Generate unique job ID for tracking
            job_id = str(uuid.uuid4())
            job_ids.append(job_id)

            # Create job record in database via JobTracker
            db_job_id = self._create_job_record(job, job_id, merged_opts)

            # Prepare payload. ``attempts`` is the retry counter that
            # the consume() failure path increments and republishes;
            # carrying it inside the payload (instead of an AMQP
            # header) keeps the count alive across the delayed-retry
            # republish without depending on header preservation
            # through the delayed-plugin path.
            payload = {
                "obj": job,
                "args": merged_opts.get("args", ()),
                "callback": merged_opts.get("callback", "handle"),
                "created": pendulum.now(tz=merged_opts.get("tz", "UTC")),
                "job_id": job_id,
                "db_job_id": db_job_id,
                "attempts": int(
                    merged_opts.get("attempts", 0)
                    if merged_opts.get("attempts", 0) is not None
                    else 0
                ),
            }
            # Stash the current trace context in the payload (same rail
            # as ``attempts``) so the consumer re-parents the job's span
            # → one product = one trace across workers. No-op when off.
            payload["_otel"] = _Trace.inject({})

            try:
                self._connect_and_publish(payload, merged_opts)
            except (
                pika.exceptions.AMQPConnectionError,
                pika.exceptions.StreamLostError,
                BrokenPipeError,
                ConnectionResetError,
                ConnectionRefusedError,
                OSError,
            ):
                # Retry once on connection/stream error — the pooled
                # connection may have been dropped by the broker (idle
                # timeout, restart, etc.). _connect_and_publish will
                # _discard_thread_connection on failure, so the retry
                # opens a fresh TCP socket.
                self._connect_and_publish(payload, merged_opts)

        return job_ids[0] if len(job_ids) == 1 else job_ids

    def batch(self, *jobs: Any, options: dict[str, Any]) -> None:
        """Batch push: push all jobs at once."""
        self.push(*jobs, options=options)

    def chain(self, jobs: list, options: dict[str, Any]) -> None:
        """Chain jobs: dispatch via ChainRunnerJob for true sequential execution.

        Previous implementation pushed all jobs in parallel which violated
        chain semantics (Job₂ should only run after Job₁ succeeds).
        """
        if not jobs:
            return

        from cara.queues.Chain import Chain

        Chain(jobs).dispatch()

    def schedule(self, job: Any, when: Any, options: dict[str, Any]) -> str | list[str]:
        """Schedule job for future execution using AMQP delayed plugin."""
        merged_opts = {**self.options, **options}

        # Calculate delay in milliseconds. ``float_timestamp`` is a pendulum
        # *property*, not a method — calling it as a method raises
        # TypeError: 'float' object is not callable. With the previous code
        # every scheduled AMQP job blew up at enqueue time.
        delay_ms = int(
            pendulum.parse(str(when)).float_timestamp * 1000
            - pendulum.now("UTC").float_timestamp * 1000
        )

        # Per-MESSAGE header for the RabbitMQ delayed-message-exchange
        # plugin. Pre-fix this was injected into ``connection_options``,
        # which ``_build_url`` then appended as URL query params used
        # as the connection-pool key. Result: every distinct
        # ``delay_ms`` value produced a unique pool key, so the
        # scheduled / retry hot path NEVER reused a pooled connection
        # and paid the full 5-50ms TCP+TLS handshake per publish
        # (defeating the explicit pool design — see
        # ``_connect_and_publish``'s docstring). The fix routes the
        # header through a dedicated ``message_headers`` opt key that
        # ``_connect_and_publish_locked`` reads at publish time but
        # ``_build_url`` ignores at connection time.
        merged_opts["message_headers"] = {
            **merged_opts.get("message_headers", {}),
            "x-delay": max(delay_ms, 0),
        }

        return self.push(job, options=merged_opts)

    def later(
        self, delay: int | pendulum.Duration, job: Any, options: dict[str, Any] = None
    ) -> str | list[str]:
        """
        Schedule a job to be executed after a delay.

        Uses AMQP message TTL and delayed plugin for scheduling.
        Implements exponential backoff for retries.

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

        # Merge options
        merged_opts = {**self.options, **options}

        # Calculate when job should run
        when = pendulum_module.now(tz=merged_opts.get("tz", "UTC")).add(
            seconds=delay_seconds
        )

        return self.schedule(job, when, merged_opts)

    def retry(
        self,
        job: Any,
        options: dict[str, Any] = None,
        attempts: int = 3,
        backoff: str = "exponential",
    ) -> str | list[str] | None:
        """
        Retry a failed job with optional exponential backoff.

        Implements exponential backoff retry strategy similar to Laravel.
        Uses AMQP dead letter exchanges and TTL for delayed retries.

        Args:
            job: Job instance to retry
            options: Queue options
            attempts: Maximum retry attempts (default: 3)
            backoff: Backoff strategy - 'exponential', 'linear', or int for fixed delay in seconds

        Returns:
            Job ID

        Example:
            driver.retry(my_job, attempts=5, backoff='exponential')
        """
        if options is None:
            options = {}

        # Get current attempt count
        attempts_made = options.get("attempts", 0)

        if attempts_made >= attempts:
            # Max attempts reached, send to dead letter
            Log.error("Job %s exceeded max retry attempts (%s). Sending to dead letter queue.", job.__class__.__name__, attempts)
            self._send_to_dead_letter(job, options, attempts_made)
            return None

        # Calculate delay based on backoff strategy
        if isinstance(backoff, str) and backoff == "exponential":
            # Exponential backoff: 1s, 2s, 4s, 8s, etc.
            delay_seconds = 2**attempts_made
        elif isinstance(backoff, str) and backoff == "linear":
            # Linear backoff: 1s, 2s, 3s, 4s, etc.
            delay_seconds = (attempts_made + 1) * 60  # 1 minute per attempt
        else:
            # Fixed delay (assume it's an int)
            delay_seconds = int(backoff) if backoff else 300  # Default 5 minutes

        attempts_made += 1
        Log.info("Retrying job %s (attempt %s/%s) with %ss delay", job.__class__.__name__, attempts_made, attempts, delay_seconds)

        # Update options with retry info
        retry_opts = {
            **options,
            "attempts": attempts_made,
            "retry_backoff": backoff,
            "max_attempts": attempts,
        }

        # Schedule retry with delay
        return self.later(delay_seconds, job, retry_opts)

    @staticmethod
    def _drive_to_completion(result: Any) -> Any:
        """Run a coroutine return value to completion.

        Pika's ``BlockingConnection`` callback (``on_message``) is
        synchronous. When the job's ``handle`` is ``async def``,
        ``Container.call`` returns a coroutine — we must drive it
        here or it gets garbage-collected un-run and the broker is
        told (by ``basic_ack``) that the work is done.

        Non-coroutine return values pass through unchanged so this
        helper is safe to call from the hot path regardless of
        whether the bound callback is sync or async.
        """
        import asyncio
        import inspect

        if not inspect.iscoroutine(result):
            return result

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            # Normal path: pika's consumer thread has no asyncio
            # loop. ``asyncio.run`` creates one for the duration of
            # this single job (matches QueueWorkCommand).
            return asyncio.run(result)

        # An asyncio loop is already running on this thread, which
        # means someone wired the AMQP consumer into an async
        # context. We can't nest ``asyncio.run`` and the pika
        # callback can't yield, so drive the coroutine on a fresh
        # loop in a dedicated thread and block until it finishes —
        # awkward, but the only correct option short of refusing
        # the call entirely.
        import threading

        container: dict[str, Any] = {}

        def _runner() -> None:
            try:
                container["value"] = asyncio.run(result)
            except BaseException as exc:  # noqa: BLE001 — re-raised below
                container["error"] = exc

        worker = threading.Thread(target=_runner, daemon=True)
        worker.start()
        worker.join()
        if "error" in container:
            raise container["error"]
        return container.get("value")

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
        except (TypeError, ValueError):
            fraction = self.DEFAULT_RETRY_JITTER_FRACTION
        if fraction <= 0:
            return base_delay
        # Clamp the spread so a bad config (1.0+) doesn't double the
        # base delay or push the lower end below zero.
        fraction = min(fraction, 0.9)
        swing = base_delay * fraction
        jitter = random.uniform(-swing, swing)
        return max(1, int(round(base_delay + jitter)))

    def _handle_failed_message(
        self,
        instance: Any,
        payload: dict[str, Any],
        exc: Exception,
        options: dict[str, Any],
    ) -> bool:
        """Route a failed in-flight message to retry or to DLX.

        Returns:
            ``True`` when a retry was scheduled (caller must NOT
            release the ``UniqueJob`` lock — the retry is in flight
            and needs the lock held until it actually runs).
            ``False`` when the message was terminally dead-lettered
            (caller MUST release the lock so the next legitimate
            dispatch can proceed).

        The decision is hard-bounded by the per-class ``max_attempts``
        (instance class attribute; default
        ``DEFAULT_MAX_ATTEMPTS``). A class can also override
        ``retry_backoff`` with a tuple/list of per-attempt delays in
        seconds (default ``DEFAULT_RETRY_BACKOFF_SECONDS = (1, 5,
        30)``).

        Conventions:

        * The first delivery has ``attempts = 0`` in the payload. On
          failure we count this as attempt #1 and republish with
          ``attempts = 1`` after the configured delay.
        * When ``attempts >= max_attempts`` we DLX immediately so the
          DLQ recovery cron (``CleanDeadLetterJob``) can re-decide
          whether to revive the job manually.
        * ``do_not_retry`` on the exception (e.g. ``PermanentScrapeError``
          flagged by upstream scrape jobs) shortcuts straight to DLX —
          no point burning the 1+5+30 seconds on a 404 that won't
          come back. Subclassing ``Exception`` with
          ``do_not_retry = True`` is the opt-in.
        * If the message couldn't even be unpickled (``instance is
          None``) we have no class to read ``max_attempts`` from, so
          we DLX immediately. Re-running an unparseable message is
          pointless and would just thrash.
        """
        # Carry the error class + message through to the DLX payload
        # so the recovery cron (CleanDeadLetterJob) can classify
        # transient vs permanent without re-running the job.
        dlx_options = {
            **options,
            "error": {
                "class": type(exc).__name__,
                "message": str(exc)[:500],
                "do_not_retry": bool(getattr(exc, "do_not_retry", False)),
            },
        }

        if instance is None:
            # Pickle/instantiate failure — nothing to retry against.
            try:
                self._send_to_dead_letter(
                    job=None,
                    options=dlx_options,
                    attempts=int(payload.get("attempts", 0)),
                )
            except Exception as e:
                Log.warning("_handle_failed_message: DLX after unpickle failure raised: %s", e)
            return False

        if getattr(exc, "do_not_retry", False):
            Log.error("Job %s raised %s marked do_not_retry — sending straight to DLX", instance.__class__.__name__, type(exc).__name__)
            self._send_to_dead_letter(
                job=instance,
                options=dlx_options,
                attempts=int(payload.get("attempts", 0)),
            )
            return False

        attempts_made = int(payload.get("attempts", 0)) + 1
        max_attempts = int(
            getattr(instance, "max_attempts", None) or self.DEFAULT_MAX_ATTEMPTS
        )

        if attempts_made >= max_attempts:
            Log.error("Job %s exhausted %s attempts (last error: %s: %s). Routing to DLX.", instance.__class__.__name__, max_attempts, type(exc).__name__, exc)
            self._send_to_dead_letter(
                job=instance, options=dlx_options, attempts=attempts_made
            )
            return False

        backoff_schedule = getattr(
            instance,
            "retry_backoff",
            self.DEFAULT_RETRY_BACKOFF_SECONDS,
        )
        # Index by attempt count; clamp to the last entry so jobs
        # with custom higher ``max_attempts`` than the schedule
        # length still get a sane delay instead of an IndexError.
        if not isinstance(backoff_schedule, (list, tuple)) or not backoff_schedule:
            backoff_schedule = self.DEFAULT_RETRY_BACKOFF_SECONDS
        idx = min(attempts_made - 1, len(backoff_schedule) - 1)
        base_delay = int(backoff_schedule[idx])
        delay_seconds = self._apply_retry_jitter(base_delay, instance)

        Log.info("Job %s attempt %s/%s failed (%s: %s). Retrying in %ss (base %ss + jitter).", instance.__class__.__name__, attempts_made, max_attempts, type(exc).__name__, exc, delay_seconds, base_delay)

        retry_options = {
            **options,
            "attempts": attempts_made,
        }
        try:
            self.later(delay_seconds, instance, retry_options)
            return True
        except Exception as republish_exc:
            # Republish failed — we've already acked the original
            # delivery, so the job is lost unless we surface this.
            # Best-effort DLX hop so the message at least survives
            # in the dead-letter store for manual recovery.
            Log.error("Job %s: retry republish failed (%s). Falling back to DLX so the payload isn't lost.", instance.__class__.__name__, republish_exc)
            try:
                self._send_to_dead_letter(
                    job=instance,
                    options={**dlx_options, **retry_options},
                    attempts=attempts_made,
                )
            except Exception:
                Log.error("Job %s: DLX fallback also failed — payload IS lost", instance.__class__.__name__, exc_info=True)
            return False

    def _send_to_dead_letter(
        self, job: Any, options: dict[str, Any], attempts: int
    ) -> None:
        """
        Send a permanently failed job to dead letter queue.

        Args:
            job: Failed job instance
            options: Queue options
            attempts: Number of attempts made
        """
        # Same thread-safety rationale as _connect_and_publish: pika
        # instance state (self.connection/self.channel) must not be
        # mutated by concurrent threads.
        with self._publish_lock:
            try:
                self._connect(self.options)

                # Prepare dead letter message
                payload = {
                    "obj": job,
                    "args": options.get("args", ()),
                    "callback": options.get("callback", "handle"),
                    "failed_at": pendulum.now(
                        tz=options.get("tz", "UTC")
                    ).to_datetime_string(),
                    "attempts": attempts,
                    "error": options.get("error"),
                }

                # Serialize
                serializer = options.get("serializer", "pickle")
                if serializer == "json":
                    body = json.dumps(payload, default=str).encode("utf-8")
                else:
                    body = pickle.dumps(payload)

                # Publish to dead letter exchange.
                #
                # MUST mirror the DLX name that queue declarations use
                # (``_connect_and_publish_locked``):
                #   exchange non-empty → ``{exchange}.dlx``
                #   exchange empty/"" → ``dead.letter.dlx``
                #
                # Pre-fix this line was:
                #   dlx_name = f'{options.get("exchange", "default")}.dlx'
                # When the driver config has ``exchange: ""`` (the default
                # exchange — every non-topic-routed job), the expression
                # evaluated to ``".dlx"`` — an exchange that was never
                # declared. The publish raised ``ChannelClosedByBroker``
                # (404), the outer ``except Exception`` swallowed it, and
                # the payload was silently lost instead of landing in the
                # DLQ. The queue-side DLX wiring always pointed at
                # ``dead.letter.dlx``, so the mismatch meant every
                # default-exchange job that exhausted its retry budget
                # vanished.
                _dlx_exchange = options.get("exchange", "")
                dlx_name = f"{_dlx_exchange}.dlx" if _dlx_exchange else "dead.letter.dlx"
                dlq_routing_key = f"dead.{options.get('queue', 'default')}"

                self.channel.basic_publish(
                    exchange=dlx_name,
                    routing_key=dlq_routing_key,
                    body=body,
                    properties=pika.BasicProperties(
                        delivery_mode=2,  # Persistent
                        headers={
                            "x-death-count": attempts,
                            "x-original-job": job.__class__.__name__,
                        },
                    ),
                )

                # Promote dead-letter to ERROR so log aggregators / Sentry
                # surface it. Previously logged at INFO and silently filled
                # the DLQ — ops only learned about job exhaustion by
                # querying the DB after-the-fact.
                # NOTE: cara's Log.error() does not accept an ``extra=``
                # kwarg (TypeError on every call). The structured fields
                # are folded into the message string instead.
                Log.error("Job dead-lettered after %s attempts: %s → %s (job_class=%s, attempts=%s, dlq_routing_key=%s)", attempts, job.__class__.__name__, dlq_routing_key, job.__class__.__name__, attempts, dlq_routing_key)

                # Best-effort metric increment so dashboards / Prometheus
                # alerts can fire when DLQ rate spikes.
                try:
                    from app.support.Metrics import Metrics  # type: ignore[attr-defined]

                    Metrics.queue_jobs_dead_lettered_total.labels(
                        job=job.__class__.__name__,
                    ).inc()
                except (ImportError, RuntimeError, AttributeError):
                    pass

            except Exception:
                Log.error(
                    "Failed to send job to dead letter queue",
                    exc_info=True,
                )
            finally:
                # Pre-fix the close lines lived inside the try block
                # *after* publish + Log.error. Any exception above
                # (broker reset, serializer error, even the legacy
                # ``Log.error(extra=...)`` TypeError) skipped them and
                # left the thread-local channel/connection bound to a
                # dead handle. The next call from this thread would
                # reuse the stale state and either fail again or open
                # a new connection without closing the old one,
                # eventually exhausting the broker's connection limit.
                try:
                    if self.channel is not None:
                        self.channel.close()
                except (OSError, ConnectionError, RuntimeError, AttributeError):
                    pass
                try:
                    if self.connection is not None:
                        self.connection.close()
                except (OSError, ConnectionError, RuntimeError, AttributeError):
                    pass
                self.channel = None
                self.connection = None

    def consume(self, options: dict[str, Any]) -> None:
        """Consume jobs from RabbitMQ. **DEPRECATED / legacy path.**

        The live production consumer is the ``queue:work`` command
        (``QueueWorkCommand``); it spawns its own worker threads and does
        NOT route through this method. This single-thread loop is kept
        only for back-compat and the driver's unit tests — prefer
        ``queue:work`` for all new code. Retry/DLX defaults are now
        single-sourced from ``cara.queues.retry.policy`` so this path and
        the production worker cannot diverge.

        Each invocation runs a single-thread blocking consume loop.
        ``QueueWorkCommand`` typically spawns one worker thread per
        ``--concurrency`` so a process with concurrency=8 has 8
        independent consumers, each with its own connection.

        Behaviour:
          * Manual ack — message is only acked after the job
            handler returns successfully. A worker crash mid-process
            requeues the message via the broker's redelivery
            machinery.
          * Failed jobs nack with ``requeue=False`` so the broker's
            DLX (configured on the queue) catches them. The driver
            also calls ``failed()`` on the job instance for app-side
            handling.
          * Releases the UniqueJob lock and dispatches batch
            completion lifecycle on every completion path
            (success / failure / cancellation).
        """
        Log.warning(
            "AMQPDriver.consume() is the legacy consumer loop and is not "
            "used in production — the live worker is the 'queue:work' "
            "command (QueueWorkCommand). Kept for back-compat/tests only."
        )
        merged_opts = {**self.options, **options}
        queue_name = merged_opts.get("queue", "default")

        try:
            import pika
        except ImportError:
            raise QueueDriverLibraryNotFoundException(
                "pika is required for AMQPDriver. Install with: pip install pika"
            )

        prefetch = int(merged_opts.get("prefetch", 1))

        # Build a dedicated consumer connection. We don't pool
        # consumers — they're long-lived and there's only one per
        # worker thread.
        connection, channel = self._open_new_connection(merged_opts)
        # Idempotent declare — same logic as the publish path so
        # consume against an existing TTL-mismatched queue still works.
        exchange_name = merged_opts.get("exchange", "")
        # Default switched to ``None`` (no per-queue TTL) to match the
        # canonical queues declared by ``dev:reset --dlx``. The previous
        # 86400000ms (24h) default produced
        # ``PRECONDITION_FAILED - inequivalent arg 'x-message-ttl'``
        # on every active declare against a queue that already existed
        # without the TTL arg — the driver then dropped to passive
        # declare and consumers silently failed to register against
        # the queue, so the worker bound to ALL named queues but never
        # received a single ``ValidateProductJob`` / ``ConsolidateProductJob``
        # event. Explicit ``message_ttl=N`` in caller opts still wins.
        message_ttl = merged_opts.get("message_ttl")
        queue_args: dict[str, object] = {
            "x-dead-letter-exchange": (
                f"{exchange_name}.dlx" if exchange_name else "dead.letter.dlx"
            ),
            "x-dead-letter-routing-key": f"dead.{queue_name}",
        }
        if message_ttl is not None:
            queue_args["x-message-ttl"] = message_ttl
        try:
            channel.queue_declare(queue=queue_name, durable=True, arguments=queue_args)
        except pika.exceptions.ChannelClosedByBroker as exc:
            if getattr(exc, "reply_code", None) != 406:
                raise
            channel = connection.channel()
            channel.confirm_delivery()
            channel.queue_declare(queue=queue_name, durable=True, passive=True)

        channel.basic_qos(prefetch_count=prefetch)

        def on_message(ch, method, properties, body):
            instance = None
            try:
                payload = restricted_pickle_loads(body)
                raw = payload.get("obj")
                callback_name = payload.get("callback", "handle")
                args = payload.get("args", ())
                instance = instantiate_job(self.application, raw, args)
                if instance is not None:
                    # Carry the dispatcher's trace context onto the job
                    # so BaseJob.handle re-parents its span (Obs-4).
                    instance._otel_carrier = payload.get("_otel")

                # Propagate tracing context so logs show real IDs
                # instead of [job_id=unknown].
                from cara.context import ExecutionContext as _EC

                _job_id = getattr(instance, "job_id", None)
                _batch_id = getattr(instance, "batch_id", None)
                _corr_id = getattr(instance, "correlation_id", None)
                if _job_id and _job_id != "unknown":
                    _EC.set_job_id(_job_id)
                if _batch_id:
                    _EC.set_batch_id(_batch_id)
                if _corr_id:
                    _EC.set_correlation_id(_corr_id)

                method_to_call = getattr(instance, callback_name, None)
                if not callable(method_to_call):
                    raise AttributeError(
                        f"Callback '{callback_name}' not found on {instance!r}"
                    )

                # Enforce job-level timeout — mirrors the timeout
                # discipline in ``QueueWorkCommand.process_message``.
                # Without this, a hung job (stuck HTTP call, dead DB
                # pool) blocks the consumer thread indefinitely and
                # no further messages are processed from this queue.
                _job_timeout = (
                    getattr(instance, "timeout", 300) if instance else 300
                )

                def _run_job():
                    if hasattr(self.application, "call"):
                        r = self.application.call(method_to_call, *args)
                    else:
                        r = method_to_call(*args) if args else method_to_call()
                    # ``BaseJob.handle`` is ``async def``.
                    # Container.call returns the bare coroutine —
                    # ``_drive_to_completion`` runs it to completion
                    # (matches QueueWorkCommand.process_message).
                    AMQPDriver._drive_to_completion(r)

                _executor = concurrent.futures.ThreadPoolExecutor(
                    max_workers=1
                )
                _future = _executor.submit(_run_job)
                try:
                    _future.result(timeout=_job_timeout)
                except concurrent.futures.TimeoutError:
                    _future.cancel()
                    _cls = (
                        instance.__class__.__name__ if instance else "?"
                    )
                    raise TimeoutError(
                        f"Job {_cls} exceeded timeout of {_job_timeout}s"
                    )
                finally:
                    _executor.shutdown(wait=True, cancel_futures=True)

                ch.basic_ack(delivery_tag=method.delivery_tag)
                self._dispatch_batch_completion(instance, None)
                # Success terminates the job's slot — release the
                # UniqueJob lock so subsequent legitimate dispatches
                # for the same ``unique_id`` can proceed. The failure
                # branch handles its own release decision (held during
                # retries, released on DLX).
                self._release_unique_lock_if_any(instance)
                self.info(f"AMQPDriver: job processed successfully, queue={queue_name}")
            except Exception as exc:
                self.danger(f"AMQPDriver: job processing failed: {exc}")
                # Bounded automatic retry. Pre-fix the consumer nacked
                # to DLX on first failure, so every transient outage
                # (network blip, DB pool exhaustion, broker
                # reconnect, scrape.do 5xx) lost the job permanently.
                # The retry decision honours the job's own
                # ``max_attempts`` knob (default 3), falls back to a
                # per-class ``retry_backoff`` tuple, and only routes
                # to DLX when the budget is exhausted. The ack-first
                # discipline avoids a duplicate delivery if the
                # republish raises.
                try:
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except (OSError, ConnectionError, RuntimeError):
                    pass
                retry_scheduled = False
                try:
                    retry_scheduled = bool(
                        self._handle_failed_message(
                            instance=instance,
                            payload=payload if "payload" in dir() else {},
                            exc=exc,
                            options=merged_opts,
                        )
                    )
                except Exception as retry_exc:
                    self.danger(f"AMQPDriver: retry/dead-letter path raised: {retry_exc}")
                # Job-side ``failed`` hook for app-level cleanup —
                # fires once per *attempt* (the framework already
                # logs the retry separately), so listeners that care
                # about every failure observe it. Hooks that should
                # only fire on terminal failure can check the
                # ``attempts`` field on the payload they receive.
                if instance is not None and hasattr(instance, "failed"):
                    try:
                        instance.failed(payload if "payload" in dir() else {}, str(exc))
                    except Exception as inner:
                        self.danger(f"AMQPDriver: failed() raised: {inner}")
                self._dispatch_batch_completion(instance, exc)
                # Release the UniqueJob lock ONLY on terminal failure.
                # If a retry was scheduled, the delayed-publish payload
                # carries the same ``unique_id`` and the retry must
                # observe the lock as still held — otherwise a
                # concurrent dispatch in the backoff window wins the
                # lock and we get a duplicate execution when the
                # retry fires. The lock TTL (``unique_for``, default
                # 3600s) caps the worst case.
                if not retry_scheduled:
                    self._release_unique_lock_if_any(instance)

        channel.basic_consume(queue=queue_name, on_message_callback=on_message)
        self.info(f"AMQPDriver: consuming from queue='{queue_name}'")

        # Graceful shutdown: SIGTERM (container stop, deploy) and SIGINT
        # (Ctrl-C) trigger stop_consuming(). The in-flight on_message
        # callback finishes and acks before the channel closes — no
        # message loss. Without this, SIGTERM kills the process mid-job
        # and the message is nacked back to the queue (or lost if acked
        # before completion).
        import signal

        def _graceful_stop(signum, frame):
            sig_name = (
                signal.Signals(signum).name if hasattr(signal, "Signals") else str(signum)
            )
            self.info(f"AMQPDriver: received {sig_name}, stopping consumer gracefully…")
            try:
                channel.stop_consuming()
            except (OSError, ConnectionError, RuntimeError):
                pass

        prev_term = signal.signal(signal.SIGTERM, _graceful_stop)
        prev_int = signal.signal(signal.SIGINT, _graceful_stop)

        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            channel.stop_consuming()
        finally:
            # Restore original handlers so nested consumers or
            # post-shutdown code isn't affected.
            signal.signal(signal.SIGTERM, prev_term)
            signal.signal(signal.SIGINT, prev_int)
            try:
                channel.close()
                connection.close()
            except (OSError, ConnectionError, RuntimeError, AttributeError):
                pass

    @staticmethod
    def _release_unique_lock_if_any(instance) -> None:
        if instance is None:
            return
        try:
            from cara.queues.contracts import UniqueJob

            if isinstance(instance, UniqueJob):
                UniqueJob.release_unique_lock(instance.unique_id())
        except (ImportError, ConnectionError, TimeoutError, OSError, RuntimeError):
            pass

    @staticmethod
    def _dispatch_batch_completion(instance, exception=None) -> None:
        if instance is None:
            return
        try:
            from cara.queues.Batch import auto_dispatch_batch_completion

            auto_dispatch_batch_completion(instance, exception)
        except (ImportError, OSError, ConnectionError, RuntimeError):
            pass

    def _create_job_record(self, job, job_id: str, opts: dict[str, Any]) -> int | None:
        """Create job record via JobTracker for consistent tracking."""
        try:
            tracker = self._resolve_job_tracker()
            if not tracker:
                return None

            # Get job queue
            queue_name = (
                job.queue
                if hasattr(job, "queue") and job.queue
                else opts.get("queue", "default")
            )

            # Get job class info
            job_name = job.__class__.__name__
            job_class = f"{job.__class__.__module__}.{job.__class__.__name__}"

            # Extract job parameters for payload
            from cara.queues import Bus

            payload = Bus.get_dispatch_params(job)

            # Create job record
            db_job_id = tracker.create_sync_job_record(
                job_name=job_name,
                job_class=job_class,
                queue=queue_name,
                payload=payload,
                metadata={"job_id": job_id, "driver": "amqp"},
            )

            return db_job_id

        except Exception as e:
            Log.warning("Failed to create job record: %s", e)
            return None

    def _resolve_job_tracker(self):
        """Resolve JobTracker from container."""
        if self.application and self.application.has("JobTracker"):
            return self.application.make("JobTracker")
        return None

    def declare_dead_letter_exchange(self, exchange_name: str = "dead.letter") -> None:
        """
        Declare RabbitMQ dead letter exchange and queues.

        Creates DLX exchange and binds dead letter queues for failed jobs.

        Args:
            exchange_name: Base exchange name (default: "dead.letter")
        """
        try:
            # Use the driver's own credentials — ``_connect({})`` would
            # build an unauthenticated URL (empty username/password) and
            # the broker refuses with ACCESS_REFUSED, leaving the DLX
            # undeclared and every nack/TTL-expiry silently dropped.
            self._connect(self.options)

            # Declare dead letter exchange
            dlx_name = f"{exchange_name}.dlx"
            self.channel.exchange_declare(
                exchange=dlx_name, exchange_type="topic", durable=True
            )

            # Declare dead letter queue. Name MUST match every
            # consumer-side reader (``replay_dead_letter`` /
            # ``get_dead_letter_messages`` /
            # ``services.app.support.QueueNames.DEAD_LETTER``) —
            # pre-fix this declared ``{exchange_name}.queue`` so any
            # deployment that called the method created a DLQ no
            # reader looked at, and the canonical
            # ``dead.letter.queue`` stayed empty while the broker
            # filled up unread.
            #
            # NO message-TTL argument: the DLQ exists specifically
            # to preserve failed messages for triage. A 24h ceiling
            # would silently DROP every dead-lettered message older
            # than 24h if ``CleanDeadLetterJob`` is paused, broken,
            # or slower than the failure rate for one rotation —
            # with no further DLX on the DLQ, expired messages just
            # evaporate. If storage capping is ever needed it
            # should be paired with another DLX and documented
            # inline.
            dlq_name = "dead.letter.queue"
            self.channel.queue_declare(
                queue=dlq_name,
                durable=True,
            )

            # Bind queue to DLX
            self.channel.queue_bind(
                exchange=dlx_name, queue=dlq_name, routing_key="dead.*"
            )

            Log.info("Dead letter exchange configured: %s", dlx_name)

            # Close connection and DROP the references. Leaving stale,
            # already-closed handles on self.channel/self.connection poisons
            # the thread-local publish path: the first dispatch's
            # _discard_thread_connection then calls .close() on an
            # already-closed channel. Nulling here fixes it at the source so
            # the next publish opens a fresh connection.
            try:
                self.channel.close()
                self.connection.close()
            except (OSError, ConnectionError, RuntimeError, AttributeError):
                pass
            self.channel = None
            self.connection = None

        except Exception as e:
            Log.error("Failed to declare dead letter exchange: %s", e)

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

                # Decode payload
                try:
                    payload = restricted_pickle_loads(body)
                except (pickle.UnpicklingError, ImportError, AttributeError, EOFError, ValueError):
                    try:
                        payload = json.loads(body.decode("utf-8"))
                    except (json.JSONDecodeError, UnicodeDecodeError, ValueError):
                        payload = {"raw": body.decode("utf-8", errors="ignore")}

                messages.append(
                    {
                        "delivery_tag": method.delivery_tag,
                        "routing_key": method.routing_key,
                        "redelivered": method.redelivered,
                        "exchange": method.exchange,
                        "headers": dict(properties.headers or {}),
                        "timestamp": properties.timestamp,
                        "payload": payload,
                    }
                )

                # Don't consume - requeue the message
                self.channel.basic_nack(method.delivery_tag, requeue=True)

            # Close connection
            try:
                self.channel.close()
                self.connection.close()
            except (OSError, ConnectionError, RuntimeError, AttributeError):
                pass

        except Exception as e:
            Log.error("Failed to get dead letter messages: %s", e)

        return messages

    def replay_dead_letter(self, queue_name: str, message_id: str | None = None) -> int:
        """
        Replay dead letter messages back to original queue.

        Args:
            queue_name: Original queue to replay to
            message_id: Specific message ID to replay, or None for all

        Returns:
            Number of messages replayed
        """
        dlq_name = "dead.letter.queue"
        replayed = 0

        try:
            self._connect(self.options)

            while True:
                method, properties, body = self.channel.basic_get(
                    dlq_name, auto_ack=False
                )

                if method is None:
                    break

                # Check if this is the message to replay
                headers = dict(properties.headers or {})
                msg_id = headers.get("message_id")

                if message_id is None or msg_id == message_id:
                    # Republish to original queue
                    self.channel.basic_publish(
                        exchange="",
                        routing_key=queue_name,
                        body=body,
                        properties=pika.BasicProperties(
                            delivery_mode=2,
                            headers=headers,
                        ),
                    )
                    replayed += 1
                    Log.info("Replayed message %s to %s", msg_id, queue_name)
                    # ACK MUST happen here, before any ``break``.
                    # Pre-fix the targeted-match branch broke out of
                    # the loop and the trailing ``basic_ack`` below
                    # never ran — pika treated the delivery as
                    # in-flight, the broker redelivered on connection
                    # close, and the job stayed in the DLQ for the
                    # next ``CleanDeadLetterJob`` sweep to re-replay
                    # forever.
                    self.channel.basic_ack(method.delivery_tag)
                    if message_id is not None:
                        break
                else:
                    # Targeted scan, non-matching message. MUST NOT
                    # ack — that removes the message from the DLQ
                    # silently. Pre-fix the trailing
                    # ``basic_ack(method.delivery_tag)`` ran on every
                    # iteration regardless of match, so a targeted
                    # replay for one stuck job silently discarded
                    # every other DLQ message in the peek window
                    # (pure data loss). Nack with requeue=True so
                    # the broker keeps the message available for the
                    # next ``CleanDeadLetterJob`` scan.
                    self.channel.basic_nack(method.delivery_tag, requeue=True)

        except Exception as e:
            Log.error("Failed to replay dead letter messages: %s", e)
        finally:
            # Pre-fix the close lines sat after the loop *inside* the
            # try block: any exception mid-loop (broker hiccup during
            # basic_publish / basic_ack) jumped past them and leaked
            # the channel/connection bound to the thread-local. Drain
            # the handles unconditionally and clear thread-local state
            # so the next call opens fresh.
            try:
                if self.channel is not None:
                    self.channel.close()
            except (OSError, ConnectionError, RuntimeError, AttributeError):
                pass
            try:
                if self.connection is not None:
                    self.connection.close()
            except (OSError, ConnectionError, RuntimeError, AttributeError):
                pass
            self.channel = None
            self.connection = None

        return replayed

    def _connect_and_publish(self, payload: Any, opts: dict[str, Any]) -> None:
        """Connect to RabbitMQ and publish message.

        Connection management — was: open + publish + close. Every
        publish was a fresh TCP+TLS handshake which dominated latency
        (5-50ms for the round trip vs. <1ms for the actual publish),
        and serialised every publish across the worker process behind
        ``_publish_lock`` because pika's BlockingConnection isn't
        thread-safe.

        Now: each thread keeps its own pooled connection (pika's
        BlockingConnection is fine when used from a single thread).
        On publish we either reuse the thread-local connection (most
        common), grab one from the cross-thread pool, or open a new
        one. After publish the connection goes back to the pool for
        reuse. Truly concurrent across threads, no global lock.
        """
        url = self._build_url(opts)
        self._acquire_thread_connection(url, opts)
        try:
            self._connect_and_publish_locked(payload, opts)
        except Exception:
            # On error drop this connection — it may be in a bad
            # state. The next publish opens a fresh one.
            self._discard_thread_connection()
            raise
        else:
            self._return_thread_connection(url)

    def _connect_and_publish_locked(self, payload: Any, opts: dict[str, Any]) -> None:
        """Inner publish path — assumes ``self.channel`` / ``self.connection``
        are bound to this thread by the caller."""

        # Get queue name from job or options
        job = payload.get("obj")
        queue_name = (
            job.queue
            if hasattr(job, "queue") and job.queue
            else opts.get("queue", "default")
        )

        # Declare queue with dead letter exchange support.
        #
        # ``message_ttl`` default switched to ``None`` (no per-queue TTL)
        # to match the canonical queues declared by ``dev:reset --dlx``.
        # The 86400000ms (24h) default ran into
        # ``PRECONDITION_FAILED - inequivalent arg 'x-message-ttl'``
        # on every active declare against a queue that already existed
        # without the TTL arg — passive declare succeeded but consumer
        # binding silently failed, leaving the worker bound to every
        # named queue while no ``Validate``/``Standardize``/``Consolidate``
        # ProductJob ever reached it. Explicit ``message_ttl=N`` via
        # caller opts still wins.
        exchange_name = opts.get("exchange", "")
        message_ttl = opts.get("message_ttl")

        queue_args: dict[str, object] = {
            "x-dead-letter-exchange": f"{exchange_name}.dlx"
            if exchange_name
            else "dead.letter.dlx",
            "x-dead-letter-routing-key": f"dead.{queue_name}",
        }
        if message_ttl is not None:
            queue_args["x-message-ttl"] = message_ttl

        # Idempotent declare: if the queue was originally created with
        # different args (e.g. older codebase or manual setup left it
        # without x-message-ttl), an active declare raises
        # PRECONDITION_FAILED (406) and kills the channel. We swallow
        # that once, reopen the channel, and fall back to passive
        # declare — the queue already exists so we can publish anyway.
        # This prevents TTL-drift from wedging every dispatcher forever.
        #
        # Per-channel declare cache: the queue is declared at most
        # ONCE per (channel, queue_name) pair. AMQP queue_declare is
        # an idempotent no-op when args match, but it still costs a
        # synchronous round-trip to the broker — and when args don't
        # match it costs round-trip + channel-close + reopen + passive
        # re-declare PER PUBLISH. Caching declared queues on the
        # channel object cuts ~5 ms / publish under the TTL-drift
        # case we observed against a 24h-TTL queue declared without
        # args (every publish was burning ~9-10 ms vs. ~3-4 ms after
        # this fix). Cache lives on the pika channel; if the channel
        # is closed/recycled, the cache dies with it (correct
        # invalidation for free).
        declared = getattr(self.channel, "_cheapa_declared_queues", None)
        if declared is None:
            declared = set()
            self.channel._cheapa_declared_queues = declared

        if queue_name not in declared:
            try:
                # PASSIVE-DECLARE FIRST. A passive declare only asserts the
                # queue exists — it never compares arguments — so it
                # succeeds against ANY pre-existing queue regardless of how
                # it was originally declared. This is the key to not
                # emitting ``PRECONDITION_FAILED - inequivalent arg
                # 'x-dead-letter-exchange'`` on every first publish to a
                # queue created by an older schema (the canonical priority
                # queues in this deployment predate the DLX arg set). The
                # previous active-declare-with-args path hit a 406, killed
                # the channel, logged a WARNING, then fell back to passive
                # anyway — so the publish still worked but spammed one
                # warning per queue. Doing the passive declare up front gets
                # the same result silently. This also mirrors the worker's
                # _process_single_queue declare path (passive first, create
                # on miss), so producer and consumer agree on topology.
                self.channel.queue_declare(queue=queue_name, durable=True, passive=True)
                declared.add(queue_name)
            except pika.exceptions.ChannelClosedByBroker as exc:
                if getattr(exc, "reply_code", None) != 404:
                    raise
                # 404 NOT_FOUND — the queue genuinely doesn't exist yet.
                # The passive declare closed the channel, so reopen and
                # actively create it with the canonical DLX argument set so
                # failed jobs dead-letter correctly.
                try:
                    self.channel = self.connection.channel()
                    self.channel.confirm_delivery()
                except (OSError, ConnectionError, RuntimeError):
                    self._connect(opts)
                self.channel.queue_declare(
                    queue=queue_name,
                    durable=True,
                    arguments=queue_args,
                )
                # New channel -> new cache; mark this queue as
                # already-confirmed so subsequent publishes through
                # the same channel don't re-declare it.
                new_cache = {queue_name}
                self.channel._cheapa_declared_queues = new_cache

        # Serialize payload
        serializer = opts.get("serializer", "pickle")
        if serializer == "json":
            # For JSON: convert job instance to string class path
            if hasattr(job, "__class__"):
                payload_copy = payload.copy()
                payload_copy["obj"] = (
                    f"{job.__class__.__module__}.{job.__class__.__name__}"
                )
                body = json.dumps(payload_copy, default=str).encode("utf-8")
            else:
                body = json.dumps(payload, default=str).encode("utf-8")
        else:
            body = pickle.dumps(payload)

        # Publish with persistence + mandatory routing.
        #
        # ``mandatory=True`` makes the broker raise ``UnroutableError``
        # (in concert with the publisher confirms enabled at channel
        # setup) when no queue is bound for our routing key. Without
        # this flag, a typo in the routing key, a wrong exchange, or
        # a deleted queue silently swallowed the dispatch — the
        # caller's job vanished with no signal anywhere. Now the
        # caller gets a hard error and the orchestrator can retry or
        # alert.
        #
        # Note: ``confirm_delivery()`` was previously called *after*
        # every publish. That's a no-op on an already-confirms-mode
        # channel — confirms are enabled once at channel construction
        # (line 636 below). The actual broker ACK is awaited
        # synchronously inside ``basic_publish`` because we use
        # ``BlockingChannel`` with confirms on. Removing the redundant
        # post-call.
        # Per-publish message headers. ``message_headers`` is the
        # dedicated key (used by ``schedule()`` for the delayed-plugin
        # ``x-delay`` header). ``connection_options`` remains as a
        # legacy fallback so any out-of-tree caller that still uses
        # the old shape keeps working — but new code MUST use
        # ``message_headers`` so the connection-pool key (built from
        # ``connection_options``) stays stable across publishes.
        publish_headers = opts.get("message_headers") or opts.get("connection_options")
        try:
            self.channel.basic_publish(
                exchange=opts.get("exchange", ""),
                routing_key=queue_name,
                body=body,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Make message persistent
                    headers=publish_headers,
                ),
                mandatory=True,
            )
        except pika.exceptions.UnroutableError as exc:
            Log.error("AMQPDriver: message unroutable to queue='%s' exchange='%s': %s", queue_name, opts.get('exchange', ''), exc, category='cara.queue.amqp')
            raise

        # No close — caller (``_connect_and_publish``) returns the
        # connection to the pool for reuse.

    # ── Pool helpers ───────────────────────────────────────────────
    def _open_new_connection(self, opts: dict[str, Any]) -> tuple:
        """Open a brand-new connection + channel pair."""
        try:
            import pika
        except ImportError:
            raise QueueDriverLibraryNotFoundException(
                "pika is required for AMQPDriver. Install with: pip install pika"
            )

        connection_url = self._build_url(opts)
        connection = pika.BlockingConnection(pika.URLParameters(connection_url))
        channel = connection.channel()
        channel.confirm_delivery()
        return connection, channel

    def _acquire_thread_connection(self, url: str, opts: dict[str, Any]) -> None:
        """Bind a connection + channel to this thread for the publish.

        Reuse priority: existing thread-local → pool → open fresh.
        """
        if self.connection is not None and self.channel is not None:
            # Already bound on this thread (typical case for hot
            # publishers reusing the same pika channel).
            try:
                if self.connection.is_open and self.channel.is_open:
                    return
            except (OSError, ConnectionError, RuntimeError, AttributeError):
                pass
            # Stale handle — drop it and fall through.
            self._discard_thread_connection()

        # Try to grab a connection from the pool.
        with self._pool_lock:
            pool = self._pool.get(url, [])
            while pool:
                conn, chan = pool.pop()
                try:
                    if conn.is_open and chan.is_open:
                        self.connection = conn
                        self.channel = chan
                        return
                except (OSError, ConnectionError, RuntimeError, AttributeError):
                    pass
                # Stale entry — close and try the next.
                try:
                    conn.close()
                except (OSError, ConnectionError, RuntimeError, AttributeError):
                    pass

        # Pool empty / nothing healthy — open a fresh connection.
        self.connection, self.channel = self._open_new_connection(opts)

    def _return_thread_connection(self, url: str) -> None:
        """Return the thread's connection to the pool, capped at
        ``_max_pool_per_url``. Excess connections are closed."""
        conn, chan = self.connection, self.channel
        # Always clear the thread-local first so a subsequent error
        # path can't accidentally reuse a returned connection.
        self.connection = None
        self.channel = None
        if conn is None or chan is None:
            return
        try:
            if not (conn.is_open and chan.is_open):
                conn.close()
                return
        except (
            OSError,
            ConnectionError,
            RuntimeError,
            AttributeError,
            pika.exceptions.AMQPError,
        ):
            # A dead/closed connection that we're dropping must not turn its
            # own ``close()`` (pika ``*WrongStateError`` on an already-closed
            # handle) into a publish-time failure.
            return

        with self._pool_lock:
            pool = self._pool.setdefault(url, [])
            if len(pool) >= self._max_pool_per_url:
                try:
                    conn.close()
                except (
                    OSError,
                    ConnectionError,
                    RuntimeError,
                    AttributeError,
                    pika.exceptions.AMQPError,
                ):
                    pass
                return
            pool.append((conn, chan))

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

        Kept for callers that don't go through ``_connect_and_publish``
        (e.g. ``declare_dead_letter_exchange``). New code should prefer
        ``_acquire_thread_connection`` + ``_return_thread_connection``.
        """
        if self.connection is not None and self.channel is not None:
            try:
                if self.connection.is_open and self.channel.is_open:
                    return
            except (OSError, ConnectionError, RuntimeError, AttributeError):
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

        base_url = (
            f"amqp://{encoded_username}:{encoded_password}"
            f"@{connection_params['host']}:{connection_params['port']}/{encoded_vhost}"
        )

        # Append connection options if present
        connection_options = opts.get("connection_options")
        if connection_options:
            from urllib.parse import urlencode

            return f"{base_url}?{urlencode(connection_options)}"

        return base_url
