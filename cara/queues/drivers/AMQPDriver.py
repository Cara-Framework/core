"""
AMQP Queue Driver for the Cara framework.

Modern, clean implementation for RabbitMQ-based job queue management.
"""

import json
import logging
import pickle
import threading
import uuid
from typing import Any, Dict, List, Optional, Union

import pendulum
import pika

from cara.exceptions import DriverLibraryNotFoundException
from cara.facades import Log
from cara.queues.contracts.Queue import Queue
from cara.queues.job_instantiation import instantiate_job
from cara.support.Console import HasColoredOutput


class AMQPDriver(HasColoredOutput, Queue):
    """
    AMQP-based queue driver for RabbitMQ.

    Features:
    - Reliable message delivery with publisher confirms
    - Job tracking with unique IDs
    - Integration with JobTracker for status updates
    - Persistent messages and durable queues
    """

    driver_name = "amqp"

    def __init__(self, application, options: Dict[str, Any]):
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
        self._pool: Dict[str, List[Any]] = {}
        self._pool_lock = threading.Lock()
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

    def push(self, *jobs: Any, options: Dict[str, Any]) -> Union[str, List[str]]:
        """Push jobs to queue and return job ID(s) for tracking."""
        merged_opts = {**self.options, **options}
        job_ids = []

        for job in jobs:
            # Generate unique job ID for tracking
            job_id = str(uuid.uuid4())
            job_ids.append(job_id)

            # Create job record in database via JobTracker
            db_job_id = self._create_job_record(job, job_id, merged_opts)

            # Prepare payload
            payload = {
                "obj": job,
                "args": merged_opts.get("args", ()),
                "callback": merged_opts.get("callback", "handle"),
                "created": pendulum.now(tz=merged_opts.get("tz", "UTC")),
                "job_id": job_id,
                "db_job_id": db_job_id,
            }

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

    def batch(self, *jobs: Any, options: Dict[str, Any]) -> None:
        """Batch push: push all jobs at once."""
        self.push(*jobs, options=options)

    def chain(self, jobs: list, options: Dict[str, Any]) -> None:
        """Chain jobs: push each job in sequence."""
        if not jobs:
            return

        for job in jobs:
            self.push(job, options=options)

    def schedule(self, job: Any, when: Any, options: Dict[str, Any]) -> None:
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

        # Add delay header for RabbitMQ delayed plugin
        headers = {"x-delay": delay_ms}
        merged_opts["connection_options"] = {
            **merged_opts.get("connection_options", {}),
            **headers,
        }

        self.push(job, options=merged_opts)

    def later(self, delay: Union[int, "pendulum.Duration"], job: Any, options: Dict[str, Any] = None) -> Union[str, List[str]]:
        """
        Schedule a job to be executed after a delay.

        Uses AMQP message TTL and delayed plugin for scheduling.
        Implements exponential backoff for retries.

        Args:
            delay: Delay in seconds or pendulum Duration
            job: Job instance to schedule
            options: Queue options

        Returns:
            Job ID(s)
        """
        import pendulum as pendulum_module

        if options is None:
            options = {}

        # Handle delay as Duration or int
        if isinstance(delay, int):
            delay_seconds = delay
        else:
            # Assume it's a Duration-like object
            delay_seconds = int(delay.total_seconds()) if hasattr(delay, "total_seconds") else delay

        # Merge options
        merged_opts = {**self.options, **options}

        # Calculate when job should run
        when = pendulum_module.now(tz=merged_opts.get("tz", "UTC")).add(seconds=delay_seconds)

        # Use schedule method which handles AMQP delayed plugin
        self.schedule(job, when, merged_opts)

        # Generate and return job ID
        job_id = str(uuid.uuid4())
        return job_id

    def retry(
        self,
        job: Any,
        options: Dict[str, Any] = None,
        attempts: int = 3,
        backoff: str = "exponential"
    ) -> Optional[Union[str, List[str]]]:
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
            Log.error(
                f"Job {job.__class__.__name__} exceeded max retry attempts ({attempts}). "
                f"Sending to dead letter queue."
            )
            self._send_to_dead_letter(job, options, attempts_made)
            return None

        # Calculate delay based on backoff strategy
        if isinstance(backoff, str) and backoff == "exponential":
            # Exponential backoff: 1s, 2s, 4s, 8s, etc.
            delay_seconds = 2 ** attempts_made
        elif isinstance(backoff, str) and backoff == "linear":
            # Linear backoff: 1s, 2s, 3s, 4s, etc.
            delay_seconds = (attempts_made + 1) * 60  # 1 minute per attempt
        else:
            # Fixed delay (assume it's an int)
            delay_seconds = int(backoff) if backoff else 300  # Default 5 minutes

        attempts_made += 1
        Log.info(
            f"Retrying job {job.__class__.__name__} (attempt {attempts_made}/{attempts}) "
            f"with {delay_seconds}s delay"
        )

        # Update options with retry info
        retry_opts = {
            **options,
            "attempts": attempts_made,
            "retry_backoff": backoff,
            "max_attempts": attempts,
        }

        # Schedule retry with delay
        return self.later(delay_seconds, job, retry_opts)

    def _send_to_dead_letter(
        self, job: Any, options: Dict[str, Any], attempts: int
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
                self._connect({})

                # Prepare dead letter message
                payload = {
                    "obj": job,
                    "args": options.get("args", ()),
                    "callback": options.get("callback", "handle"),
                    "failed_at": pendulum.now(tz=options.get("tz", "UTC")).to_datetime_string(),
                    "attempts": attempts,
                    "error": options.get("error"),
                }

                # Serialize
                serializer = options.get("serializer", "pickle")
                if serializer == "json":
                    body = json.dumps(payload, default=str).encode("utf-8")
                else:
                    body = pickle.dumps(payload)

                # Publish to dead letter exchange
                dlx_name = options.get("exchange", "default") + ".dlx"
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
                Log.error(
                    f"Job dead-lettered after {attempts} attempts: "
                    f"{job.__class__.__name__} → {dlq_routing_key}",
                    extra={
                        "job_class": job.__class__.__name__,
                        "attempts": attempts,
                        "dlq_routing_key": dlq_routing_key,
                    },
                )

                # Best-effort metric increment so dashboards / Prometheus
                # alerts can fire when DLQ rate spikes.
                try:
                    from app.support.Metrics import Metrics  # type: ignore[attr-defined]
                    Metrics.queue_jobs_dead_lettered_total.labels(
                        job=job.__class__.__name__,
                    ).inc()
                except Exception:
                    pass

                # Close connection
                try:
                    self.channel.close()
                    self.connection.close()
                except Exception:
                    pass

            except Exception:
                Log.error(
                    "Failed to send job to dead letter queue",
                    exc_info=True,
                )

    def consume(self, options: Dict[str, Any]) -> None:
        """Consume jobs from RabbitMQ.

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
        merged_opts = {**self.options, **options}
        queue_name = merged_opts.get("queue", "default")

        try:
            import pika
        except ImportError:
            raise DriverLibraryNotFoundException(
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
        message_ttl = merged_opts.get("message_ttl", 86400000)
        queue_args = {
            "x-dead-letter-exchange": (
                f"{exchange_name}.dlx" if exchange_name else "dead.letter.dlx"
            ),
            "x-dead-letter-routing-key": f"dead.{queue_name}",
            "x-message-ttl": message_ttl,
        }
        try:
            channel.queue_declare(
                queue=queue_name, durable=True, arguments=queue_args
            )
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
                payload = pickle.loads(body)
                raw = payload.get("obj")
                callback_name = payload.get("callback", "handle")
                args = payload.get("args", ())
                instance = instantiate_job(self.application, raw, args)

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

                if hasattr(self.application, "call"):
                    self.application.call(method_to_call, *args)
                else:
                    method_to_call(*args) if args else method_to_call()

                ch.basic_ack(delivery_tag=method.delivery_tag)
                self._dispatch_batch_completion(instance, None)
                self.info(
                    f"AMQPDriver: job processed successfully, queue={queue_name}"
                )
            except Exception as exc:
                self.danger(f"AMQPDriver: job processing failed: {exc}")
                # nack with requeue=False so DLX gets the message.
                try:
                    ch.basic_nack(
                        delivery_tag=method.delivery_tag, requeue=False
                    )
                except Exception:
                    pass
                # Job-side ``failed`` hook for app-level cleanup.
                if instance is not None and hasattr(instance, "failed"):
                    try:
                        instance.failed(payload, str(exc))
                    except Exception as inner:
                        self.danger(f"AMQPDriver: failed() raised: {inner}")
                self._dispatch_batch_completion(instance, exc)
            finally:
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
            sig_name = signal.Signals(signum).name if hasattr(signal, "Signals") else str(signum)
            self.info(f"AMQPDriver: received {sig_name}, stopping consumer gracefully…")
            try:
                channel.stop_consuming()
            except Exception:
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
            except Exception:
                pass

    @staticmethod
    def _release_unique_lock_if_any(instance) -> None:
        if instance is None:
            return
        try:
            from cara.queues.contracts import UniqueJob

            if isinstance(instance, UniqueJob):
                UniqueJob.release_unique_lock(instance.unique_id())
        except Exception:
            pass

    @staticmethod
    def _dispatch_batch_completion(instance, exception=None) -> None:
        if instance is None:
            return
        try:
            from cara.queues.Batch import auto_dispatch_batch_completion

            auto_dispatch_batch_completion(instance, exception)
        except Exception:
            pass

    def _create_job_record(self, job, job_id: str, opts: Dict[str, Any]) -> Optional[int]:
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
            Log.warning(f"Failed to create job record: {e}")
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
            self._connect({})

            # Declare dead letter exchange
            dlx_name = f"{exchange_name}.dlx"
            self.channel.exchange_declare(
                exchange=dlx_name,
                exchange_type="topic",
                durable=True
            )

            # Declare dead letter queue
            dlq_name = f"{exchange_name}.queue"
            self.channel.queue_declare(
                queue=dlq_name,
                durable=True,
                arguments={
                    "x-message-ttl": 86400000,  # 24 hours
                }
            )

            # Bind queue to DLX
            self.channel.queue_bind(
                exchange=dlx_name,
                queue=dlq_name,
                routing_key="dead.*"
            )

            Log.info(f"Dead letter exchange configured: {dlx_name}")

            # Close connection
            try:
                self.channel.close()
                self.connection.close()
            except Exception:
                pass

        except Exception as e:
            Log.error(f"Failed to declare dead letter exchange: {e}")

    def get_dead_letter_messages(
        self, queue_name: str = "dead.letter.queue", limit: int = 100
    ) -> List[Dict[str, Any]]:
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
            self._connect({})

            # Use basic_get to peek at messages without consuming
            for _ in range(limit):
                method, properties, body = self.channel.basic_get(queue_name, auto_ack=False)

                if method is None:
                    break

                # Decode payload
                try:
                    payload = pickle.loads(body)
                except Exception:
                    try:
                        payload = json.loads(body.decode("utf-8"))
                    except Exception:
                        payload = {"raw": body.decode("utf-8", errors="ignore")}

                messages.append({
                    "delivery_tag": method.delivery_tag,
                    "routing_key": method.routing_key,
                    "redelivered": method.redelivered,
                    "exchange": method.exchange,
                    "headers": dict(properties.headers or {}),
                    "timestamp": properties.timestamp,
                    "payload": payload,
                })

                # Don't consume - requeue the message
                self.channel.basic_nack(method.delivery_tag, requeue=True)

            # Close connection
            try:
                self.channel.close()
                self.connection.close()
            except Exception:
                pass

        except Exception as e:
            Log.error(f"Failed to get dead letter messages: {e}")

        return messages

    def replay_dead_letter(
        self, queue_name: str, message_id: Optional[str] = None
    ) -> int:
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
            self._connect({})

            while True:
                method, properties, body = self.channel.basic_get(dlq_name, auto_ack=False)

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
                    Log.info(f"Replayed message {msg_id} to {queue_name}")

                    if message_id is not None:
                        break

                # Remove from dead letter queue
                self.channel.basic_ack(method.delivery_tag)

            # Close connection
            try:
                self.channel.close()
                self.connection.close()
            except Exception:
                pass

        except Exception as e:
            Log.error(f"Failed to replay dead letter messages: {e}")

        return replayed

    def _connect_and_publish(self, payload: Any, opts: Dict[str, Any]) -> None:
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

    def _connect_and_publish_locked(self, payload: Any, opts: Dict[str, Any]) -> None:
        """Inner publish path — assumes ``self.channel`` / ``self.connection``
        are bound to this thread by the caller."""

        # Get queue name from job or options
        job = payload.get("obj")
        queue_name = (
            job.queue
            if hasattr(job, "queue") and job.queue
            else opts.get("queue", "default")
        )

        # Declare queue with dead letter exchange support
        exchange_name = opts.get("exchange", "")
        message_ttl = opts.get("message_ttl", 86400000)  # 24h default

        queue_args = {
            "x-dead-letter-exchange": f"{exchange_name}.dlx" if exchange_name else "dead.letter.dlx",
            "x-dead-letter-routing-key": f"dead.{queue_name}",
            "x-message-ttl": message_ttl,
        }

        # Idempotent declare: if the queue was originally created with
        # different args (e.g. older codebase or manual setup left it
        # without x-message-ttl), an active declare raises
        # PRECONDITION_FAILED (406) and kills the channel. We swallow
        # that once, reopen the channel, and fall back to passive
        # declare — the queue already exists so we can publish anyway.
        # This prevents TTL-drift from wedging every dispatcher forever.
        try:
            self.channel.queue_declare(
                queue=queue_name,
                durable=True,
                arguments=queue_args,
            )
        except pika.exceptions.ChannelClosedByBroker as exc:
            if getattr(exc, "reply_code", None) != 406:
                raise
            Log.warning(
                f"⚠️  Queue '{queue_name}' exists with different args "
                f"(reply={exc.reply_text}); falling back to passive declare"
            )
            # Channel is dead after 406 — reopen on the same connection.
            try:
                self.channel = self.connection.channel()
                self.channel.confirm_delivery()
            except Exception:
                self._connect(opts)
            self.channel.queue_declare(
                queue=queue_name, durable=True, passive=True
            )

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
        try:
            self.channel.basic_publish(
                exchange=opts.get("exchange", ""),
                routing_key=queue_name,
                body=body,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Make message persistent
                    headers=opts.get("connection_options"),
                ),
                mandatory=True,
            )
        except pika.exceptions.UnroutableError as exc:
            Log.error(
                f"AMQPDriver: message unroutable to queue='{queue_name}' "
                f"exchange='{opts.get('exchange', '')}': {exc}",
                category="cara.queue.amqp",
            )
            raise

        # No close — caller (``_connect_and_publish``) returns the
        # connection to the pool for reuse.

    # ── Pool helpers ───────────────────────────────────────────────
    def _open_new_connection(self, opts: Dict[str, Any]) -> tuple:
        """Open a brand-new connection + channel pair."""
        try:
            import pika
        except ImportError:
            raise DriverLibraryNotFoundException(
                "pika is required for AMQPDriver. Install with: pip install pika"
            )

        connection_url = self._build_url(opts)
        connection = pika.BlockingConnection(pika.URLParameters(connection_url))
        channel = connection.channel()
        channel.confirm_delivery()
        return connection, channel

    def _acquire_thread_connection(self, url: str, opts: Dict[str, Any]) -> None:
        """Bind a connection + channel to this thread for the publish.

        Reuse priority: existing thread-local → pool → open fresh.
        """
        if self.connection is not None and self.channel is not None:
            # Already bound on this thread (typical case for hot
            # publishers reusing the same pika channel).
            try:
                if self.connection.is_open and self.channel.is_open:
                    return
            except Exception:
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
                except Exception:
                    pass
                # Stale entry — close and try the next.
                try:
                    conn.close()
                except Exception:
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
        except Exception:
            return

        with self._pool_lock:
            pool = self._pool.setdefault(url, [])
            if len(pool) >= self._max_pool_per_url:
                try:
                    conn.close()
                except Exception:
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
            except Exception:
                pass

    def _connect(self, opts: Dict[str, Any]) -> None:
        """Bind a connection + channel to this thread.

        Kept for callers that don't go through ``_connect_and_publish``
        (e.g. ``declare_dead_letter_exchange``). New code should prefer
        ``_acquire_thread_connection`` + ``_return_thread_connection``.
        """
        if self.connection is not None and self.channel is not None:
            try:
                if self.connection.is_open and self.channel.is_open:
                    return
            except Exception:
                pass
        self.connection, self.channel = self._open_new_connection(opts)

        # NOTE: Queue declaration is intentionally NOT done here.
        # Each caller (_connect_and_publish, setup_dead_letter_exchange, etc.)
        # declares its target queue with the correct arguments (x-message-ttl,
        # x-dead-letter-exchange, ...). Declaring here without arguments
        # conflicted with existing queues and caused PRECONDITION_FAILED
        # (inequivalent arg 'x-message-ttl') on reconnects.

    def _build_url(self, opts: Dict[str, Any]) -> str:
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
