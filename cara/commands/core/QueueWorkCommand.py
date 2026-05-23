"""
Queue Worker Command for the Cara framework.

This module provides a CLI command to process jobs from the queue with enhanced UX.
"""

import asyncio
import builtins
import concurrent.futures
import inspect
import json
import logging
import os
import pickle
import threading
import time
from typing import Any

from cara.commands import CommandBase
from cara.commands.AutoReloadMixin import AutoReloadMixin
from cara.configuration import config
from cara.decorators import command
from cara.facades import Log
from cara.queues.contracts import UniqueJob

# Prometheus metrics — optional import so a bare ``cara`` package import
# (e.g. tests) doesn't require the services-tree ``app.support.Metrics``.
try:
    from app.support.Metrics import Metrics as _M
except Exception:  # pragma: no cover
    _M = None  # type: ignore[assignment]


def _queue_label(
    msg: dict | None, instance: Any = None, queue_name: str | None = None
) -> str:
    """Best-effort queue label for the current message (bounded cardinality).

    Priority order:
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

    def __init__(self, config_func):
        self.config = config_func
        self.connection = None

    def ensure_connection(self) -> bool:
        """Ensure AMQP connection is alive.

        Treats any prior operational failure (``StreamLostError``,
        ``ConnectionClosedByBroker``, TCP RST during a long scrape)
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
                    try:
                        self.connection.close()
                    except Exception:
                        pass
                    self.connection = None

            if self.connection is None or self.connection.is_closed:
                self.connection = self._create_connection()
            return True
        except Exception as e:
            try:
                from cara.facades import Log

                Log.error(f"Failed to connect to RabbitMQ: {e}")
            except ImportError:
                pass
            self.connection = None
            return False

    def _create_connection(self):
        """Create new AMQP connection."""
        import pika

        credentials = pika.PlainCredentials(
            self.config("queue.drivers.amqp.username"),
            self.config("queue.drivers.amqp.password"),
        )
        parameters = pika.ConnectionParameters(
            host=self.config("queue.drivers.amqp.host"),
            port=self.config("queue.drivers.amqp.port", 5672),
            virtual_host=self.config("queue.drivers.amqp.vhost", "/"),
            credentials=credentials,
            # Long-running jobs (Amazon browser scrape + proxy rotation ~30-120s)
            # blocked pika's I/O thread past the default 60s heartbeat → broker
            # closed the channel → basic_ack fails → RabbitMQ re-delivers →
            # second worker picks up same job → idempotency lock collision.
            # 600s gives plenty of headroom; prefetch=1 keeps fairness.
            heartbeat=600,
            blocked_connection_timeout=300,
            socket_timeout=30,
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
            try:
                self.connection.close()
            except Exception:
                pass


class JobProcessor:
    """Processes individual jobs from queue messages (Single Responsibility)."""

    # Class-level constants for job execution
    DEFAULT_JOB_TIMEOUT = 3600  # 1 hour in seconds
    MAX_PAYLOAD_SIZE = 50 * 1024 * 1024  # 50 MB

    @staticmethod
    def _execute_job_with_timeout(method_to_call, init_args, timeout_seconds):
        """Execute job with timeout enforcement using ThreadPoolExecutor.

        On timeout the worker thread may still be holding DB connections
        or UniqueJob locks. We give it a brief grace period (5s) to
        finish cleanup before abandoning it. ``shutdown(wait=True)``
        with a bounded join prevents the leak that ``wait=False``
        caused — orphaned threads kept connections checked-out from
        the pool until process restart.
        """
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        future = executor.submit(method_to_call, *init_args)
        try:
            future.result(timeout=timeout_seconds)
        except concurrent.futures.TimeoutError:
            # Cancel the future (best-effort) and allow a short grace
            # period for thread cleanup (DB rollback, lock release).
            future.cancel()
            raise TimeoutError(f"Job exceeded timeout of {timeout_seconds}s")
        finally:
            # Give worker thread up to 5s to release resources, then
            # abandon. True wait prevents the pool-leak that wait=False
            # caused, while the bounded timeout prevents infinite hangs.
            executor.shutdown(wait=True, cancel_futures=True)

    @staticmethod
    def _execute_async_job_with_timeout(method_to_call, init_args, timeout_seconds):
        """Execute async job with timeout enforcement."""
        try:
            asyncio.run(method_to_call(*init_args))
        except TimeoutError as e:
            raise TimeoutError(f"Async job exceeded timeout of {timeout_seconds}s") from e

    # Framework-default retry policy used when the failing job does
    # not declare its own ``max_attempts`` / ``retry_backoff``. Kept
    # in lockstep with ``AMQPDriver`` so the production worker path
    # (this command) and the legacy ``AMQPDriver.consume`` path agree
    # on the budget. Pre-fix this command had its own broken policy
    # that effectively gave every job zero retries.
    DEFAULT_MAX_ATTEMPTS = 3
    DEFAULT_RETRY_BACKOFF_SECONDS = (1, 5, 30)

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
        and every first failure was ACKed straight to the DLQ. The
        whole ``AMQPDriver._handle_failed_message`` retry schedule was
        dead code.
        """
        if not msg:
            return False
        try:
            attempts_done = int(msg.get("attempts", 0) or 0)
        except (TypeError, ValueError):
            attempts_done = 0
        max_attempts = int(
            getattr(instance, "max_attempts", None)
            or JobProcessor.DEFAULT_MAX_ATTEMPTS
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
    ) -> None:
        """ACK the current delivery and republish with backoff.

        ``basic_nack(requeue=True)`` puts the message back on the queue
        head immediately. With ``prefetch=1`` the same worker thread
        re-claims it on the very next iteration — a poison message
        loops at 100% CPU. The Cara contract is ``republish-with-
        backoff`` (1s / 5s / 30s by default, jittered), which only
        works when we re-publish through the AMQP delayed-message
        path. We ACK the original delivery and stamp the new message
        with ``attempts = attempts_done + 1`` so the next failure can
        decide budget correctly.
        """
        try:
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        except Exception as ack_err:
            Log.error(f"Failed to ACK before retry republish: {ack_err}")
            # Fall through — pika may have already auto-rejected.

        attempts_done = int(msg.get("attempts", 0) or 0)
        next_attempt = attempts_done + 1

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
            }
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
            # ``later`` is the Laravel-compatible delay entry point.
            # Falls back to ``schedule`` for drivers that don't expose
            # one; ``Queue.later`` already handles that delegation.
            _Queue.later(delay_seconds, instance, **retry_options)
            Log.info(
                f"↻ Retry queued for {instance.__class__.__name__} "
                f"(attempt {next_attempt}, +{delay_seconds}s, "
                f"reason={type(exc).__name__})"
            )
        except Exception as republish_err:
            Log.error(
                f"Retry republish failed for {instance.__class__.__name__}: "
                f"{republish_err}. Falling back to DLQ to avoid losing the payload."
            )
            JobProcessor._ack_to_dlq(channel, method_frame, msg, str(exc))

    @staticmethod
    def _route_failed_message(
        *,
        channel,
        method_frame,
        msg,
        instance,
        exc: Exception,
        queue_name: str | None,
    ) -> None:
        """Single failure router: retry-with-delay OR dead-letter.

        Centralises three rules that the two ``except`` branches
        previously duplicated and routinely diverged on:

        * ``do_not_retry`` exceptions skip straight to DLQ — no point
          burning the backoff budget on a 404 that won't come back.
        * Retries leave the ``UniqueJob`` lock in place so a concurrent
          dispatch with the same ``unique_id`` doesn't slip in during
          the backoff window. The lock TTL (``unique_for``, default 1h)
          remains the safety cap.
        * Terminal failure (budget exhausted, unpickleable instance,
          ``do_not_retry``) releases the lock so the next legitimate
          dispatch can proceed.
        """
        do_not_retry = bool(getattr(exc, "do_not_retry", False))
        can_retry = (
            msg
            and instance is not None
            and not do_not_retry
            and JobProcessor._should_retry_job(msg, instance)
        )

        if can_retry:
            JobProcessor._requeue_with_delay(
                channel=channel,
                method_frame=method_frame,
                msg=msg,
                instance=instance,
                exc=exc,
                queue_name=queue_name,
            )
            # Intentional: DO NOT release the UniqueJob lock. The
            # retry is in flight (delayed publish); releasing now
            # would allow a duplicate dispatch to win the lock and
            # both copies would run when the delay expires.
            return

        # Terminal — give up the slot.
        JobProcessor._ack_to_dlq(channel, method_frame, msg, str(exc))
        if instance is not None and isinstance(instance, UniqueJob):
            try:
                UniqueJob.release_unique_lock(instance.unique_id())
            except Exception:
                pass

    @staticmethod
    def _ack_to_dlq(channel, method_frame, msg, error_msg):
        """ACK message and log to dead letter pattern for failed jobs."""
        try:
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            # Log to DLQ-style pattern for monitoring
            dlq_queue = f"{msg.get('queue', 'unknown')}.dlq" if msg else "unknown.dlq"
            job_id = msg.get("job_id", "unknown") if msg else "unknown"
            Log.error(
                f"💀 Job moved to DLQ: {job_id} | Queue: {dlq_queue} | Error: {error_msg}"
            )
        except Exception as e:
            Log.error(f"Failed to ACK message: {e}")

    @staticmethod
    def process_message(
        channel, method_frame, body, queue_name: str | None = None
    ) -> bool:
        """Process a single queue message and return success status.

        ``queue_name`` is the queue the worker dequeued from. Used as
        the highest-fidelity label for Prometheus metrics — otherwise
        we'd have to infer the queue from the pickled message
        payload, which is lossy.
        """
        # Start of job window — used across all exit paths below.
        _mx_start = time.time()
        _mx_queue = str(queue_name) if queue_name else "unknown"
        _mx_job = "unknown"
        _mx_inflight_entered = False

        def _mx_record(outcome: str) -> None:
            """Emit metrics for this job exit. Safe to call multiple times
            (we only set ``_mx_recorded`` once inside the closure)."""
            if _M is None:
                return
            nonlocal _mx_recorded
            if _mx_recorded:
                return
            _mx_recorded = True
            try:
                _M.queue_jobs_consumed_total.labels(
                    queue=_mx_queue,
                    job=_mx_job,
                    outcome=outcome,
                ).inc()
                _M.queue_job_duration_seconds.labels(
                    queue=_mx_queue,
                    job=_mx_job,
                ).observe(time.time() - _mx_start)
                if _mx_inflight_entered:
                    _M.queue_jobs_in_flight.labels(
                        queue=_mx_queue,
                        job=_mx_job,
                    ).dec()
            except Exception:
                pass

        _mx_recorded = False

        # CRITICAL FIX #4: Validate payload size before unpickling
        if len(body) > JobProcessor.MAX_PAYLOAD_SIZE:
            Log.error(
                f"❌ Payload exceeds max size ({len(body)} > {JobProcessor.MAX_PAYLOAD_SIZE})"
            )
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)
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

        try:
            # Unpickle message. When a cross-service queue shares a
            # RabbitMQ instance (e.g. the API publishes ``ai.*`` jobs
            # whose classes only live in the api/ tree), pickle raises
            # ``ModuleNotFoundError`` here because services/ doesn't
            # have the module. We catch those specifically and ACK
            # without a full traceback — the message isn't for us, and
            # swamping logs with a stack trace per orphan message hides
            # the real errors underneath.
            try:
                # Try JSON first (messages published with serializer="json"),
                # then fall back to pickle (the default serializer).
                if body and body[0:1] == b"{":
                    import importlib

                    msg = json.loads(body)
                    # JSON-serialized jobs store the class as a dotted path
                    # string instead of a live object. Reconstruct it.
                    obj_ref = msg.get("obj")
                    if isinstance(obj_ref, str) and "." in obj_ref:
                        module_path, class_name = obj_ref.rsplit(".", 1)
                        mod = importlib.import_module(module_path)
                        cls = getattr(mod, class_name)
                        # Reconstruct with init_kwargs if available, or
                        # with individual known keys from the payload.
                        init_kwargs = msg.get("init_kwargs", {})
                        if not init_kwargs:
                            # Try to extract constructor args from the
                            # payload itself (e.g. product_id, asin).
                            for key in (
                                "product_id",
                                "asin",
                                "container_id",
                                "url",
                                "keyword",
                                "category_id",
                            ):
                                if key in msg and key != "obj":
                                    init_kwargs[key] = msg[key]
                        try:
                            msg["obj"] = cls(**init_kwargs)
                        except TypeError:
                            msg["obj"] = cls()
                else:
                    msg = pickle.loads(body)
            except (ModuleNotFoundError, AttributeError, ImportError) as e:
                Log.warning(
                    f"Dropping orphan message (class not importable in this service): "
                    f"{e.__class__.__name__}: {e}"
                )
                channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                _mx_queue = _queue_label(msg, queue_name=queue_name)
                _mx_job = _job_label(None, msg)
                _mx_record("orphan")
                return True
            instance = msg.get("obj")
            callback = msg.get("callback", "handle")
            init_args = msg.get("args", ())
            db_job_id = msg.get("db_job_id")
            job_timeout = msg.get("timeout", JobProcessor.DEFAULT_JOB_TIMEOUT)

            # A payload with no ``obj`` (or ``obj=None``) is malformed —
            # the worker has no class to call, no UniqueJob lock to
            # release, no failed() hook to invoke. Pre-fix the
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
                    f"❌ Malformed payload (missing 'obj'): job_id={msg.get('job_id')} "
                    f"keys={sorted(msg.keys())} — routing to DLQ"
                )
                JobProcessor._ack_to_dlq(
                    channel, method_frame, msg, "payload missing 'obj'"
                )
                _mx_queue = _queue_label(msg, queue_name=queue_name)
                _mx_job = _job_label(None, msg)
                _mx_record("malformed")
                return "failure"

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
            _mx_queue = _queue_label(msg)
            _mx_job = _job_label(instance, msg)
            if _M is not None:
                try:
                    _M.queue_jobs_in_flight.labels(
                        queue=_mx_queue,
                        job=_mx_job,
                    ).inc()
                    _mx_inflight_entered = True
                except Exception:
                    pass
                if wait_secs is not None:
                    try:
                        _M.queue_wait_seconds.labels(
                            queue=_mx_queue,
                            job=_mx_job,
                        ).observe(wait_secs)
                    except Exception:
                        pass

            # Set up job tracking
            job_id = msg.get("job_id")
            if hasattr(instance, "set_tracking_id") and job_id:
                instance.set_tracking_id(job_id)

            if db_job_id and hasattr(instance, "__dict__"):
                instance._db_job_id = db_job_id

            # Start tracking (Trackable trait tracks entity_id)
            if hasattr(instance, "_start_tracking"):
                instance._start_tracking()

            # Update job table status to processing
            if tracker and db_job_id:
                tracker.update_job_status(db_job_id, "processing")

            # Mark as processing in unified job table
            if hasattr(instance, "_mark_processing"):
                instance._mark_processing()

            # Stamp container on job so BaseJob and method-level DI can use it.
            if app_instance is not None and hasattr(instance, "__dict__"):
                instance._app = app_instance
                if hasattr(type(instance), "_app"):
                    type(instance)._app = app_instance

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
            if callable(method_to_call):
                from cara.queues.middleware import run_through_middleware_async

                if inspect.iscoroutinefunction(method_to_call):

                    async def _async_handler(_job, _m=method_to_call, _args=init_args):
                        if app_instance is not None:
                            return await app_instance.call(_m, *_args)
                        return await _m(*_args)

                    try:
                        coro = run_through_middleware_async(instance, _async_handler)
                        asyncio.run(asyncio.wait_for(coro, timeout=job_timeout))
                    except TimeoutError as e:
                        raise TimeoutError(
                            f"Job exceeded timeout of {job_timeout}s"
                        ) from e
                else:

                    async def _sync_handler(_job, _m=method_to_call, _args=init_args):
                        if app_instance is not None:
                            return app_instance.call(_m, *_args)
                        return _m(*_args)

                    def _call_with_middleware():
                        coro = run_through_middleware_async(instance, _sync_handler)
                        asyncio.run(coro)

                    JobProcessor._execute_job_with_timeout(
                        _call_with_middleware, (), job_timeout
                    )

            # Mark success in unified job table
            if hasattr(instance, "_mark_success"):
                instance._mark_success()

            # Update job table status to completed
            if tracker and db_job_id:
                tracker.update_job_status(db_job_id, "completed")

            # Release UniqueJob lock if applicable. The release MUST
            # not be allowed to raise through to the outer handler —
            # the work is already done, and skipping basic_ack here
            # causes the broker to redeliver a completed message
            # under prefetch=1, double-executing every side-effecting
            # handler the next time the cache backing the lock blips.
            # The lock TTL (``unique_for``, default 1h) is the safety
            # net for any release we couldn't write — better a delayed
            # re-dispatch than a duplicated side effect.
            if isinstance(instance, UniqueJob):
                try:
                    UniqueJob.release_unique_lock(instance.unique_id())
                except Exception as release_err:
                    Log.warning(
                        f"UniqueJob lock release failed for "
                        f"{instance.__class__.__name__}: {release_err}. "
                        f"Lock will expire on its TTL — proceeding to ACK."
                    )

            # Acknowledge message
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            _mx_record("success")
            return "success"

        except TimeoutError as timeout_error:
            Log.error(f"⏱️ Job timeout: {str(timeout_error)}")

            # Mark as failed in unified job table
            if instance and hasattr(instance, "_mark_failed"):
                instance._mark_failed(str(timeout_error), should_retry=True)

            # Update job table status to failed
            if tracker and db_job_id:
                tracker.update_job_status(db_job_id, "failed")

            JobProcessor._route_failed_message(
                channel=channel,
                method_frame=method_frame,
                msg=msg,
                instance=instance,
                exc=timeout_error,
                queue_name=queue_name,
            )

            # Try to call failed method
            try:
                if instance and hasattr(instance, "failed"):
                    failed_method = instance.failed
                    if inspect.iscoroutinefunction(failed_method):
                        asyncio.run(failed_method(msg, str(timeout_error)))
                    else:
                        failed_method(msg, str(timeout_error))
            except Exception:
                pass

            _mx_record("timeout")
            return "failure"

        except Exception as job_error:
            import traceback

            Log.error(f"❌ Job failed: {str(job_error)}")
            Log.error(f"   Traceback: {traceback.format_exc()}")

            # Mark as failed in unified job table
            if instance and hasattr(instance, "_mark_failed"):
                instance._mark_failed(str(job_error), should_retry=False)

            # Update job table status to failed
            if tracker and db_job_id:
                tracker.update_job_status(db_job_id, "failed")

            JobProcessor._route_failed_message(
                channel=channel,
                method_frame=method_frame,
                msg=msg,
                instance=instance,
                exc=job_error,
                queue_name=queue_name,
            )

            # Try to call failed method
            try:
                if instance and hasattr(instance, "failed"):
                    failed_method = instance.failed
                    if inspect.iscoroutinefunction(failed_method):
                        asyncio.run(failed_method(msg, str(job_error)))
                    else:
                        failed_method(msg, str(job_error))
            except Exception:
                pass

            _mx_record("failed")
            return "failure"  # Still processed (failed gracefully)


@command(
    name="queue:work",
    help="Run the queue worker to consume jobs with enhanced UX.",
    options={
        "--driver=?": "Queue driver to use (overrides default configuration)",
        "--queue=?": "Queue name(s) to process (comma-separated for priority: high,default,low)",
        "--pool=?": "Worker pool name from config/queue.py WORKER_POOLS (e.g. pipeline, enrichment, background, realtime). Overrides --queue and --concurrency with pool config.",
        "--timeout=?": "Poll timeout in seconds (default: 5)",
        "--max-jobs=?": "Maximum number of jobs to process before stopping",
        "--max-time=?": "Maximum runtime in seconds before stopping",
        "--concurrency=?": "Number of parallel consumer threads inside this worker process (default: 1). Each thread keeps its own AMQP connection/channel, so --concurrency=5 is roughly equivalent to starting 5 worker processes but shares the Python heap, the Cara DB connection pool, and the HTTP clients — much lower memory and cleaner lifecycle.",
        "--reload": "Enable auto-reload on file changes",
    },
)
class QueueWorkCommand(AutoReloadMixin, CommandBase):
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
            from cara.environment import env as _env

            default_mb = 512
            limit_mb = int(_env("WORKER_MEMORY_LIMIT_MB", default_mb))
        except Exception:
            limit_mb = 512
        # Bumped minimum so multi-thread scrape workloads (BrowserPool +
        # extractor pipelines + HTTP clients per thread) breathe without
        # bouncing. Override with the env if 2 GB is too aggressive for
        # the deploy box.
        limit_mb = max(limit_mb, 2048)
        self.memory_limit_bytes = limit_mb * 1024 * 1024
        # Queues that don't exist yet — skipped until the retry TTL expires.
        # A passive queue_declare for a missing queue closes the channel
        # (RabbitMQ returns 404) which triggers expensive reconnects every
        # poll tick. Cache the miss to avoid the loop and retry periodically
        # so newly-published queues are picked up.
        self._missing_queues: dict[str, float] = {}
        self._missing_queue_retry_s: float = 5.0

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
                return
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
            from app.support.Metrics import start_http_server as _start_metrics

            _port = _start_metrics()
            if _port:
                Log.info(f"📈 Metrics server on :{_port}/metrics")
        except ImportError:
            # Module only exists in services project — silently skip in other projects.
            pass
        except Exception as e:
            # Non-fatal: worker keeps running with no metrics exposure.
            Log.warning(f"metrics server startup failed: {e}")

        # Background DB sampler — emits domain gauges (product lifecycle,
        # queue depth, job table, entity counts) every 30s so the
        # dashboard never has to hit the database itself.
        try:
            from app.support.MetricsSampler import start as _start_sampler

            _start_sampler()
        except ImportError:
            # Module only exists in services project — silently skip in other projects.
            pass
        except Exception as e:
            Log.warning(f"metrics sampler startup failed: {e}")

        # Parse concurrency early so we can use it to gate the reload path
        # (auto-reload restarts the whole worker — fine with 1 thread, but
        # with N parallel consumer threads we want to drain them first).
        concurrency_val = 1
        if concurrency:
            try:
                concurrency_val = max(1, int(concurrency))
            except ValueError:
                self.error(f"× Invalid --concurrency value: {concurrency!r}")
                return
        self._concurrency = concurrency_val

        # Store parameters for restart
        self.store_restart_params(driver, queue, timeout, max_jobs, max_time)

        # Auto-reload only when explicitly requested — module purging
        # invalidates IoC container bindings (contract→implementation
        # identity is lost after re-import), causing resolution failures
        # like "Can't instantiate abstract class …Contract".
        if self.option("reload"):
            self.enable_auto_reload()

        # Start main worker loop
        try:
            self._run_main_loop(driver, queue, timeout, max_jobs, max_time)
        except Exception as e:
            import traceback

            self.error(f"× Worker error: {e}")
            self.error(f"× Stack trace: {traceback.format_exc()}")
        finally:
            self.cleanup_auto_reload()
            self._show_final_stats()

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
            raise Exception(
                "No driver specified and no default 'queue.default' configured"
            )

        drivers = config("queue.drivers", {})
        if driver_name not in drivers:
            raise Exception(f"Driver '{driver_name}' is not configured")

        # Parse timeout
        timeout_val = 5
        if timeout:
            try:
                timeout_val = int(timeout)
                if timeout_val < 1:
                    raise ValueError("Timeout must be at least 1 second")
            except ValueError as e:
                raise Exception(f"Invalid timeout value: {e}") from e
        else:
            # Get from driver config
            timeout_val = config(f"queue.drivers.{driver_name}.poll", 5)

        # Parse limits
        max_jobs_val = None
        if max_jobs:
            try:
                max_jobs_val = int(max_jobs)
                if max_jobs_val <= 0:
                    raise ValueError("max-jobs must be positive")
            except ValueError as e:
                raise Exception(f"Invalid max-jobs value: {e}") from e

        max_time_val = None
        if max_time:
            try:
                max_time_val = int(max_time)
                if max_time_val <= 0:
                    raise ValueError("max-time must be positive")
            except ValueError as e:
                raise Exception(f"Invalid max-time value: {e}") from e

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
            if "*" in pattern:
                expanded_queues.extend(self._expand_wildcard_pattern(pattern))
            else:
                expanded_queues.append(pattern)

        return expanded_queues if expanded_queues else ["default"]

    def _expand_wildcard_pattern(self, pattern: str) -> list:
        """Expand wildcard pattern to actual queue names.

        Two-phase expansion:
        1. Try to discover real queues from RabbitMQ Management API and
           match with fnmatch. This catches nested prefixes like
           ``notification.email.default`` when the user passes
           ``notification.*``.
        2. Fallback: generate standard priority suffixes (the old
           behaviour) so the worker starts even when RabbitMQ
           management is unavailable.
        """
        import fnmatch as _fnmatch

        # Always include standard priority levels so the worker
        # subscribes even if some queues don't exist yet in RabbitMQ.
        # Without this, a fresh broker only has queues that received
        # messages before — the worker misses queues created later.
        priority_levels = ["critical", "high", "default", "low"]

        if pattern.endswith(".*"):
            prefix = pattern[:-2]
            static = {f"{prefix}.{level}" for level in priority_levels}
        elif pattern.endswith("*"):
            prefix = pattern[:-1]
            static = {f"{prefix}.{level}" for level in priority_levels}
        else:
            return [pattern]

        # Merge with any extra queues discovered from RabbitMQ
        # (e.g. notification.email.default) that the static set misses.
        discovered = self._discover_rabbitmq_queues()
        if discovered:
            matched = {q for q in discovered if _fnmatch.fnmatch(q, pattern)}
            static |= matched

        # Sort by priority rank, NOT alphabetically. The worker polls this
        # list head-first in ``_process_queue_cycle`` and only restarts
        # from index 0 after a job runs, so alphabetical order
        # (critical, default, high, low) starved high-priority jobs behind
        # the default queue. Names that don't carry a known suffix sort
        # alphabetically *after* the priority-bearing queues to preserve
        # deterministic ordering.
        return sorted(static, key=lambda q: self._priority_sort_key(q))

    @staticmethod
    def _priority_sort_key(queue_name: str) -> tuple[int, str]:
        """Sort key that puts higher-priority queues first.

        Rank is derived from the suffix after the final dot; unknown or
        missing suffixes sort after every known priority and then
        alphabetically so the order is stable across runs.
        """
        rank = {"critical": 0, "high": 1, "default": 2, "low": 3}
        suffix = queue_name.rsplit(".", 1)[-1]
        return (rank.get(suffix, 99), queue_name)

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

            host = config("queue.connections.amqp.host", "127.0.0.1")
            mgmt_port = config("queue.connections.amqp.management_port", 15672)
            user = config("queue.connections.amqp.user", "guest")
            password = config("queue.connections.amqp.password", "guest")
            vhost = config("queue.connections.amqp.vhost", "/")

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
                f"[#e5c07b]│[/#e5c07b] [white]Queues:[/white] [dim]{len(queue_names)} queues in priority order[/dim]"
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
            f"[#e5c07b]│[/#e5c07b] [white]Poll Timeout:[/white] [dim]{config['timeout']}s[/dim]"
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
        """Run the queue worker with multiple queue priority support.

        When ``self._concurrency > 1`` we spin up N independent consumer
        threads, each with its own AMQP connection + channel. pika's
        BlockingConnection is not thread-safe across threads, so each
        thread keeps its own manager. The threads share:

        * The job processor (stateless, so safe to share).
        * The Cara DB connection pool + in-flight semaphore (module-level,
          built for multi-thread access from the start).
        * The ``_missing_queues`` cache — this IS mutated by each thread
          but only ever as ``dict[str]->float`` writes, which CPython's
          GIL makes atomic. Worst case two threads race to re-probe a
          recently-missed queue, which just costs one extra channel open.
        * ``jobs_processed`` / ``jobs_failed`` counters — incremented
          under a lock below (would otherwise race and undercount).

        The number of in-flight jobs is bounded by ``concurrency``; each
        thread processes one job at a time in its own loop so we preserve
        the at-most-once-per-thread semantics that JobProcessor assumes.
        """
        queue_names = config["queue_names"]
        concurrency = getattr(self, "_concurrency", 1)

        self._show_worker_startup_info(queue_names, concurrency)
        self.start_time = time.time()

        # Lock protecting shared counters + shutdown flag read-modify-writes.
        # shutdown_requested itself is a bool (atomic) so we read it
        # unlocked; counters genuinely need a lock.
        self._stats_lock = threading.Lock()

        if concurrency <= 1:
            # Fast path: original single-threaded loop, no thread overhead.
            from cara.configuration import config as global_config

            connection_manager = AMQPConnectionManager(global_config)
            job_processor = JobProcessor()

            try:
                while not self.shutdown_requested:
                    outcome = self._process_queue_cycle(
                        queue_names, connection_manager, job_processor, config
                    )

                    # Update terminal-attempt counters. Without this,
                    # ``jobs_processed`` and ``jobs_failed`` stay at 0
                    # for the worker's lifetime — they were only ever
                    # initialised, never incremented — which (a) made
                    # the final-stats summary lie and (b) silently
                    # neutralised --max-jobs entirely.
                    if outcome == "success":
                        self.jobs_processed += 1
                    elif outcome == "failure":
                        self.jobs_failed += 1

                    # Enforce --max-jobs / --max-time. Without this
                    # check the limits printed in the startup banner
                    # are decorative only — the worker would only
                    # exit on SIGTERM/SIGINT.
                    if self._should_stop(config):
                        self.shutdown_requested = True
                        break

                    # Sleep if no jobs found
                    if not outcome:
                        time.sleep(config["timeout"])

            finally:
                connection_manager.close()
            return

        # Multi-threaded consumer mode.
        from cara.configuration import config as global_config

        job_processor = JobProcessor()  # stateless, shared
        threads: list[threading.Thread] = []
        managers: list[AMQPConnectionManager] = []

        def _consumer_loop(slot_idx: int) -> None:
            """One consumer slot. Owns its own AMQP connection."""
            mgr = AMQPConnectionManager(global_config)
            managers.append(mgr)
            try:
                while not self.shutdown_requested:
                    try:
                        outcome = self._process_queue_cycle(
                            queue_names, mgr, job_processor, config
                        )
                    except Exception as e:
                        # Don't let a single thread's error kill the slot —
                        # log it and keep polling. The original _run_worker
                        # swallowed these via _handle_queue_error; we
                        # mirror that behaviour.
                        Log.warning(f"[worker-{slot_idx}] cycle error: {e}")
                        outcome = False

                    # Per-slot counter increments. ``_stats_lock`` (set
                    # up just above before the consumer threads start)
                    # guards concurrent increments so the cap stays
                    # accurate under --concurrency>1.
                    if outcome:
                        with self._stats_lock:
                            if outcome == "success":
                                self.jobs_processed += 1
                            elif outcome == "failure":
                                self.jobs_failed += 1

                    # Same enforcement as the single-threaded path.
                    # Any consumer slot tripping the limit asks the
                    # whole worker to drain — the main thread sees
                    # ``shutdown_requested=True`` and joins.
                    if self._should_stop(config):
                        self.shutdown_requested = True
                        break

                    if not outcome:
                        # Stagger sleeps a tiny bit so N threads don't wake
                        # up in lockstep and hammer the broker simultaneously.
                        jittered = config["timeout"] * (1.0 + (slot_idx % 4) * 0.1)
                        time.sleep(jittered)
            finally:
                mgr.close()

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
            while not self.shutdown_requested:
                if self._should_stop(config):
                    self.shutdown_requested = True
                    break
                time.sleep(1)
        finally:
            # Ask all threads to stop and let them drain gracefully.
            self.shutdown_requested = True
            for t in threads:
                try:
                    t.join(timeout=10)
                except Exception:
                    pass
            for mgr in managers:
                try:
                    mgr.close()
                except Exception:
                    pass

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
                    f"⚠️ Memory limit exceeded: {current_mb:.1f}MB > {limit_mb:.1f}MB. "
                    f"Initiating graceful shutdown for supervisor restart."
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
                                    f"⚠️ Memory limit exceeded: {current_mb:.1f}MB > {limit_mb:.1f}MB. "
                                    f"Initiating graceful shutdown for supervisor restart."
                                )
                                self.shutdown_requested = True
                                return False
                            break
            except Exception:
                pass

            return True

    def _show_worker_startup_info(self, queue_names: list, concurrency: int = 1) -> None:
        """Display worker startup information in ServeCommand style."""
        self.console.print("[bold #e5c07b]┌─ Worker Status[/bold #e5c07b]")

        if len(queue_names) > 1:
            self.console.print(
                f"[#e5c07b]│[/#e5c07b] [white]Processing:[/white] [dim]{len(queue_names)} queues in priority order[/dim]"
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

    def _process_queue_cycle(
        self,
        queue_names: list,
        connection_manager: AMQPConnectionManager,
        job_processor: JobProcessor,
        config: dict[str, Any],
    ) -> bool:
        """Process one cycle through all queues in priority order."""
        # CRITICAL FIX #3: Check memory usage after each job
        if not self._check_memory_usage():
            return False  # Memory limit exceeded, signal shutdown

        for queue_name in queue_names:
            if self.shutdown_requested:
                break

            try:
                outcome = self._process_single_queue(
                    queue_name, connection_manager, job_processor
                )
                if outcome:
                    # Memory check after successful job
                    self._check_memory_usage()
                    # Outcome is "success" / "failure" — propagate so
                    # the caller can update jobs_processed / jobs_failed
                    # counters that gate --max-jobs.
                    return outcome  # Job processed, restart from highest priority

            except Exception as e:
                self._handle_queue_error(queue_name, e, connection_manager)
                continue

        return False  # No jobs processed

    def _process_single_queue(
        self,
        queue_name: str,
        connection_manager: AMQPConnectionManager,
        job_processor: JobProcessor,
    ) -> bool:
        """Process a single queue and return True if job was processed."""
        # Skip queues we've recently seen as missing. A failed passive
        # declare closes the channel, so without this cache every poll
        # tick triggers a reconnect storm.
        now = time.time()
        missed_at = self._missing_queues.get(queue_name)
        if missed_at is not None and (now - missed_at) < self._missing_queue_retry_s:
            return False

        channel = connection_manager.create_channel()
        if not channel:
            return False

        try:
            # First try passive declare (cheap — no arg mismatch risk).
            # If the queue doesn't exist yet, RabbitMQ returns 404 and
            # closes the channel. In that case we open a fresh channel
            # and create the queue with a normal declare so the worker
            # doesn't have to wait for the publisher to go first.
            try:
                channel.queue_declare(queue=queue_name, durable=True, passive=True)
            except Exception:
                # Channel is dead after 404 — get a new one.
                channel = connection_manager.create_channel()
                if not channel:
                    self._missing_queues[queue_name] = now
                    return False
                try:
                    channel.queue_declare(queue=queue_name, durable=True)
                except Exception:
                    # Truly cannot create — cache miss briefly.
                    self._missing_queues[queue_name] = now
                    return False

            # Queue exists — drop from miss cache if we had marked it.
            self._missing_queues.pop(queue_name, None)

            # Non-blocking message retrieval
            method_frame, header_frame, body = channel.basic_get(queue=queue_name)

            if method_frame:
                # Process the job — pass the real queue name through
                # so metrics label it correctly instead of falling
                # back to "unknown".
                return job_processor.process_message(
                    channel,
                    method_frame,
                    body,
                    queue_name=queue_name,
                )

            return False  # No message

        finally:
            # Always close channel
            try:
                channel.close()
            except Exception:
                pass

    def _handle_queue_error(
        self,
        queue_name: str,
        error: Exception,
        connection_manager: AMQPConnectionManager,
    ) -> None:
        """Handle queue processing errors."""
        error_msg = str(error)

        # Skip queues that don't exist
        if "NOT_FOUND" not in error_msg:
            if "connection" in error_msg.lower() or "closed" in error_msg.lower():
                # Connection issue, reset connection
                connection_manager.connection = None
            else:
                self.error(f"Error checking queue {queue_name}: {error_msg}")

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
        if config["max_jobs"] and terminal_jobs >= config["max_jobs"]:
            self.info(
                f"🎯 Reached maximum job limit ({config['max_jobs']}) "
                f"[processed={self.jobs_processed} failed={self.jobs_failed}]"
            )
            return True

        if config["max_time"] and (time.time() - self.start_time) >= config["max_time"]:
            self.info(f"⏰ Reached maximum runtime ({config['max_time']} seconds)")
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
        except Exception:
            # If Job model not available or DB error, skip enhanced stats
            pass

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
        """Main worker loop - called by AutoReloadMixin on restart."""
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
            return

        # Show worker configuration
        self._show_config(worker_config)

        # Clean up connections before starting
        self._cleanup_connections_for_restart()

        # Reset counters for fresh start
        self.jobs_processed = 0
        self.jobs_failed = 0

        # Run the worker
        self._run_worker(worker_config)

    def _cleanup_connections_for_restart(self):
        """Clean up connections specifically for restart - simple and effective."""
        try:
            from cara.facades.Queue import Queue

            # Simple approach: Just clear all references without trying to close broken connections
            drivers = config("queue.drivers", {})
            for driver_name in drivers.keys():
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

        except Exception:
            pass

    def _cleanup_watching(self):
        """Cleanup file watching resources."""
        if hasattr(self, "command_watcher") and self.command_watcher:
            try:
                self.command_watcher.shutdown()
            except Exception:
                pass
