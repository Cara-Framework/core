"""Queue message execution and durable settlement orchestration."""

from __future__ import annotations

import asyncio
import builtins
import contextlib
import inspect
import time
from typing import Any

import pendulum

from cara import observability
from cara.configuration import config
from cara.exceptions import QueueException
from cara.facades import Log, Queue
from cara.queues import SignedJsonJobSerializer, instantiate_job
from cara.queues.middleware import run_through_middleware_async

from .ActiveJobCancellationRegistry import ActiveJobCancellationRegistry
from .DeliverySettlementError import DeliverySettlementError

try:
    from cara.observability.MetricsBase import MetricsBase
except ImportError, RuntimeError:  # pragma: no cover
    MetricsBase = None  # type: ignore[assignment]

JobProcessor: type[Any] | None = None


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


def _process_message(
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
        except ImportError, RuntimeError, AttributeError, OSError:
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
            clock_skew_seconds=int(config("queue.drivers.amqp.clock_skew_seconds", 30)),
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
            except ImportError, RuntimeError, AttributeError, OSError:
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
                    raise TimeoutError(f"Job exceeded timeout of {job_timeout}s") from e
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
        # Terminal catch: the outcome is returned, never re-raised, so the
        # explicit capture is Sentry's only path to a job timeout.
        observability.capture_exception(timeout_error)

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
        # Terminal catch: the worker consumes the failure ("failed
        # gracefully"), so the explicit capture is Sentry's only path.
        observability.capture_exception(job_error)

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


def _bind_job_processor(processor_class: type[Any]) -> None:
    """Bind the public processor facade without creating an import cycle."""
    global JobProcessor
    JobProcessor = processor_class
