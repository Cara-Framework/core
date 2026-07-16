"""
Job Bus - Laravel-style unified job dispatcher.

Automatically decides whether to run jobs synchronously or dispatch to queue
based on execution context. Inspired by Laravel's Bus facade.
"""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from cara.queues.contracts import Queueable
    from cara.queues.tracking import JobTracker


class Bus:
    """
    Laravel-style Bus for unified job dispatching.

    Automatically handles sync/async execution based on context:
    - Sync context (ExecutionContext.sync()): Runs immediately with tracking
    - Async context: Dispatches to queue

    This eliminates if/else blocks throughout the codebase.

    Example:
        >>> # In sync context (testing, CLI with --sync)
        >>> with ExecutionContext.sync():
        ...     await Bus.dispatch(job)  # Runs immediately

        >>> # In async context (queue worker, default)
        >>> await Bus.dispatch(job)  # Dispatches to queue
    """

    @staticmethod
    async def dispatch(
        job: Queueable,
        routing_key: str | None = None,
        delay: float | None = None,
        queue: str | None = None,
    ) -> Any:
        """
        Dispatch job with automatic sync/async handling.

        Context-aware dispatch:
        - ExecutionContext.sync() → Run immediately with full tracking
        - Default → Dispatch through the signed AMQP queue rail

        Args:
            job: Job instance to dispatch.
            routing_key: Optional routing key for topic exchange (e.g. ``collection.high``).
            delay: Optional delay in seconds before the job becomes visible on the
                queue. Ignored in sync mode (the delay of zero is immediate) and
                forwarded through ``PendingDispatch.delay()`` to the durable AMQP
                delayed-delivery store.
            queue: Optional queue name override.

        Returns:
            Job result if sync mode, None if queued.

        Example:
            >>> # Context-aware (recommended)
            >>> await Bus.dispatch(MyJob(id=123), routing_key="processing.high")

            >>> # Explicit sync
            >>> with ExecutionContext.sync():
            ...     result = await Bus.dispatch(MyJob(id=123))

            >>> # Delayed dispatch (retry-with-backoff pattern)
            >>> await Bus.dispatch(MyJob(id=123), delay=120)
        """
        # Check execution context
        from cara.context import ExecutionContext

        is_sync = ExecutionContext.is_sync()

        if is_sync:
            # Run synchronously WITH tracking. Any `delay` is intentionally
            # ignored in sync mode — the caller asked for immediate execution.
            return await Bus._run_sync_with_tracking(job)
        else:
            # Check if job is UniqueJob — use a single atomic acquire
            # instead of check-then-acquire (TOCTOU race).
            from cara.queues.contracts import UniqueJob

            if isinstance(job, UniqueJob):
                uid = job.unique_id()
                if not UniqueJob.acquire_unique_lock(uid, job.unique_for):
                    # Lock already held — another instance is pending/processing.
                    from cara.facades import Log

                    Log.debug("UniqueJob skipped (lock held): %s", uid)
                    try:
                        from cara.observability.Metrics import MetricsBase as _M

                        _M.idempotency_total.labels(
                            scope="unique_job", outcome="collision"
                        ).inc()
                    except Exception:
                        pass
                    return None
                try:
                    from cara.observability.Metrics import MetricsBase as _M

                    _M.idempotency_total.labels(scope="unique_job", outcome="fresh").inc()
                except Exception:
                    pass

            # Dispatch to queue. Wrap in try/except so a failed
            # ``push()`` (broker down or AMQP unroutable) releases the
            # unique-job lock — without
            # this release, the next legitimate dispatch for the same
            # ``unique_id`` is silently dropped for the full
            # ``unique_for`` window (default 1h) even though no job
            # ever ran.
            try:
                params = Bus.get_dispatch_params(job)
                dispatch_call = job.__class__.dispatch(**params)
                if routing_key:
                    dispatch_call.with_routing_key(routing_key)
                if queue:
                    dispatch_call.on_queue(queue)
                if delay and hasattr(dispatch_call, "delay"):
                    dispatch_call.delay(delay)
                # The terminal call is mandatory: builder destruction never
                # queues work, and dispatch failures must reach the caller.
                dispatch_call.dispatch()
            except Exception:
                # Dispatch failed before the job was queued — release
                # the unique lock so the caller can retry.
                if isinstance(job, UniqueJob):
                    with contextlib.suppress(ImportError, ConnectionError, TimeoutError, OSError, RuntimeError):
                        UniqueJob.release_unique_lock(job.unique_id())
                raise

            # Prometheus dispatch counter — bounded by the (queue, job)
            # label pair; "unknown" covers jobs that don't carry an
            # explicit queue attribute. Guarded so a metrics hiccup never
            # breaks dispatch.
            try:
                from cara.observability.Metrics import MetricsBase as _M

                _queue_lbl = (
                    queue or routing_key or getattr(job, "queue", None) or "unknown"
                )
                _M.queue_dispatches_total.labels(
                    queue=str(_queue_lbl),
                    job_class=job.__class__.__name__,
                ).inc()
            except Exception:
                pass
            return None

    @staticmethod
    async def _run_sync_with_tracking(job: Queueable) -> Any:
        """
        Run job synchronously with full tracking support.

        Tracking flow (automatic):
        1. Create job record (unified job table) via JobTracker
        2. Track entity_id, pipeline_id in metadata
        3. Update status: pending → processing → completed/failed
        4. Track performance, retries, conflicts

        Args:
            job: Job instance with Trackable trait

        Returns:
            Job result

        Raises:
            Exception: Re-raises job exceptions after tracking failure
        """
        # Check if job has tracking enabled (Trackable trait)
        has_tracking = hasattr(job, "_tracking_enabled") and job._tracking_enabled

        tracker: JobTracker | None = None
        job_id: int | None = None

        if has_tracking:
            # Resolve JobTracker from container (registered in ApplicationProvider)
            tracker = Bus._resolve_job_tracker()

            if tracker:
                # Create job record via JobTracker (unified with queue dispatch)
                job_name = job.__class__.__name__
                job_class = f"{job.__class__.__module__}.{job.__class__.__name__}"
                queue = getattr(job, "queue", "default")

                # Extract job parameters for payload
                payload = Bus.get_dispatch_params(job)

                job_id = tracker.create_job_record(
                    job_name=job_name,
                    job_class=job_class,
                    queue=queue,
                    execution_mode="sync",
                    payload=payload,
                )

                # Set job_id so Trackable can use it for unified job tracking
                if job_id:
                    job._db_job_id = job_id

            # Start tracking (Trackable trait handles entity_id tracking)
            if hasattr(job, "_start_tracking"):
                job._start_tracking()

            # Update job status to processing
            if tracker and job_id:
                tracker.update_job_status(job_id, "processing")

            # Mark as processing in unified job table
            if hasattr(job, "_mark_processing"):
                job._mark_processing()

        # Run the job through middleware pipeline.
        #
        # ``fresh_dispatch_scope`` clears the in-flight event-dispatch
        # stack for the duration of the job's execution. Sync jobs run
        # INLINE in the caller's async context, so any contextvar set
        # by the caller's listener fan-out leaks into the job's own
        # event chain. In particular, when a listener triggered by
        # event ``X`` dispatches a child job whose ``handle()`` also
        # fires event ``X`` (for a DIFFERENT entity — e.g. variation
        # sibling discovery in ``AmazonPostCollectionListener``), the
        # cycle detector pre-fix saw ``X`` already in the stack and
        # raised ``EventDispatchCycleException``. Queued mode doesn't
        # have this problem because each worker has its own contextvar
        # context; sync mode shares the caller's context, and that's
        # where the leak happens. Resetting at this boundary preserves
        # cycle protection WITHIN the job's own listener chain (the
        # stack starts empty but accumulates as the job dispatches its
        # own events) while letting legitimate fan-out trees run.
        try:
            import asyncio

            from cara.events.Event import fresh_dispatch_scope
            from cara.queues.contracts import JobThrottledException
            from cara.queues.middleware import run_through_middleware_async

            async def job_handler(j):
                app = Bus._resolve_application()
                if app is not None and hasattr(app, "call"):
                    out = app.call(j.handle)
                else:
                    out = j.handle()
                if asyncio.iscoroutine(out):
                    return await out
                return out

            with fresh_dispatch_scope():
                result = await run_through_middleware_async(job, job_handler)

            # ``None`` is a legitimate successful return: every pipeline
            # stage routes its work through wrap_with_idempotency(_do_work) and
            # _do_work returns None on success, so ``result is None`` is the
            # NORMAL success case — not a skip. Recording completion only for a
            # non-None result left every Trackable pipeline job stuck at
            # 'processing' forever on --sync runs. The idempotency layer caches
            # None via its own sentinel rather than treating it as "did
            # nothing"; mirror that here and record completion unconditionally.

            # Mark as success in unified job table
            if has_tracking and hasattr(job, "_mark_success"):
                job._mark_success()

            # Update job record status
            if tracker and job_id:
                tracker.update_job_status(job_id, "completed")

            return result

        except JobThrottledException:
            if tracker and job_id:
                tracker.update_job_status(job_id, "throttled")
            return None

        except Exception as e:
            # Mark as failed in unified job table
            if has_tracking and hasattr(job, "_mark_failed"):
                job._mark_failed(str(e), should_retry=False)

            # Update job record status
            if tracker and job_id:
                tracker.update_job_status(job_id, "failed")

            raise

    @staticmethod
    def _resolve_application() -> Any:
        """Return the global application instance when available (sync Bus dispatch)."""
        import builtins

        if not hasattr(builtins, "app"):
            return None
        try:
            return builtins.app()
        except (TypeError, AttributeError, RuntimeError):
            return None

    @staticmethod
    def _resolve_job_tracker() -> JobTracker | None:
        """
        Resolve JobTracker from container.

        If not registered, returns None (tracking disabled).

        Returns:
            JobTracker instance or None
        """
        app_instance = Bus._resolve_application()
        if app_instance and app_instance.has("JobTracker"):
            return app_instance.make("JobTracker")

        return None

    @staticmethod
    def get_dispatch_params(job: Queueable) -> dict:
        """
        Extract dispatch parameters from job instance.

        Handles Pydantic models and other complex objects by converting them
        to serializable dictionaries.

        Args:
            job: Job instance

        Returns:
            Dict of parameters for dispatch
        """
        # Get all init parameters from job
        # Exclude internal attributes, queue-specific fields, and runtime objects
        excluded_keys = {
            "queue",
            "attempts",
            "routing_key",
            "connection",
            "delay",
            "timeout",
            "tries",
            "backoff",
            "kwargs",
            # Runtime objects that should be reconstructed by the job
            "job_metadata",
            "job_context",
            "job_tracking_id",
            "is_cancelled",
            "repository",
            # Runtime-only DB fencing value acquired inside handle(); never
            # serialize it into the immutable broker envelope.
            "claim_token",
        }

        params = {}
        if hasattr(job, "__dict__"):
            for key, value in job.__dict__.items():
                if not key.startswith("_") and key not in excluded_keys:
                    params[key] = value

            # Special handling: if job has kwargs dict, merge it into params
            # This ensures all init parameters are passed correctly
            if "kwargs" in job.__dict__ and isinstance(job.__dict__["kwargs"], dict):
                for k, v in job.__dict__["kwargs"].items():
                    if k not in params and k not in excluded_keys:
                        params[k] = v

        return params
