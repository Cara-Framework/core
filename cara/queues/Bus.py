"""
Job Bus - Laravel-style unified job dispatcher.

Automatically decides whether to run jobs synchronously or dispatch to queue
based on execution context. Inspired by Laravel's Bus facade.
"""

from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from cara.queues.contracts import Queueable
    from cara.queues.tracking import JobTracker
    from cara.queues.contracts import UniqueJob


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
        job: "Queueable",
        routing_key: Optional[str] = None,
        delay: Optional[float] = None,
        queue: Optional[str] = None,
    ) -> Any:
        """
        Dispatch job with automatic sync/async handling.

        Context-aware dispatch:
        - ExecutionContext.sync() → Run immediately with full tracking
        - Default → Dispatch to queue (RabbitMQ/Redis/Database)

        Args:
            job: Job instance to dispatch.
            routing_key: Optional routing key for topic exchange (e.g. ``collection.high``).
            delay: Optional delay in seconds before the job becomes visible on the
                queue. Ignored in sync mode (the delay of zero is immediate) and
                forwarded to the driver via ``PendingDispatch.delay()`` in async
                mode. Accepts any numeric value that the driver can interpret
                (AMQP uses milliseconds internally, Redis/Database treat it as
                seconds — ``PendingDispatch`` normalizes this).
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
            # Check if job is UniqueJob and already locked
            from cara.queues.contracts import UniqueJob
            if isinstance(job, UniqueJob):
                uid = job.unique_id()
                if UniqueJob.is_unique_locked(uid):
                    from cara.facades import Log
                    Log.debug(f"UniqueJob skipped (already locked): {uid}")
                    try:
                        from app.support.Metrics import Metrics as _M
                        _M.idempotency_total.labels(scope="unique_job", outcome="collision").inc()
                    except Exception:
                        pass
                    return None  # Silent drop
                if not UniqueJob.acquire_unique_lock(uid, job.unique_for):
                    # Another worker acquired the lock between our check
                    # and our acquire — respect the lock and drop silently.
                    from cara.facades import Log
                    Log.debug(f"UniqueJob lock race lost: {uid}")
                    try:
                        from app.support.Metrics import Metrics as _M
                        _M.idempotency_total.labels(scope="unique_job", outcome="collision").inc()
                    except Exception:
                        pass
                    return None
                try:
                    from app.support.Metrics import Metrics as _M
                    _M.idempotency_total.labels(scope="unique_job", outcome="fresh").inc()
                except Exception:
                    pass

            # Dispatch to queue
            params = Bus.get_dispatch_params(job)
            dispatch_call = job.__class__.dispatch(**params)
            if routing_key:
                dispatch_call.withRoutingKey(routing_key)
            if queue:
                # PendingDispatch exposes both camelCase and snake_case helpers.
                # Prefer onQueue() for Laravel parity; fall back gracefully if
                # a driver supplies a different chainable.
                if hasattr(dispatch_call, "onQueue"):
                    dispatch_call.onQueue(queue)
                elif hasattr(dispatch_call, "on_queue"):
                    dispatch_call.on_queue(queue)
            if delay:
                if hasattr(dispatch_call, "delay"):
                    dispatch_call.delay(delay)

            # Prometheus dispatch counter — bounded by the (queue, job)
            # label pair; "unknown" covers jobs that don't carry an
            # explicit queue attribute. Safe to no-op if Metrics
            # isn't available (e.g. cara imported standalone in tests).
            try:
                from app.support.Metrics import Metrics as _M
                _queue_lbl = (
                    queue or routing_key or getattr(job, "queue", None) or "unknown"
                )
                _M.queue_dispatches_total.labels(
                    queue=str(_queue_lbl),
                    job=job.__class__.__name__,
                ).inc()
            except Exception:
                pass
            return None

    @staticmethod
    async def _run_sync_with_tracking(job: "Queueable") -> Any:
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

        tracker: Optional["JobTracker"] = None
        job_id: Optional[int] = None

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

                job_id = tracker.create_sync_job_record(
                    job_name=job_name, job_class=job_class, queue=queue, payload=payload
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

        # Run the job through middleware pipeline
        try:
            from cara.queues.middleware import run_through_middleware_async

            async def job_handler(j):
                return await j.handle()

            result = await run_through_middleware_async(job, job_handler)

            # If middleware skipped the job (returned None), still mark success
            if result is None:
                if has_tracking and hasattr(job, "_mark_success"):
                    job._mark_success()
                if tracker and job_id:
                    tracker.update_job_status(job_id, "completed")
                return None

            # Mark as success in unified job table
            if has_tracking and hasattr(job, "_mark_success"):
                job._mark_success()

            # Update job record status
            if tracker and job_id:
                tracker.update_job_status(job_id, "completed")

            return result

        except Exception as e:
            # Mark as failed in unified job table
            if has_tracking and hasattr(job, "_mark_failed"):
                job._mark_failed(str(e), should_retry=False)

            # Update job record status
            if tracker and job_id:
                tracker.update_job_status(job_id, "failed")

            raise

    @staticmethod
    def _resolve_job_tracker() -> Optional["JobTracker"]:
        """
        Resolve JobTracker from container.

        If not registered, returns None (tracking disabled).

        Returns:
            JobTracker instance or None
        """
        import builtins

        if not hasattr(builtins, "app"):
            return None

        app_instance = builtins.app()
        if app_instance and app_instance.has("JobTracker"):
            return app_instance.make("JobTracker")

        return None

    @staticmethod
    def get_dispatch_params(job: "Queueable") -> dict:
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
            "repository",
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
