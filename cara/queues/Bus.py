"""
Job Bus - Laravel-style unified job dispatcher.

Automatically decides whether to run jobs synchronously or dispatch to queue
based on execution context. Inspired by Laravel's Bus facade.
"""

from __future__ import annotations

import asyncio
import builtins
import inspect
import uuid
from typing import TYPE_CHECKING, Any

import cara.facades as facades
from cara.context import ExecutionContext
from cara.observability import MetricsBase
from cara.queues.contracts import JobThrottledException, UniqueJob
from cara.queues.middleware import run_through_middleware_async

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
            Job result in sync mode. Queued dispatches return the durable
            queue-delivery UUID written by the queue driver. A coalesced
            :class:`UniqueJob` returns the UUID of the already-open delivery
            when one is known.

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

        is_sync = ExecutionContext.is_sync()

        if is_sync:
            # Run synchronously WITH tracking. Any `delay` is intentionally
            # ignored in sync mode — the caller asked for immediate execution.
            return await Bus._run_sync_with_tracking(job)
        else:
            # PostgreSQL's delivery ledger is the uniqueness authority. The
            # queue driver inserts the delivery and its unique key in one
            # transaction, so a duplicate can return the already-pollable id
            # without a Redis→database crash window.

            reserved_job_id: str | None = None
            unique_key: str | None = None
            if isinstance(job, UniqueJob):
                reserved_job_id = str(uuid.uuid4())
                unique_key = job.unique_id()

            params = Bus.get_dispatch_params(job)
            dispatch_call = job.__class__.dispatch(**params)
            if routing_key:
                dispatch_call.with_routing_key(routing_key)
            if queue:
                dispatch_call.on_queue(queue)
            if delay and hasattr(dispatch_call, "delay"):
                dispatch_call.delay(delay)
            if reserved_job_id is not None:
                dispatch_call.with_job_id(reserved_job_id)
                dispatch_call.with_unique_key(unique_key)
            # The terminal call is mandatory: builder destruction never
            # queues work, and dispatch failures must reach the caller.
            job_id = dispatch_call.dispatch()

            if reserved_job_id is not None:
                outcome = "fresh" if str(job_id) == reserved_job_id else "collision"
                MetricsBase.safe_inc(
                    MetricsBase.idempotency_total,
                    {"scope": "unique_job", "outcome": outcome},
                )

            # Prometheus dispatch counter — bounded by the (queue, job)
            # label pair; "unknown" covers jobs that don't carry an
            # explicit queue attribute. Guarded so a metrics hiccup never
            # breaks dispatch.
            _queue_lbl = queue or routing_key or getattr(job, "queue", None) or "unknown"
            MetricsBase.safe_inc(
                MetricsBase.queue_dispatches_total,
                {"queue": str(_queue_lbl), "job_class": job.__class__.__name__},
            )
            return job_id

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

            # Mark as processing in unified job table.
            #
            # The Trackable ``_mark_*`` hooks delegate straight to the same
            # JobTracker, so an extra ``tracker.update_job_status`` call here
            # would write the SAME row twice with a different vocabulary —
            # and on the terminal transition the second write clobbered
            # ``success`` with ``completed``. One writer per transition.
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
        # fires event ``X`` for a different entity (e.g. sibling-record
        # discovery), the
        # cycle detector pre-fix saw ``X`` already in the stack and
        # raised ``EventDispatchCycleException``. Queued mode doesn't
        # have this problem because each worker has its own contextvar
        # context; sync mode shares the caller's context, and that's
        # where the leak happens. Resetting at this boundary preserves
        # cycle protection WITHIN the job's own listener chain (the
        # stack starts empty but accumulates as the job dispatches its
        # own events) while letting legitimate fan-out trees run.
        try:
            from cara.events._DispatchScope import (
                _fresh_dispatch_scope,  # local: cycle with cara.events._DispatchScope
            )

            async def job_handler(j):
                app = Bus._resolve_application()
                if app is not None and hasattr(app, "call"):
                    out = app.call(j.handle)
                else:
                    out = j.handle()
                if asyncio.iscoroutine(out):
                    return await out
                return out

            # A synchronous dispatch is the framework-owned transaction
            # boundary for the levels IT opens — and only those. Sync
            # dispatch runs inline in the caller's asyncio task and therefore
            # shares the caller's ContextVar-pinned connection registry, so
            # finalizing *every* open level seized the caller's ambient
            # business transaction: ``with DB.transaction(): repo.write();
            # await Bus.dispatch(job)`` had the business write committed early
            # on success (a later failure in the same use case could no longer
            # undo it), had its already-written rows rolled back underneath it
            # on failure, and then saw ``No active transaction found for
            # connection: app`` from its own ``__exit__`` instead of the real
            # job error. DOCTRINE §8 leaves the business transaction with the
            # use-case service.
            #
            # Recording the depth first and unwinding back to it keeps the
            # original guarantee intact — a job that leaks its own open level
            # is still committed before the next sync pipeline stage runs, so
            # that stage's ``with db.transaction()`` is a real transaction and
            # not a SAVEPOINT whose release never persists — while leaving the
            # caller's levels, and the after_commit/after_rollback callbacks
            # registered at them, exactly where they were.

            baseline_level = facades.DB.transaction_level()

            try:
                with _fresh_dispatch_scope():
                    result = await run_through_middleware_async(job, job_handler)
            except BaseException:
                facades.DB.rollback_transactions_above(baseline_level)
                raise
            else:
                facades.DB.commit_transactions_above(baseline_level)

            # ``None`` is a legitimate successful return: every pipeline
            # stage routes its work through wrap_with_idempotency(_do_work) and
            # _do_work returns None on success, so ``result is None`` is the
            # NORMAL success case — not a skip. Recording completion only for a
            # non-None result left every Trackable pipeline job stuck at
            # 'processing' forever on --sync runs. The idempotency layer caches
            # None via its own sentinel rather than treating it as "did
            # nothing"; mirror that here and record completion unconditionally.

            # Mark as success in unified job table (single writer — see
            # the ``_mark_processing`` note above).
            if has_tracking and hasattr(job, "_mark_success"):
                job._mark_success()

            return result

        except JobThrottledException:
            if tracker and job_id:
                tracker.update_job_status(job_id, "throttled")
            return None

        except Exception as e:
            # Mark as failed in unified job table (single writer — the hook
            # also owns retry / dead-letter routing, which the plain status
            # update did not).
            if has_tracking and hasattr(job, "_mark_failed"):
                job._mark_failed(str(e), should_retry=False)

            raise

    @staticmethod
    def _resolve_application() -> Any:
        """Return the global application instance when available (sync Bus dispatch)."""

        if not hasattr(builtins, "app"):
            return None
        try:
            return builtins.app()
        except TypeError, AttributeError, RuntimeError:
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
        Extract the constructor-owned state required to rebuild a job.

        Args:
            job: Job instance

        Returns:
            Dict of parameters for dispatch
        """
        if not hasattr(job, "__dict__"):
            return {}

        state = vars(job)
        parameter_names: set[str] = set()
        accepts_stored_kwargs = False

        # A subclass commonly accepts ``**kwargs`` and forwards framework-owned
        # options to a base job. Walk the complete constructor chain so those
        # explicit base parameters remain part of the durable wire contract.
        for base in job.__class__.__mro__:
            constructor = base.__dict__.get("__init__")
            if constructor is None:
                continue
            try:
                signature = inspect.signature(constructor)
            except TypeError, ValueError:
                continue
            for name, parameter in signature.parameters.items():
                if name == "self":
                    continue
                if parameter.kind in (
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                    inspect.Parameter.KEYWORD_ONLY,
                ):
                    parameter_names.add(name)
                elif parameter.kind is inspect.Parameter.VAR_KEYWORD:
                    accepts_stored_kwargs = True

        params = {name: state[name] for name in parameter_names if name in state}

        # Some dynamic jobs retain their original ``**kwargs`` explicitly.
        # That mapping is constructor input by definition; ordinary derived
        # public attributes are not.
        stored_kwargs = state.get("kwargs")
        if accepts_stored_kwargs and isinstance(stored_kwargs, dict):
            for name, value in stored_kwargs.items():
                if isinstance(name, str) and name not in params:
                    params[name] = value

        return params
