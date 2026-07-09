"""
Base class for queueable tasks in the Cara framework.

This module provides the foundation for creating background tasks with retry capabilities and
failure handling. Includes automatic serialization support and job cancellation.
"""

from __future__ import annotations

import asyncio
from typing import Any

from cara.queues.JobStateManager import get_job_state_manager

from .CancellableJob import CancellableJob, JobCancelledException
from .SerializesModels import SerializesModels


class PendingDispatch:
    """
    PendingDispatch for method chaining.

    Allows chaining like: MyJob.dispatch().on_queue('high').delay(30)
    Enhanced with routing key support for topic exchange.
    """

    def __init__(self, job_instance):
        """Initialize with job instance."""
        self.job = job_instance
        self._queue_name = getattr(job_instance, "queue", "default")
        self._delay = None
        self._connection = None
        self._routing_key = None
        self._use_exchange = False
        self._exchange_name: str | None = None
        # Defer the push until the enclosing DB transaction commits —
        # set by .after_commit() or the ShouldDispatchAfterCommit marker.
        self._after_commit = False
        # Idempotency guard — __del__ must not redispatch or raise during GC.
        self._dispatched = False

    def on_queue(self, queue: str) -> PendingDispatch:
        """Set the queue name."""
        self._queue_name = queue
        if hasattr(self.job, "queue"):
            self.job.queue = queue
        return self

    def delay(self, seconds: int) -> PendingDispatch:
        """Set delay in seconds."""
        self._delay = seconds
        return self

    def on_connection(self, connection: str) -> PendingDispatch:
        """Set connection."""
        self._connection = connection
        return self

    def with_routing_key(self, routing_key: str) -> PendingDispatch:
        """Set routing key for topic exchange dispatch."""
        self._routing_key = routing_key
        self._use_exchange = True
        return self

    def to_exchange(self, exchange_name: str | None = None) -> PendingDispatch:
        """Force dispatch to specific exchange."""
        self._exchange_name = exchange_name
        self._use_exchange = True
        return self

    def after_commit(self) -> PendingDispatch:
        """Defer the push until the enclosing DB transaction commits.

        See :class:`cara.queues.contracts.ShouldDispatchAfterCommit` for
        the full semantics (immediate when no transaction is open,
        discarded on rollback).
        """
        self._after_commit = True
        return self

    def __del__(self):
        """Auto-dispatch when PendingDispatch is garbage collected (Laravel pattern).

        __del__ runs during GC and must never raise — Python would otherwise
        print "Exception ignored in ..." to stderr, polluting logs. Also skip
        if the caller already dispatched explicitly (idempotency).
        """
        try:
            if getattr(self, "_dispatched", False):
                return
            self._dispatch_now()
        except Exception as e:
            # Log quietly — the dispatch call site already raises if caller
            # invoked _dispatch_now() directly, so this path only matters for
            # fire-and-forget usage (MyJob.dispatch(...).with_routing_key(...)).
            try:
                from cara.facades import Log

                Log.error("PendingDispatch auto-dispatch failed during GC: %s", e, category='cara.queue')
            except (ImportError, RuntimeError):
                pass

    def _dispatch_now(self):
        """
        Dispatch job to queue.

        IMPORTANT: NO FALLBACK - If queue dispatch fails, exception is raised.
        Idempotent: second call returns the prior job id without redispatching.

        After-commit deferral: when the job opts in (``.after_commit()``
        or the ShouldDispatchAfterCommit marker) and a DB transaction is
        open in this context, the actual push is registered on the
        transaction and fires right after the OUTERMOST commit — or is
        discarded entirely on rollback. With no transaction open the push
        happens immediately.
        """
        if getattr(self, "_dispatched", False):
            return getattr(self.job, "job_tracking_id", None)

        from .ShouldDispatchAfterCommit import ShouldDispatchAfterCommit

        if self._after_commit or isinstance(self.job, ShouldDispatchAfterCommit):
            from cara.eloquent import DatabaseManager

            # Mark dispatched up front so __del__'s GC-time auto-dispatch
            # and repeated _dispatch_now calls can't double-register.
            self._dispatched = True
            outcome: list = []
            DatabaseManager.get_instance().after_commit(
                lambda: outcome.append(self._push())
            )
            # Immediate mode (no open transaction) ran the push
            # synchronously — hand back the job id. Deferred mode has
            # nothing to return yet.
            return outcome[0] if outcome else None

        job_id = self._push()
        self._dispatched = True
        return job_id

    def _push(self):
        """Perform the actual broker push (no idempotency guard — the
        caller manages ``_dispatched``)."""
        # Check if we should use exchange routing
        if self._use_exchange and self._routing_key:
            return self._dispatch_via_exchange()

        # Standard queue dispatch
        from cara.facades import Queue

        # Set final queue properties
        if hasattr(self.job, "queue"):
            self.job.queue = self._queue_name

        if self._delay and hasattr(self.job, "delay"):
            self.job.delay = self._delay

        # Push to queue (will raise exception if fails). A requested delay
        # MUST route through Queue.later (→ AMQPDriver.schedule, which sets the
        # broker x-delay header) — Queue.push ignores job.delay and would fire
        # the job immediately, silently dropping the delay. A delayed retry
        # would then fire at once and collide on the idempotency cache,
        # dedup-skipping against the attempt it was meant to retry.
        if self._delay:
            job_id = Queue.later(self._delay, self.job)
        else:
            job_id = Queue.push(self.job)

        # Set tracking ID
        if hasattr(self.job, "set_tracking_id"):
            self.job.set_tracking_id(str(job_id))

        return job_id

    def _dispatch_via_exchange(self):
        """
        Dispatch job via topic exchange with routing key.

        NO FALLBACK - If exchange dispatch fails, exception is raised.
        """
        from cara.queues.exchanges import TopicExchange

        # Get or create exchange — if _exchange_name is None, TopicExchange reads
        # the default from config('queue.topic_exchange_name').
        exchange_name = getattr(self, "_exchange_name", None)
        exchange = TopicExchange(exchange_name)

        # Set job properties
        if self._delay and hasattr(self.job, "delay"):
            self.job.delay = self._delay

        # Dispatch via exchange (will raise exception if fails). Forward the
        # delay so dispatch_job routes through the delayed-message path after
        # it resolves the target queue (Queue.push there also ignores
        # job.delay; see TopicExchange.dispatch_job).
        job_id = exchange.dispatch_job(
            routing_key=self._routing_key,
            job_instance=self.job,
            delay=self._delay,
        )

        # Set tracking ID
        if hasattr(self.job, "set_tracking_id"):
            self.job.set_tracking_id(str(job_id))

        return job_id


class Queueable(SerializesModels, CancellableJob):
    """
    Makes classes Queueable with Laravel-style dispatch.

    The Queueable class is responsible for handling background tasks.
    Includes automatic serialization, cancellation support, and universal job tracking.
    """

    run_again_on_fail = True
    run_times = 3

    def __init__(self, *args, **kwargs):
        """Initialize queueable job."""
        super().__init__()  # CancellableJob.__init__() handles its own initialization
        self.job_tracking_id: str | None = None
        self._job_state_manager = get_job_state_manager()
        self._job_tracker: Any | None = None  # Lazy-loaded from container
        self._db_job_id: int | None = None  # Database job ID for unified tracking

        # Laravel-style properties
        self.queue = "default"
        self.delay = None
        self.connection = None

    def set_tracking_id(self, tracking_id: str) -> Queueable:
        """
        Set job tracking ID for cancellation management.

        Args:
            tracking_id: Unique identifier for job tracking

        Returns:
            self: For method chaining
        """
        self.job_tracking_id = tracking_id
        return self

    def should_continue(self) -> bool:
        """
        Check if job should continue execution.

        Override this method to implement custom cancellation logic.
        Default implementation checks job state manager.

        Returns:
            bool: True if job should continue, False if cancelled
        """
        if not self.job_tracking_id:
            return True

        return not self._job_state_manager.is_job_cancelled(self.job_tracking_id)

    def check_cancellation(self, operation: str = "operation") -> None:
        """
        Check if job has been cancelled and raise exception if so.

        Args:
            operation: Name of the operation being checked (for logging)

        Raises:
            JobCancelledException: If the job has been cancelled
        """
        if not self.should_continue():
            raise JobCancelledException(
                f"Job {self.job_tracking_id} was cancelled during {operation}"
            )

    def register_job(self, context: dict) -> None:
        """
        Register job with cancellation system.

        Args:
            context: Context dictionary containing job information
        """
        if self.job_tracking_id:
            self._job_state_manager.register_job(self.job_tracking_id, context)

    def unregister_job(self) -> None:
        """Remove the job from the cancellation registry (terminal state).

        ``JobStateManager`` only tracks ACTIVE jobs — completion and
        failure are both "stop tracking". The DatabaseDriver's success/
        failure paths call this via ``hasattr`` (which previously found
        nothing and silently leaked registry entries until the
        cleanup sweep).
        """
        if self.job_tracking_id:
            self._job_state_manager.unregister_job(self.job_tracking_id)

    def mark_completed(self) -> None:
        """Mark job as completed — drops it from the cancellation registry.

        The previous implementation called
        ``JobStateManager.mark_completed``, a method that has never
        existed; every invocation raised AttributeError.
        """
        self.unregister_job()

    def mark_failed(self, error: str) -> None:
        """
        Mark job as failed — drops it from the cancellation registry.

        Args:
            error: Error message describing the failure (kept for caller
                context/logging; the registry has no failure store).
        """
        self.unregister_job()

    def get_cancellation_context(self) -> dict:
        """
        Get context for job cancellation tracking.

        Override this method to provide specific cancellation context.

        Returns:
            dict: Context information for cancellation tracking
        """
        return {
            "job_class": self.__class__.__name__,
            "job_id": self.job_tracking_id,
        }

    def serialize(self) -> dict:
        """Serialize the job for storage."""
        return {
            **super().serialize(),
            "job_tracking_id": self.job_tracking_id,
            "queue": self.queue,
            "delay": self.delay,
            "connection": self.connection,
        }

    def unserialize(self, data: dict) -> None:
        """Unserialize the job from storage."""
        super().unserialize(data)
        self.job_tracking_id = data.get("job_tracking_id")
        self.queue = data.get("queue", "default")
        self.delay = data.get("delay")
        self.connection = data.get("connection")

    def __repr__(self):
        return f"<{self.__class__.__name__}>"

    @classmethod
    def dispatch(cls, *args, **kwargs) -> PendingDispatch:
        """
        Job dispatch with method chaining support.

        Returns PendingDispatch for chaining methods like on_queue(), delay(), etc.

        Usage:
            MyJob.dispatch(param1, param2).on_queue('high-priority').delay(30)
        """
        # Create job instance
        instance = cls(*args, **kwargs)

        # Return PendingDispatch for method chaining
        return PendingDispatch(instance)

    @classmethod
    def dispatch_after(cls, delay, *args, **kwargs):
        """Delayed job dispatch."""
        return cls.dispatch(*args, **kwargs).delay(delay)

    @classmethod
    async def dispatch_now(cls, *args, **kwargs):
        """Immediate job execution (bypasses queue)."""
        instance = cls(*args, **kwargs)
        if not hasattr(instance, "handle") or not callable(instance.handle):
            return None

        app = getattr(instance, "_app", None) or getattr(cls, "_app", None)
        if app is not None and hasattr(app, "call"):
            result = app.call(instance.handle)
        elif asyncio.iscoroutinefunction(instance.handle):
            result = await instance.handle()
        else:
            result = instance.handle()

        if asyncio.iscoroutine(result):
            result = await result
        return result

    def _safe_serialize(self) -> dict:
        """Safely serialize job data for database storage."""
        try:
            return self.serialize()
        except (TypeError, ValueError, AttributeError, RuntimeError):
            # Fallback to basic info if serialize fails
            return {
                "job_class": self.__class__.__name__,
                "job_id": getattr(self, "job_tracking_id", None),
            }
