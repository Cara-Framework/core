"""
Base class for queueable tasks in the Cara framework.

This module provides the foundation for creating background tasks with retry capabilities and
failure handling. Includes automatic serialization support and job cancellation.
"""

from __future__ import annotations

import asyncio
from typing import Any

from .SerializesModels import SerializesModels


class PendingDispatch:
    """
    Explicit queue-dispatch builder.

    Configure routing/delay options, then call ``dispatch()`` or ``send()``.
    Dropping the builder without a terminal call never queues work.
    """

    def __init__(self, job_instance):
        """Initialize with job instance."""
        self.job = job_instance
        self._queue_name = getattr(job_instance, "queue", None)
        self._delay = None
        self._connection = None
        self._routing_key = None
        self._job_id = None
        self._unique_key = None
        # Defer the push until the enclosing DB transaction commits —
        # set by .after_commit() or the ShouldDispatchAfterCommit marker.
        self._after_commit = False
        # Idempotency guard for repeated explicit terminal calls.
        self._dispatched = False
        self._dispatch_result = None

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
        """Select a canonical queue through local routing rules."""
        self._routing_key = routing_key
        return self

    def with_job_id(self, job_id: str) -> PendingDispatch:
        """Reserve the durable delivery UUID before queue registration."""
        if not isinstance(job_id, str) or not job_id:
            raise ValueError("job_id must be a non-empty string")
        self._job_id = job_id
        if hasattr(self.job, "set_tracking_id"):
            self.job.set_tracking_id(job_id)
        return self

    def with_unique_key(self, unique_key: str) -> PendingDispatch:
        """Carry the originating instance's uniqueness metadata to storage."""
        if not isinstance(unique_key, str) or not unique_key.strip():
            raise ValueError("unique_key must be a non-empty string")
        self._unique_key = unique_key.strip()
        return self

    def after_commit(self) -> PendingDispatch:
        """Defer the push until the enclosing DB transaction commits.

        See :class:`cara.queues.contracts.ShouldDispatchAfterCommit` for
        the full semantics (immediate when no transaction is open,
        discarded on rollback).
        """
        self._after_commit = True
        return self

    def dispatch(self):
        """
        Explicitly dispatch the configured job to the queue.

        IMPORTANT: NO FALLBACK - If queue dispatch fails, exception is raised.
        Repeated calls return the prior result without redispatching.

        After-commit deferral: when the job opts in (``.after_commit()``
        or the ShouldDispatchAfterCommit marker) and a DB transaction is
        open in this context, the actual push is registered on the
        transaction and fires right after the OUTERMOST commit — or is
        discarded entirely on rollback. With no transaction open the push
        happens immediately.
        """
        if getattr(self, "_dispatched", False):
            return self._dispatch_result

        from .ShouldDispatchAfterCommit import ShouldDispatchAfterCommit

        if self._after_commit or isinstance(self.job, ShouldDispatchAfterCommit):
            from cara.eloquent import DatabaseManager
            from cara.facades import Queue

            driver = Queue.driver(self._connection)
            if getattr(driver, "durable_transactional_outbox", False):
                # The AMQP driver must register its delivery ledger row INSIDE
                # the domain transaction. It defers only broker publication.
                # Deferring the entire push creates a commit→ledger crash gap.
                job_id = self._push()
                self._dispatch_result = job_id
                self._dispatched = True
                return job_id

            # Mark dispatched before callback registration so repeated terminal
            # calls cannot double-register the same after-commit callback.
            self._dispatched = True
            self._dispatch_result = self._job_id

            def _push_after_commit() -> None:
                pushed_job_id = self._push()
                if self._job_id is not None and str(pushed_job_id) != self._job_id:
                    raise RuntimeError(
                        "Queue driver did not honor the reserved delivery UUID."
                    )
                self._dispatch_result = pushed_job_id

            DatabaseManager.get_instance().after_commit(_push_after_commit)
            # Immediate mode (no open transaction) ran the push
            # synchronously — hand back the job id. Deferred mode has
            # nothing to return yet.
            return self._dispatch_result

        job_id = self._push()
        self._dispatch_result = job_id
        self._dispatched = True
        return job_id

    def send(self):
        """Alias for :meth:`dispatch` when it reads better as a terminal call."""
        return self.dispatch()

    def _push(self):
        """Perform the actual broker push (no idempotency guard — the
        caller manages ``_dispatched``)."""
        if self._routing_key:
            return self._dispatch_via_router()

        # Standard queue dispatch
        from cara.facades import Queue

        # Set final queue properties
        if hasattr(self.job, "queue"):
            self.job.queue = self._queue_name

        if self._delay and hasattr(self.job, "delay"):
            self.job.delay = self._delay

        # Push to queue (will raise exception if fails). A requested delay
        # MUST route through Queue.later. For AMQP this commits a signed
        # delayed-job outbox row; Queue.push would fire immediately and
        # silently drop the requested clock.
        options = {"job_id": self._job_id}
        if self._unique_key is not None:
            options["unique_key"] = self._unique_key
        if self._delay:
            job_id = Queue.later(self._delay, self.job, **options)
        else:
            job_id = Queue.push(self.job, **options)

        # Set tracking ID
        if hasattr(self.job, "set_tracking_id"):
            self.job.set_tracking_id(str(job_id))

        return job_id

    def _dispatch_via_router(self):
        """
        Resolve a routing key locally before durable queue registration.

        NO FALLBACK - An invalid or ambiguous rule raises before persistence.
        """
        from cara.queues.routing import QueueRouter

        router = QueueRouter()

        # Set job properties
        if self._delay and hasattr(self.job, "delay"):
            self.job.delay = self._delay

        job_id = router.dispatch_job(
            routing_key=self._routing_key,
            job_instance=self.job,
            delay=self._delay,
            job_id=self._job_id,
            unique_key=self._unique_key,
        )

        # Set tracking ID
        if hasattr(self.job, "set_tracking_id"):
            self.job.set_tracking_id(str(job_id))

        return job_id


class Queueable(SerializesModels):
    """
    Makes classes Queueable with Laravel-style dispatch.

    The Queueable class is responsible for handling background tasks.
    Includes automatic serialization, cancellation support, and universal job tracking.
    """

    run_again_on_fail = True
    run_times = 3

    def __init__(self, *args, **kwargs):
        """Initialize queueable job."""
        super().__init__()
        self.job_tracking_id: str | None = None
        self._job_tracker: Any | None = None  # Lazy-loaded from container
        self._db_job_id: int | None = None  # Database job ID for unified tracking

        # Laravel-style properties
        self.queue = None
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

        Returns a builder; callers must finish with ``.dispatch()`` or ``.send()``.

        Usage:
            MyJob.dispatch(param1, param2).on_queue("high-priority").delay(30).send()
        """
        # Create job instance
        instance = cls(*args, **kwargs)

        # Return PendingDispatch for method chaining
        return PendingDispatch(instance)

    @classmethod
    def dispatch_after(cls, delay, *args, **kwargs):
        """Explicitly dispatch a delayed job."""
        return cls.dispatch(*args, **kwargs).delay(delay).send()

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
        except TypeError, ValueError, AttributeError, RuntimeError:
            # Fallback to basic info if serialize fails
            return {
                "job_class": self.__class__.__name__,
                "job_id": getattr(self, "job_tracking_id", None),
            }
