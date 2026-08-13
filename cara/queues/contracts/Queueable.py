"""
Base class for queueable tasks in the Cara framework.

This module provides the foundation for creating background tasks with retry capabilities and
failure handling. Includes automatic serialization support and job cancellation.
"""

from __future__ import annotations

import asyncio
from typing import Any

from .PendingDispatch import PendingDispatch
from .SerializesModels import SerializesModels


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

        Returns a builder; callers must finish with ``.dispatch()``.

        Usage:
            MyJob.dispatch(param1, param2).on_queue("high-priority").delay(30).dispatch()
        """
        # Create job instance
        instance = cls(*args, **kwargs)

        # Return PendingDispatch for method chaining
        return PendingDispatch(instance)

    @classmethod
    def dispatch_after(cls, delay, *args, **kwargs):
        """Explicitly dispatch a delayed job."""
        return cls.dispatch(*args, **kwargs).delay(delay).dispatch()

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
