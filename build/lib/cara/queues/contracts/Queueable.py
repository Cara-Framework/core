"""
Base class for queueable tasks in the Cara framework.

This module provides the foundation for creating background tasks with retry capabilities and
failure handling. Includes automatic serialization support and job cancellation.
"""

from typing import Optional

from cara.queues.JobStateManager import get_job_state_manager
from cara.queues.JobTracker import get_job_tracker

from .CancellableJob import CancellableJob, JobCancelledException
from .SerializesModels import SerializesModels


class Queueable(SerializesModels, CancellableJob):
    """
    Makes classes Queueable.

    The Queueable class is responsible for handling background tasks.
    Includes automatic serialization, cancellation support, and universal job tracking.
    """

    run_again_on_fail = True
    run_times = 3

    def __init__(self, *args, **kwargs):
        """Initialize queueable job."""
        super().__init__(*args, **kwargs)
        self.job_tracking_id: Optional[str] = None
        self._job_state_manager = get_job_state_manager()
        self._job_tracker = get_job_tracker()
        self._db_record_id: Optional[str] = None  # Database tracking record ID

    def set_tracking_id(self, tracking_id: str) -> "Queueable":
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
        Check for cancellation and raise exception if cancelled.

        Call this at checkpoints in long-running operations.

        Args:
            operation: Description of current operation

        Raises:
            JobCancelledException: If job has been cancelled
        """
        if not self.should_continue():
            raise JobCancelledException(
                f"Job cancelled during {operation}", tracking_id=self.job_tracking_id
            )

    def register_job(self, context: dict) -> None:
        """Register job with state manager and database tracker."""
        if self.job_tracking_id:
            # Register with in-memory state manager
            self._job_state_manager.register_job(self.job_tracking_id, context)

            # Track in database if enabled
            if self._job_tracker.is_enabled():
                from cara.facades import Log

                Log.info(f"🎯 JobTracker enabled - tracking job: {self.job_tracking_id}")

                job_data = {
                    "job_class": self.__class__.__name__,
                    "queue": "default",  # Could be extracted from context
                    "context": context,
                    "payload": self._safe_serialize(),
                    "driver": "tracked",
                    "attempts": 0,
                }
                self._db_record_id = self._job_tracker.track_job_started(
                    self.job_tracking_id, job_data
                )
                Log.info(f"🎯 Job tracked with DB record ID: {self._db_record_id}")
            else:
                from cara.facades import Log

                Log.info(
                    f"⚠️ JobTracker disabled - not tracking job: {self.job_tracking_id}"
                )

    def unregister_job(self) -> None:
        """Unregister job from state manager."""
        if self.job_tracking_id:
            self._job_state_manager.unregister_job(self.job_tracking_id)

    def handle(self):
        """Override this method with the job's processing logic."""
        pass

    def failed(self, obj, e):
        """Handle job failure - enhanced with database tracking."""
        # Track failure in database
        if self._job_tracker.is_enabled() and self.job_tracking_id:
            self._job_tracker.track_job_failed(
                self.job_tracking_id, str(e), self._db_record_id
            )

        self.unregister_job()

    def on_job_complete(self):
        """Handle job completion - enhanced with database tracking."""
        # Track completion in database
        if self._job_tracker.is_enabled() and self.job_tracking_id:
            self._job_tracker.track_job_completed(
                self.job_tracking_id, self._db_record_id
            )

        self.unregister_job()

    def on_cancelled(self, reason: str) -> None:
        """Handle job cancellation - enhanced with database tracking."""
        # Track cancellation in database
        if self._job_tracker.is_enabled() and self.job_tracking_id:
            self._job_tracker.track_job_cancelled(
                self.job_tracking_id, reason, self._db_record_id
            )

    def get_cancellation_context(self) -> dict:
        """
        Get context for cancellation checks.

        Default implementation returns empty context.
        Override in subclasses to provide specific context.

        Returns:
            dict: Context data for cancellation logic
        """
        return {}

    def display_name(self) -> str:
        """
        Get the display name for the job.

        Returns:
            Human-readable job name
        """
        return self.__class__.__name__

    def __repr__(self):
        return f"<{self.__class__.__name__}>"

    @classmethod
    def dispatch(cls, *args, **kwargs):
        """Laravel-style job dispatch - returns job ID for tracking."""
        from cara.facades import Queue

        return Queue.dispatch(cls, *args, **kwargs)

    @classmethod
    def dispatchAfter(cls, delay, *args, **kwargs):
        """Laravel-style delayed job dispatch."""
        from cara.facades import Queue

        return Queue.dispatchAfter(cls, delay, *args, **kwargs)

    @classmethod
    def dispatchNow(cls, *args, **kwargs):
        """Laravel-style immediate job execution."""
        from cara.facades import Queue

        return Queue.dispatchNow(cls, *args, **kwargs)

    def _safe_serialize(self) -> dict:
        """Safely serialize job data for database storage."""
        try:
            return self.serialize()
        except Exception:
            # Fallback to basic info if serialize fails
            return {
                "job_class": self.__class__.__name__,
                "job_id": getattr(self, "job_tracking_id", None),
            }
