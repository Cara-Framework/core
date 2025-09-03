"""
Cancellable Job Contract for Cara Framework.

Provides a generic interface for jobs that can be cancelled during execution.
This is framework-level functionality, not app-specific.
"""

from abc import ABC, abstractmethod
from typing import Optional


class CancellableJob(ABC):
    """
    Contract for jobs that support cancellation during execution.

    Framework-level abstraction for job lifecycle management.
    """

    def __init__(self, *args, **kwargs):
        """Initialize cancellable job with tracking ID."""
        super().__init__(*args, **kwargs)
        self.job_tracking_id: Optional[str] = None
        self.is_cancelled: bool = False

    def set_tracking_id(self, tracking_id: str) -> "CancellableJob":
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
        Default implementation always returns True.

        Returns:
            bool: True if job should continue, False if cancelled
        """
        return not self.is_cancelled

    def cancel(self, reason: str = "Job cancelled") -> None:
        """
        Cancel the job execution.

        Args:
            reason: Reason for cancellation
        """
        self.is_cancelled = True
        self.on_cancelled(reason)

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
            raise JobCancelledException(f"Job cancelled during {operation}")

    def on_cancelled(self, reason: str) -> None:
        """
        Handle job cancellation.

        Override this method to implement custom cancellation cleanup.

        Args:
            reason: Reason for cancellation
        """
        pass

    @abstractmethod
    def get_cancellation_context(self) -> dict:
        """
        Get context for cancellation checks.

        This method should return information needed to determine
        if the job should be cancelled (e.g., entity IDs, current state).

        Returns:
            dict: Context data for cancellation logic
        """
        pass


class JobCancelledException(Exception):
    """Exception raised when a job is cancelled during execution."""

    def __init__(
        self, message: str = "Job was cancelled", tracking_id: Optional[str] = None
    ):
        super().__init__(message)
        self.tracking_id = tracking_id
