"""
Cancellable Job Contract for Cara Framework.

Provides a generic interface for jobs that can be cancelled during execution.
This is framework-level functionality, not app-specific.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from cara.exceptions.types.queue import QueueException


class CancellableJob(ABC):
    """
    Contract for jobs that support cancellation during execution.

    Framework-level abstraction for job lifecycle management.
    """

    def __init__(self, *args, **kwargs):
        """Initialize cancellable job with tracking ID."""
        super().__init__()  # object.__init__() takes no arguments
        self.job_tracking_id: str | None = None
        self.is_cancelled: bool = False

    def set_tracking_id(self, tracking_id: str) -> CancellableJob:
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


class JobCancelledException(QueueException):
    """Exception raised when a job is cancelled during execution.

    ``do_not_retry`` is the load-bearing signal, read by
    ``JobProcessor._route_failed_message``. A cancellation is a DECISION, not
    a fault: retrying it re-runs work an operator deliberately stopped.
    Pre-fix this exception declared nothing, so a cancelled job burned the
    full 1s/5s/30s retry schedule — re-executing the cancelled work twice
    more — before dead-lettering with ``Log.error`` tracebacks that read like
    a crash.
    """

    do_not_retry: bool = True

    def __init__(
        self, message: str = "Job was cancelled", tracking_id: str | None = None
    ):
        super().__init__(message)
        self.tracking_id = tracking_id


class JobThrottledException(QueueException):
    """Raised when middleware skips job execution due to throttling.

    ``is_throttle`` opts this exception into the worker's STARVATION budget
    (``throttle_attempts`` / ``max_throttle_attempts``) instead of the FAILURE
    budget (``attempts`` / ``max_attempts``) — the lane
    ``JobProcessor._requeue_with_delay`` documents as the one "future throttle
    classes opt in for free".

    Cara's own throttle exception never opted in. A throttled job therefore
    spent the failure budget: three attempts at 1s/5s/30s, all of them inside
    a gate whose default ``retry_after`` is 300s, so a perfectly healthy job
    dead-lettered roughly six seconds after it was first throttled.
    ``ConcurrencyExceeded`` (``cara/queues/middleware/ConcurrencyLimited.py``)
    has always declared this; the two throttle signals now settle the same way.
    """

    is_throttle: bool = True

    def __init__(
        self,
        message: str = "Job was throttled",
        key: str | None = None,
        retry_after: int | None = None,
    ):
        super().__init__(message)
        self.key = key
        self.retry_after = retry_after
