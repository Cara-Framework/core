"""
Job State Manager for Cara Framework.

Generic job state tracking and cancellation management.
Framework-level component, independent of application logic.
"""

import threading
from datetime import datetime
from typing import Any, Callable, Dict, Optional


class JobStateManager:
    """
    Thread-safe job state manager for tracking active jobs and handling cancellation.

    Framework-level service for job lifecycle management.
    """

    def __init__(self):
        """Initialize job state manager."""
        self._active_jobs: Dict[str, Dict[str, Any]] = {}
        self._cancellation_callbacks: Dict[str, Callable] = {}
        self._lock = threading.RLock()

    def register_job(
        self,
        job_id: str,
        context: Dict[str, Any],
        cancellation_callback: Optional[Callable] = None,
    ) -> None:
        """
        Register an active job for tracking.

        Args:
            job_id: Unique job identifier
            context: Job context for cancellation logic
            cancellation_callback: Optional callback for custom cancellation logic
        """
        with self._lock:
            self._active_jobs[job_id] = {
                "context": context,
                "started_at": datetime.now().isoformat(),
                "status": "running",
            }

            if cancellation_callback:
                self._cancellation_callbacks[job_id] = cancellation_callback

    def unregister_job(self, job_id: str) -> None:
        """
        Unregister a job (completed or failed).

        Args:
            job_id: Job identifier to unregister
        """
        with self._lock:
            self._active_jobs.pop(job_id, None)
            self._cancellation_callbacks.pop(job_id, None)

    def is_job_cancelled(self, job_id: str) -> bool:
        """
        Check if a job has been cancelled.

        Args:
            job_id: Job identifier to check

        Returns:
            bool: True if job is cancelled, False otherwise
        """
        with self._lock:
            job_state = self._active_jobs.get(job_id)
            return job_state and job_state.get("status") == "cancelled"

    def cancel_job(self, job_id: str, reason: str = "Job cancelled") -> bool:
        """
        Cancel a specific job.

        Args:
            job_id: Job identifier to cancel
            reason: Reason for cancellation

        Returns:
            bool: True if job was cancelled, False if not found
        """
        with self._lock:
            if job_id not in self._active_jobs:
                return False

            # Mark as cancelled
            self._active_jobs[job_id]["status"] = "cancelled"
            self._active_jobs[job_id]["cancelled_at"] = datetime.now().isoformat()
            self._active_jobs[job_id]["cancel_reason"] = reason

            # Execute cancellation callback if provided
            callback = self._cancellation_callbacks.get(job_id)
            if callback:
                try:
                    callback(job_id, reason)
                except Exception:
                    # Don't let callback errors prevent cancellation
                    pass

            return True

    def cancel_jobs_by_context(
        self,
        context_filter: Callable[[Dict[str, Any]], bool],
        reason: str = "Job superseded",
    ) -> int:
        """
        Cancel jobs based on context filter.

        Args:
            context_filter: Function that returns True for jobs to cancel
            reason: Reason for cancellation

        Returns:
            int: Number of jobs cancelled
        """
        cancelled_count = 0

        with self._lock:
            jobs_to_cancel = []

            for job_id, job_data in self._active_jobs.items():
                if job_data.get("status") == "running" and context_filter(
                    job_data["context"]
                ):
                    jobs_to_cancel.append(job_id)

            for job_id in jobs_to_cancel:
                if self.cancel_job(job_id, reason):
                    cancelled_count += 1

        return cancelled_count

    def get_active_jobs(self) -> Dict[str, Dict[str, Any]]:
        """
        Get all active jobs.

        Returns:
            dict: Active jobs with their states
        """
        with self._lock:
            return {
                job_id: job_data.copy()
                for job_id, job_data in self._active_jobs.items()
                if job_data.get("status") == "running"
            }

    def get_job_status(self, job_id: str) -> Optional[str]:
        """
        Get status of a specific job.

        Args:
            job_id: Job identifier

        Returns:
            str: Job status or None if not found
        """
        with self._lock:
            job_data = self._active_jobs.get(job_id)
            return job_data.get("status") if job_data else None

    def cleanup_old_jobs(self, max_age_hours: int = 24) -> int:
        """
        Clean up old job records.

        Args:
            max_age_hours: Maximum age in hours for job records

        Returns:
            int: Number of jobs cleaned up
        """
        from datetime import timedelta

        cutoff = datetime.now() - timedelta(hours=max_age_hours)
        cutoff_iso = cutoff.isoformat()

        cleaned_count = 0

        with self._lock:
            jobs_to_remove = []

            for job_id, job_data in self._active_jobs.items():
                started_at = job_data.get("started_at", "")
                if started_at < cutoff_iso:
                    jobs_to_remove.append(job_id)

            for job_id in jobs_to_remove:
                self.unregister_job(job_id)
                cleaned_count += 1

        return cleaned_count


# Global instance for framework use
_job_state_manager = JobStateManager()


def get_job_state_manager() -> JobStateManager:
    """Get the global job state manager instance."""
    return _job_state_manager
