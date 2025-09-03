"""
Scheduling Interface for the Cara framework.

This module defines the contract for scheduling drivers, specifying the required methods for job
scheduling implementations.
"""

from typing import Any, Dict, Iterable


class Scheduling:
    """Interface that any scheduling driver must implement."""

    def schedule_job(
        self,
        identifier: str,
        callback: Any,
        schedule_spec: Dict[str, Any],
        options: Dict[str, Any],
    ) -> None:
        """
        Register a job according to schedule_spec.

        - identifier: unique name for this scheduled job.
        - callback: a callable to execute when triggered.
        - schedule_spec: a dict describing the schedule, for example:
            {"type": "cron", "expression": "0 3 * * *", "timezone": <tz>}
            {"type": "daily", "hour": 3, "minute": 0, "timezone": <tz>}
            {"type": "hourly", "minute": 30, "timezone": <tz>}
            {"type": "interval", "seconds": 0, "minutes": 5, "hours": 0}
            {"type": "at", "run_date": <datetime or parseable string>}
        - options: additional options; driver-specific. For APSchedulerDriver may include:
            {"apscheduler_job_options": {...}}
        """

    def start(self) -> None:
        """Start the scheduling engine if needed (e.g. APScheduler.start())."""

    def shutdown(self, wait: bool = True) -> None:
        """Shutdown scheduling engine gracefully."""

    def remove_job(self, identifier: str) -> None:
        """Remove a scheduled job by its identifier."""

    def list_jobs(self) -> Iterable[Any]:
        """Return a list or iterable of scheduled job metadata/objects."""
