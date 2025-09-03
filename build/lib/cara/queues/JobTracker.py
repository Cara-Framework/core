"""
Universal Job Tracker for Cara Framework.

Laravel-style database tracking for all jobs, independent of queue driver.
Uses proper Model pattern like Laravel does.
"""

from typing import Any, Dict, Optional

import pendulum

from cara.configuration import config


class JobTracker:
    """
    Universal job tracker using proper Model pattern.

    Only active for non-database drivers (AMQP, Redis) to avoid duplication
    with DatabaseDriver's built-in tracking.
    """

    def __init__(self):
        self.enabled = config("queue.TRACK_JOBS_IN_DATABASE", True)
        self.track_failed = config("queue.TRACK_FAILED_JOBS", True)

        # Auto-disable for database driver to avoid duplication
        current_driver = config("queue.DEFAULT", "amqp")
        if current_driver == "database":
            self.enabled = False

    def is_enabled(self) -> bool:
        """Check if job tracking is enabled."""
        return self.enabled

    def track_job_started(self, job_id: str, job_data: Dict[str, Any]) -> Optional[str]:
        """Track when a job starts processing."""
        if not self.enabled:
            return None

        try:
            # Use the same pattern as everywhere else in Cara
            from cara.eloquent.models import Model

            # Create a dynamic model for job table (Laravel-style)
            class JobModel(Model):
                __table__ = "job"
                __fillable__ = [
                    "name",
                    "queue",
                    "payload",
                    "status",
                    "job_class",
                    "metadata",
                    "attempts",
                    "started_at",
                    "available_at",
                    "created_at",
                    "updated_at",
                ]

            job_record = JobModel.create(
                {
                    "name": job_data.get("job_class", "Unknown"),
                    "queue": job_data.get("queue", "default"),
                    "payload": self._serialize_payload(job_data.get("payload", {})),
                    "status": "processing",
                    "job_class": job_data.get("job_class"),
                    "metadata": self._serialize_payload(
                        {
                            "job_id": job_id,
                            "context": job_data.get("context", {}),
                            "driver": job_data.get("driver", "tracked"),
                        }
                    ),
                    "attempts": job_data.get("attempts", 0),
                    "started_at": pendulum.now().to_datetime_string(),
                    "available_at": pendulum.now().to_datetime_string(),
                }
            )

            return str(job_record.id) if job_record else None

        except Exception as e:
            from cara.facades import Log

            Log.warning(f"Failed to track job start: {e}")
            return None

    def track_job_completed(
        self, job_id: str, db_record_id: Optional[str] = None
    ) -> None:
        """Track when a job completes successfully."""
        if not self.enabled:
            return

        try:
            from cara.eloquent.models import Model

            class JobModel(Model):
                __table__ = "job"

            if db_record_id:
                job = JobModel.find(db_record_id)
                if job:
                    job.update(
                        {
                            "status": "completed",
                            "completed_at": pendulum.now().to_datetime_string(),
                        }
                    )

        except Exception as e:
            from cara.facades import Log

            Log.warning(f"Failed to track job completion: {e}")

    def track_job_failed(
        self, job_id: str, error: str, db_record_id: Optional[str] = None
    ) -> None:
        """Track when a job fails."""
        if not self.enabled:
            return

        try:
            from cara.eloquent.models import Model

            class JobModel(Model):
                __table__ = "job"

            if db_record_id:
                job = JobModel.find(db_record_id)
                if job:
                    job.update(
                        {
                            "status": "failed",
                        }
                    )

            if self.track_failed:
                self._track_in_failed_jobs(job_id, error, db_record_id)

        except Exception as e:
            from cara.facades import Log

            Log.warning(f"Failed to track job failure: {e}")

    def track_job_cancelled(
        self, job_id: str, reason: str, db_record_id: Optional[str] = None
    ) -> None:
        """Track when a job is cancelled."""
        if not self.enabled:
            return

        try:
            from cara.eloquent.models import Model

            class JobModel(Model):
                __table__ = "job"

            if db_record_id:
                job = JobModel.find(db_record_id)
                if job:
                    job.update(
                        {
                            "status": "cancelled",
                            "cancelled_at": pendulum.now().to_datetime_string(),
                        }
                    )

        except Exception as e:
            from cara.facades import Log

            Log.warning(f"Failed to track job cancellation: {e}")

    def _track_in_failed_jobs(
        self, job_id: str, error: str, db_record_id: Optional[str] = None
    ) -> None:
        """Track failed job in separate failed_job table."""
        try:
            from cara.eloquent.models import Model

            class FailedJobModel(Model):
                __table__ = "failed_job"
                __fillable__ = [
                    "driver",
                    "queue",
                    "name",
                    "connection",
                    "payload",
                    "exception",
                    "failed_at",
                ]

            FailedJobModel.create(
                {
                    "driver": "tracked",
                    "queue": "default",
                    "name": "Unknown",
                    "connection": "default",
                    "payload": "{}",
                    "exception": self._serialize_payload(
                        {"message": error, "job_id": job_id}
                    ),
                    "failed_at": pendulum.now().to_datetime_string(),
                }
            )

        except Exception as e:
            from cara.facades import Log

            Log.warning(f"Failed to track in failed_job table: {e}")

    def _serialize_payload(self, data: Any) -> str:
        """Serialize data to JSON string."""
        import json

        try:
            return json.dumps(data, default=str)
        except Exception:
            return "{}"

    def get_job_stats(self, queue: str = "default") -> Dict[str, int]:
        """Get job statistics from tracked data."""
        if not self.enabled:
            return {}

        try:
            from cara.eloquent.models import Model

            class JobModel(Model):
                __table__ = "job"

            stats = {}
            for status in ["pending", "processing", "completed", "cancelled", "failed"]:
                count = JobModel.where("queue", queue).where("status", status).count()
                stats[f"{status}_jobs"] = count

            stats["total_jobs"] = JobModel.where("queue", queue).count()
            return stats

        except Exception:
            return {}


# Global instance
_job_tracker = JobTracker()


def get_job_tracker() -> JobTracker:
    """Get the global job tracker instance."""
    return _job_tracker
