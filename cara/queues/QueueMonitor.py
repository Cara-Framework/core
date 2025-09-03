"""
Laravel-style Queue Monitor for Cara Framework.

This module provides monitoring capabilities for queue jobs,
including job status tracking, performance metrics, and failure handling.
"""

import json
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from cara.facades import Log


class QueueMonitor:
    """
    Queue monitoring service for tracking job performance and status.

    Provides Laravel-style monitoring features:
    - Job timing and performance metrics
    - Success/failure tracking
    - Queue size monitoring
    - Failed job analysis
    """

    def __init__(self):
        """Initialize queue monitor."""
        self.job_stats: Dict[str, Dict[str, Any]] = {}
        self.queue_stats: Dict[str, Dict[str, Any]] = {}
        self.start_time = time.time()

    def job_started(self, job, queue_name: str = "default") -> str:
        """
        Track when a job starts processing.

        Args:
            job: Job instance
            queue_name: Queue name

        Returns:
            Job tracking ID
        """
        job_id = f"{job.__class__.__name__}_{int(time.time() * 1000)}"
        job_name = getattr(job, "display_name", lambda: job.__class__.__name__)()

        self.job_stats[job_id] = {
            "job_name": job_name,
            "job_class": job.__class__.__name__,
            "queue": queue_name,
            "status": "processing",
            "started_at": datetime.now(),
            "attempts": getattr(job, "attempts", 1),
            "payload_size": len(str(job.__dict__)),
        }

        # Update queue stats
        if queue_name not in self.queue_stats:
            self.queue_stats[queue_name] = {
                "jobs_processed": 0,
                "jobs_failed": 0,
                "total_time": 0.0,
                "avg_time": 0.0,
                "last_job": None,
            }

        Log.info(f"[QueueMonitor] Job started: {job_name} on queue '{queue_name}'")
        return job_id

    def job_completed(
        self, job_id: str, success: bool = True, error: Optional[Exception] = None
    ):
        """
        Track when a job completes (success or failure).

        Args:
            job_id: Job tracking ID
            success: Whether job completed successfully
            error: Exception if job failed
        """
        if job_id not in self.job_stats:
            return

        job_stat = self.job_stats[job_id]
        job_stat["completed_at"] = datetime.now()
        job_stat["duration"] = (
            job_stat["completed_at"] - job_stat["started_at"]
        ).total_seconds()
        job_stat["status"] = "completed" if success else "failed"

        if error:
            job_stat["error"] = str(error)
            job_stat["error_type"] = error.__class__.__name__

        # Update queue stats
        queue_name = job_stat["queue"]
        queue_stat = self.queue_stats[queue_name]
        queue_stat["jobs_processed"] += 1
        queue_stat["total_time"] += job_stat["duration"]
        queue_stat["avg_time"] = queue_stat["total_time"] / queue_stat["jobs_processed"]
        queue_stat["last_job"] = job_stat["job_name"]

        if not success:
            queue_stat["jobs_failed"] += 1

        status_emoji = "✅" if success else "❌"
        Log.info(
            f"[QueueMonitor] {status_emoji} Job {job_stat['status']}: {job_stat['job_name']} ({job_stat['duration']:.2f}s)"
        )

        # Clean up old job stats (keep last 1000)
        if len(self.job_stats) > 1000:
            oldest_jobs = sorted(self.job_stats.keys())[:100]
            for old_job_id in oldest_jobs:
                del self.job_stats[old_job_id]

    def get_queue_stats(self, queue_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Get queue statistics.

        Args:
            queue_name: Specific queue name, or None for all queues

        Returns:
            Queue statistics
        """
        if queue_name:
            return self.queue_stats.get(queue_name, {})
        return self.queue_stats

    def get_job_stats(self, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Get recent job statistics.

        Args:
            limit: Maximum number of jobs to return

        Returns:
            List of job statistics
        """
        recent_jobs = sorted(
            self.job_stats.values(), key=lambda x: x["started_at"], reverse=True
        )[:limit]

        # Convert datetime objects to strings for JSON serialization
        for job in recent_jobs:
            job["started_at"] = job["started_at"].isoformat()
            if "completed_at" in job:
                job["completed_at"] = job["completed_at"].isoformat()

        return recent_jobs

    def get_failed_jobs(self, limit: int = 20) -> List[Dict[str, Any]]:
        """
        Get recent failed jobs.

        Args:
            limit: Maximum number of failed jobs to return

        Returns:
            List of failed job statistics
        """
        failed_jobs = [
            job for job in self.job_stats.values() if job["status"] == "failed"
        ]

        recent_failed = sorted(failed_jobs, key=lambda x: x["started_at"], reverse=True)[
            :limit
        ]

        # Convert datetime objects to strings
        for job in recent_failed:
            job["started_at"] = job["started_at"].isoformat()
            if "completed_at" in job:
                job["completed_at"] = job["completed_at"].isoformat()

        return recent_failed

    def get_performance_summary(self) -> Dict[str, Any]:
        """
        Get overall performance summary.

        Returns:
            Performance summary statistics
        """
        total_jobs = len(self.job_stats)
        failed_jobs = len([j for j in self.job_stats.values() if j["status"] == "failed"])

        # Calculate success rate
        success_rate = (
            ((total_jobs - failed_jobs) / total_jobs * 100) if total_jobs > 0 else 0
        )

        # Get average processing time
        completed_jobs = [j for j in self.job_stats.values() if "duration" in j]
        avg_processing_time = (
            sum(j["duration"] for j in completed_jobs) / len(completed_jobs)
            if completed_jobs
            else 0
        )

        uptime = time.time() - self.start_time

        return {
            "uptime_seconds": uptime,
            "total_jobs_processed": total_jobs,
            "failed_jobs": failed_jobs,
            "success_rate_percent": round(success_rate, 2),
            "average_processing_time_seconds": round(avg_processing_time, 3),
            "queues_active": len(self.queue_stats),
            "queue_stats": self.queue_stats,
            "timestamp": datetime.now().isoformat(),
        }

    def export_stats(self, format: str = "json") -> str:
        """
        Export statistics in specified format.

        Args:
            format: Export format (json, csv)

        Returns:
            Exported statistics string
        """
        if format.lower() == "json":
            return json.dumps(self.get_performance_summary(), indent=2)
        else:
            raise ValueError(f"Unsupported export format: {format}")

    def clear_stats(self):
        """Clear all statistics."""
        self.job_stats.clear()
        self.queue_stats.clear()
        self.start_time = time.time()
        Log.info("[QueueMonitor] Statistics cleared")


# Global monitor instance
_monitor_instance = None


def get_monitor() -> QueueMonitor:
    """Get global queue monitor instance."""
    global _monitor_instance
    if _monitor_instance is None:
        _monitor_instance = QueueMonitor()
    return _monitor_instance
