"""
Laravel-style Queue Monitor for Cara Framework.

This module provides monitoring capabilities for queue jobs,
including job status tracking, performance metrics, and failure handling.
"""

from __future__ import annotations

import json
import time
from datetime import datetime
from typing import Any

import pendulum

from cara.exceptions import InvalidArgumentException

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
        self.job_stats: dict[str, dict[str, Any]] = {}
        self.queue_stats: dict[str, dict[str, Any]] = {}
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
            "started_at": pendulum.now("UTC"),
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

        Log.debug(f"[QueueMonitor] Job started: {job_name} on queue '{queue_name}'")
        return job_id

    def job_completed(
        self, job_id: str, success: bool = True, error: Exception | None = None
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
        job_stat["completed_at"] = pendulum.now("UTC")
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
        Log.debug(
            f"[QueueMonitor] {status_emoji} Job {job_stat['status']}: {job_stat['job_name']} ({job_stat['duration']:.2f}s)"
        )

        # Clean up old job stats (keep last 1000).
        # ROOT-CAUSE: the previous eviction sorted by dict KEY (which is
        # ``f"{class_name}_{ms_ts}"``), so alphabetisation buckets by
        # class name FIRST and only orders by timestamp WITHIN a class.
        # A high-volume A-named class (``AggregatePricesJob``, 10/s) kept
        # displacing rare Z-named rows (``WishlistPriceDropSweepJob``,
        # 1/min) from the visible history regardless of actual age —
        # the rare jobs effectively vanished from monitoring. Sorting by
        # ``started_at`` makes the eviction genuinely time-ordered.
        if len(self.job_stats) > 1000:
            oldest_first = sorted(
                self.job_stats.items(),
                key=lambda kv: kv[1].get("started_at") or datetime.min,
            )
            for old_job_id, _ in oldest_first[:100]:
                del self.job_stats[old_job_id]

    def get_queue_stats(self, queue_name: str | None = None) -> dict[str, Any]:
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

    def get_job_stats(self, limit: int = 50) -> list[dict[str, Any]]:
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

    def get_stuck_jobs(self, threshold_seconds: int = 300) -> list[dict[str, Any]]:
        """Return every job still in ``processing`` past the threshold.

        A "stuck" job is one whose lifecycle ended without a
        ``job_completed`` call — typically because the worker crashed
        mid-execution (segfault, OOM, ``kill -9``), got cancelled
        externally, or the consumer ack'd without firing the monitor
        hook. The row sits in this monitor's ``job_stats`` as
        ``status="processing"`` indefinitely (until the eviction cap
        clears it ~1000 jobs later), and the framework has no native
        watchdog for "still running too long".

        ``threshold_seconds`` is the caller's contract for "how long
        is too long" — a typical value is 5×p99 latency for the
        slowest healthy job in the queue. Use ``0`` to dump every
        in-flight job (debug / incident response).

        Returns
        -------
        Oldest-first list of stat dicts enriched with
        ``elapsed_seconds`` — the single most useful field on a
        stuck-job alert payload (avoids forcing every caller to
        recompute against ``started_at``).
        """
        now = pendulum.now("UTC")
        stuck: list[tuple[float, dict[str, Any]]] = []
        for stat in self.job_stats.values():
            if stat.get("status") != "processing":
                continue
            started = stat.get("started_at")
            if not isinstance(started, datetime):
                # Defensive: ``job_started`` writes ``pendulum.now("UTC")`` but
                # ``get_job_stats`` mutates entries to ISO strings on the
                # return path. Skip anything we can't compare numerically.
                continue
            elapsed = (now - started).total_seconds()
            if elapsed < threshold_seconds:
                continue
            enriched = dict(stat)
            enriched["elapsed_seconds"] = round(elapsed, 3)
            enriched["started_at"] = started.isoformat()
            stuck.append((elapsed, enriched))

        # Oldest first — ops should see the worst offender at the top
        # of the alert payload.
        stuck.sort(key=lambda kv: -kv[0])
        return [s for _, s in stuck]

    def get_failed_jobs(self, limit: int = 20) -> list[dict[str, Any]]:
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

    def get_performance_summary(self) -> dict[str, Any]:
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
            "timestamp": pendulum.now("UTC").isoformat(),
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
            raise InvalidArgumentException(f"Unsupported export format: {format}")

    def health_check(self) -> dict[str, Any]:
        """
        Get overall queue system health status.

        Evaluates failure rates, wait times, and queue congestion to determine health.

        Returns:
            Dict with status (healthy/degraded/critical), issues, and metrics
        """
        stats = self.get_performance_summary()

        status = "healthy"
        issues = []

        # Check failure rate
        total = stats.get("total_jobs_processed", 0)
        failed = stats.get("failed_jobs", 0)
        failed_rate = (failed / max(total, 1)) if total > 0 else 0

        if failed_rate > 0.3:  # >30% failure rate
            status = "critical"
            issues.append(f"Critical failure rate: {failed_rate:.1%}")
        elif failed_rate > 0.1:  # >10% failure rate
            status = "degraded"
            issues.append(f"High failure rate: {failed_rate:.1%}")

        # Check average processing time
        avg_wait = stats.get("average_processing_time_seconds", 0)
        if avg_wait > 300:  # >5 min avg processing time
            if status == "healthy":
                status = "degraded"
            issues.append(f"High processing time: {avg_wait:.0f}s")

        # Check queue backlog
        queue_stats = stats.get("queue_stats", {})
        for queue_name, q_stat in queue_stats.items():
            jobs_processed = q_stat.get("jobs_processed", 0)
            jobs_failed = q_stat.get("jobs_failed", 0)

            if jobs_processed > 0:
                queue_failure_rate = jobs_failed / jobs_processed
                if queue_failure_rate > 0.2:
                    if status == "healthy":
                        status = "degraded"
                    issues.append(
                        f"Queue '{queue_name}' failure rate: {queue_failure_rate:.1%}"
                    )

        return {
            "status": status,
            "issues": issues,
            "metrics": stats,
            "timestamp": pendulum.now("UTC").isoformat(),
        }

    def get_throughput(self, window_minutes: int = 60) -> dict[str, Any]:
        """
        Get jobs processed per minute over a time window.

        Args:
            window_minutes: Time window in minutes to measure

        Returns:
            Dict with throughput metrics per queue
        """
        cutoff_time = pendulum.now("UTC").subtract(minutes=window_minutes)

        throughput = {}
        for job_id, job_stat in self.job_stats.items():
            started = job_stat.get("started_at")
            if not started or started < cutoff_time:
                continue

            queue_name = job_stat.get("queue", "unknown")
            if queue_name not in throughput:
                throughput[queue_name] = {
                    "jobs_processed": 0,
                    "jobs_per_minute": 0.0,
                    "jobs_completed": 0,
                }

            throughput[queue_name]["jobs_processed"] += 1

            if job_stat.get("status") == "completed":
                throughput[queue_name]["jobs_completed"] += 1

        # Calculate jobs per minute
        minutes_elapsed = max(window_minutes, 1)
        for queue_name, q_throughput in throughput.items():
            q_throughput["jobs_per_minute"] = round(
                q_throughput["jobs_processed"] / minutes_elapsed, 2
            )

        return {
            "window_minutes": window_minutes,
            "throughput": throughput,
            "timestamp": pendulum.now("UTC").isoformat(),
        }

    def get_error_breakdown(self) -> dict[str, Any]:
        """
        Get failure counts grouped by error type and job class.

        Returns:
            Dict with error statistics by type and job class
        """
        error_stats = {
            "by_type": {},
            "by_job_class": {},
            "by_queue": {},
        }

        for job_stat in self.job_stats.values():
            if job_stat.get("status") != "failed":
                continue

            # Track by error type
            error_type = job_stat.get("error_type", "unknown")
            if error_type not in error_stats["by_type"]:
                error_stats["by_type"][error_type] = {
                    "count": 0,
                    "jobs": [],
                    "sample_errors": [],
                }
            error_stats["by_type"][error_type]["count"] += 1
            error_stats["by_type"][error_type]["jobs"].append(job_stat["job_name"])

            if len(error_stats["by_type"][error_type]["sample_errors"]) < 3:
                error_stats["by_type"][error_type]["sample_errors"].append(
                    job_stat.get("error", "No error message")[:200]
                )

            # Track by job class
            job_class = job_stat.get("job_class", "unknown")
            if job_class not in error_stats["by_job_class"]:
                error_stats["by_job_class"][job_class] = {
                    "count": 0,
                    "errors": {},
                }
            error_stats["by_job_class"][job_class]["count"] += 1

            error_key = job_stat.get("error_type", "unknown")
            error_stats["by_job_class"][job_class]["errors"][error_key] = (
                error_stats["by_job_class"][job_class]["errors"].get(error_key, 0) + 1
            )

            # Track by queue
            queue_name = job_stat.get("queue", "unknown")
            if queue_name not in error_stats["by_queue"]:
                error_stats["by_queue"][queue_name] = {
                    "count": 0,
                    "errors": {},
                }
            error_stats["by_queue"][queue_name]["count"] += 1

            error_type_for_queue = job_stat.get("error_type", "unknown")
            error_stats["by_queue"][queue_name]["errors"][error_type_for_queue] = (
                error_stats["by_queue"][queue_name]["errors"].get(error_type_for_queue, 0)
                + 1
            )

        return {
            "error_breakdown": error_stats,
            "total_failed": len(
                [j for j in self.job_stats.values() if j["status"] == "failed"]
            ),
            "timestamp": pendulum.now("UTC").isoformat(),
        }

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
