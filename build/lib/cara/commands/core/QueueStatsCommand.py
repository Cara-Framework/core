"""
QueueStatsCommand: Display enhanced queue statistics and job status breakdown.
Shows comprehensive job tracking information from the enhanced job management system.
"""

import time
from datetime import datetime, timedelta

from cara.commands import CommandBase
from cara.decorators import command
from cara.queues.JobStateManager import get_job_state_manager


@command(
    name="queue:stats",
    help="Display enhanced queue statistics and job status breakdown",
    options={
        "--queue=default": "Queue name to show stats for",
        "--active": "Show only active jobs",
        "--recent=24": "Show jobs from last N hours (default: 24)",
        "--watch": "Watch mode - refresh every 5 seconds",
    },
)
class QueueStatsCommand(CommandBase):
    """Display comprehensive queue statistics with job status tracking."""

    def handle(self):
        """Show queue statistics with optional watch mode."""
        queue_name = self.option("queue", "default")
        watch_mode = self.option("watch", False)
        recent_hours = int(self.option("recent", 24))
        show_active_only = self.option("active", False)

        if watch_mode:
            self._watch_mode(queue_name, recent_hours, show_active_only)
        else:
            self._show_stats_once(queue_name, recent_hours, show_active_only)

    def _watch_mode(self, queue_name: str, recent_hours: int, show_active_only: bool):
        """Watch mode - refresh stats every 5 seconds."""
        self.info("📊 Queue Stats Watch Mode (Ctrl+C to exit)")
        self.info("Refreshing every 5 seconds...\n")

        try:
            while True:
                # Clear screen
                import os

                os.system("clear" if os.name == "posix" else "cls")

                self.info(
                    f"📊 Queue Stats Watch Mode - {datetime.now().strftime('%H:%M:%S')}"
                )
                self.info("=" * 60)

                self._show_stats_once(queue_name, recent_hours, show_active_only)

                self.info("\n🔄 Refreshing in 5 seconds... (Ctrl+C to exit)")
                time.sleep(5)

        except KeyboardInterrupt:
            self.info("\n👋 Watch mode stopped.")

    def _show_stats_once(
        self, queue_name: str, recent_hours: int, show_active_only: bool
    ):
        """Show queue statistics once."""
        try:
            # Get job state manager
            job_state_manager = get_job_state_manager()

            # Show active jobs from memory
            self._show_active_jobs(job_state_manager, show_active_only)

            # Show database stats if Job model available
            self._show_database_stats(queue_name, recent_hours)

        except Exception as e:
            self.error(f"Failed to get queue stats: {e}")

    def _show_active_jobs(self, job_state_manager, show_active_only: bool):
        """Show currently active jobs from JobStateManager."""
        active_jobs = job_state_manager.get_active_jobs()

        self.info(f"🚀 Active Jobs in Memory: {len(active_jobs)}")

        if not active_jobs:
            self.info("   No active jobs currently running")
            return

        self.info("-" * 60)

        for job_id, job_info in active_jobs.items():
            context = job_info.get("context", {})
            status = job_info.get("status", "unknown")
            start_time = job_info.get("start_time")

            # Calculate runtime
            runtime = "unknown"
            if start_time:
                runtime_seconds = int(time.time() - start_time.timestamp())
                minutes, seconds = divmod(runtime_seconds, 60)
                runtime = f"{minutes:02d}:{seconds:02d}"

            self.info(f"   Job ID: {job_id}")
            self.info(f"   Type: {context.get('job_type', 'unknown')}")
            self.info(f"   Receipt ID: {context.get('receipt_id', 'unknown')}")
            self.info(f"   Status: {status}")
            self.info(f"   Runtime: {runtime}")
            self.info("")

    def _show_database_stats(self, queue_name: str, recent_hours: int):
        """Show database job statistics if available."""
        try:
            # Try universal job tracker first (Cara framework)
            from cara.queues.JobTracker import get_job_tracker

            job_tracker = get_job_tracker()
            if job_tracker.is_enabled():
                stats = job_tracker.get_job_stats(queue_name)
                if stats:
                    self._display_stats(stats, queue_name, "Universal Job Tracker")
                    return

            # Fallback to app-level Job model
            from app.models import Job

            # Get overall stats
            stats = Job.get_queue_stats(queue_name)

            self._display_stats(stats, queue_name, "App Job Model")

            # Recent activity
            cutoff_time = datetime.now() - timedelta(hours=recent_hours)
            recent_jobs = Job.where("created_at", ">=", cutoff_time.isoformat()).get()

            if recent_jobs:
                self.info(
                    f"\n📈 Recent Activity (Last {recent_hours}h): {len(recent_jobs)} jobs"
                )

                # Group by status
                status_counts = {}
                for job in recent_jobs:
                    status = getattr(job, "status", "unknown")
                    status_counts[status] = status_counts.get(status, 0) + 1

                for status, count in status_counts.items():
                    self.info(f"   {status.title()}: {count}")

        except ImportError:
            self.info("💾 Database Stats: Job model not available")
        except Exception as e:
            self.error(f"Failed to get database stats: {e}")

    def _display_stats(self, stats: dict, queue_name: str, source: str):
        """Display job statistics in a formatted way."""
        self.info(f"💾 Database Job Stats - {source} (Queue: {queue_name})")
        self.info("-" * 60)
        self.info(f"   Pending: {stats.get('pending_jobs', 0)}")
        self.info(f"   Processing: {stats.get('processing_jobs', 0)}")
        self.info(f"   Completed: {stats.get('completed_jobs', 0)}")
        self.info(f"   Cancelled: {stats.get('cancelled_jobs', 0)}")
        self.info(f"   Failed: {stats.get('failed_jobs', 0)}")
        self.info(f"   Total: {stats.get('total_jobs', 0)}")
