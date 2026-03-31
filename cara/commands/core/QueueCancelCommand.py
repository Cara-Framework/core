"""
QueueCancelCommand: Cancel running jobs with flexible filtering options.
Provides safe job cancellation capabilities for the enhanced queue system.
"""

from cara.commands import CommandBase
from cara.decorators import command
from cara.facades import Queue
from cara.queues.JobStateManager import get_job_state_manager


@command(
    name="queue:cancel",
    help="Cancel running jobs with flexible filtering options",
    options={
        "--job-id=": "Cancel specific job by ID",
        "--receipt-id=": "Cancel all jobs for specific receipt",
        "--job-type=": "Cancel all jobs of specific type (ocr, vision, automation)",
        "--all": "Cancel all active jobs (use with caution)",
        "--force": "Force cancellation without confirmation",
        "--dry-run": "Show what would be cancelled without actually cancelling",
    },
)
class QueueCancelCommand(CommandBase):
    """Cancel running jobs with comprehensive filtering and safety features."""

    def handle(self):
        """Handle job cancellation with various filtering options."""
        job_id = self.option("job-id")
        receipt_id = self.option("receipt-id")
        job_type = self.option("job-type")
        cancel_all = self.option("all", False)
        force = self.option("force", False)
        dry_run = self.option("dry-run", False)

        # Validate options
        if not any([job_id, receipt_id, job_type, cancel_all]):
            self.error(
                "❌ Must specify one of: --job-id, --receipt-id, --job-type, or --all"
            )
            return

        if sum(bool(x) for x in [job_id, receipt_id, job_type, cancel_all]) > 1:
            self.error("❌ Cannot combine multiple filtering options")
            return

        try:
            job_state_manager = get_job_state_manager()
            active_jobs = job_state_manager.get_active_jobs()

            if not active_jobs:
                self.info("ℹ️  No active jobs to cancel")
                return

            # Determine what to cancel
            if job_id:
                cancelled = self._cancel_by_job_id(job_id, dry_run, force)
            elif receipt_id:
                cancelled = self._cancel_by_receipt_id(
                    receipt_id, active_jobs, dry_run, force
                )
            elif job_type:
                cancelled = self._cancel_by_job_type(
                    job_type, active_jobs, dry_run, force
                )
            elif cancel_all:
                cancelled = self._cancel_all_jobs(active_jobs, dry_run, force)

            if dry_run:
                self.info(f"🔍 Dry run complete. Would cancel {cancelled} job(s)")
            else:
                self.success(f"✅ Successfully cancelled {cancelled} job(s)")

        except Exception as e:
            self.error(f"❌ Failed to cancel jobs: {e}")

    def _cancel_by_job_id(self, job_id: str, dry_run: bool, force: bool) -> int:
        """Cancel specific job by ID."""
        if dry_run:
            job_state_manager = get_job_state_manager()
            active_jobs = job_state_manager.get_active_jobs()
            if job_id in active_jobs:
                self.info(f"🔍 Would cancel job: {job_id}")
                return 1
            else:
                self.info(f"🔍 Job {job_id} not found in active jobs")
                return 0

        if not force:
            if not self._confirm_action(f"Cancel job {job_id}?"):
                self.info("❌ Cancelled by user")
                return 0

        success = Queue.cancel_job(job_id)
        if success:
            self.info(f"🚫 Cancelled job: {job_id}")
            return 1
        else:
            self.error(f"❌ Failed to cancel job: {job_id}")
            return 0

    def _cancel_by_receipt_id(
        self, receipt_id: str, active_jobs: dict, dry_run: bool, force: bool
    ) -> int:
        """Cancel all jobs for specific receipt."""
        matching_jobs = [
            job_id
            for job_id, job_info in active_jobs.items()
            if job_info.get("context", {}).get("receipt_id") == receipt_id
        ]

        if not matching_jobs:
            self.info(f"ℹ️  No active jobs found for receipt: {receipt_id}")
            return 0

        if dry_run:
            self.info(
                f"🔍 Would cancel {len(matching_jobs)} job(s) for receipt {receipt_id}:"
            )
            for job_id in matching_jobs:
                job_info = active_jobs[job_id]
                job_type = job_info.get("context", {}).get("job_type", "unknown")
                self.info(f"   - {job_id} ({job_type})")
            return len(matching_jobs)

        if not force:
            if not self._confirm_action(
                f"Cancel {len(matching_jobs)} job(s) for receipt {receipt_id}?"
            ):
                self.info("❌ Cancelled by user")
                return 0

        def should_cancel(context: dict) -> bool:
            return context.get("receipt_id") == receipt_id

        cancelled = Queue.cancel_jobs_by_context(
            should_cancel, f"Cancelled by admin for receipt {receipt_id}"
        )
        self.info(f"🚫 Cancelled {cancelled} job(s) for receipt: {receipt_id}")
        return cancelled

    def _cancel_by_job_type(
        self, job_type: str, active_jobs: dict, dry_run: bool, force: bool
    ) -> int:
        """Cancel all jobs of specific type."""
        matching_jobs = [
            job_id
            for job_id, job_info in active_jobs.items()
            if job_info.get("context", {}).get("job_type") == job_type
        ]

        if not matching_jobs:
            self.info(f"ℹ️  No active jobs found for type: {job_type}")
            return 0

        if dry_run:
            self.info(f"🔍 Would cancel {len(matching_jobs)} job(s) of type {job_type}:")
            for job_id in matching_jobs:
                job_info = active_jobs[job_id]
                receipt_id = job_info.get("context", {}).get("receipt_id", "unknown")
                self.info(f"   - {job_id} (receipt: {receipt_id})")
            return len(matching_jobs)

        if not force:
            if not self._confirm_action(
                f"Cancel {len(matching_jobs)} job(s) of type {job_type}?"
            ):
                self.info("❌ Cancelled by user")
                return 0

        def should_cancel(context: dict) -> bool:
            return context.get("job_type") == job_type

        cancelled = Queue.cancel_jobs_by_context(
            should_cancel, f"Cancelled by admin for job type {job_type}"
        )
        self.info(f"🚫 Cancelled {cancelled} job(s) of type: {job_type}")
        return cancelled

    def _cancel_all_jobs(self, active_jobs: dict, dry_run: bool, force: bool) -> int:
        """Cancel all active jobs."""
        if not active_jobs:
            return 0

        if dry_run:
            self.info(f"🔍 Would cancel ALL {len(active_jobs)} active job(s):")
            for job_id, job_info in active_jobs.items():
                context = job_info.get("context", {})
                job_type = context.get("job_type", "unknown")
                receipt_id = context.get("receipt_id", "unknown")
                self.info(f"   - {job_id} ({job_type}, receipt: {receipt_id})")
            return len(active_jobs)

        if not force:
            self.error("⚠️  WARNING: This will cancel ALL active jobs!")
            if not self._confirm_action(f"Cancel ALL {len(active_jobs)} active job(s)?"):
                self.info("❌ Cancelled by user")
                return 0

        def should_cancel_all(context: dict) -> bool:
            return True  # Cancel everything

        cancelled = Queue.cancel_jobs_by_context(
            should_cancel_all, "Cancelled by admin - cancel all command"
        )
        self.info(f"🚫 Cancelled ALL {cancelled} active job(s)")
        return cancelled

    def _confirm_action(self, message: str) -> bool:
        """Ask for user confirmation."""
        response = input(f"{message} [y/N]: ").lower().strip()
        return response in ["y", "yes"]
