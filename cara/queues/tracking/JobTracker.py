"""
Advanced Job Tracker for Cara Framework.

App-agnostic job tracking service with smart retry logic, conflict resolution,
and performance analytics. Similar to Laravel's job tracking but enhanced.
"""

import uuid
from typing import Any, Dict, List, Optional

import pendulum

from cara.facades import Log


class JobTracker:
    """
    Unified job tracking and management service for Cara Framework.

    Features:
    - Smart retry with exponential backoff
    - Job chaining and dependencies
    - Conflict resolution (prevent duplicate jobs)
    - Performance analytics
    - Dead letter queue management
    - App-agnostic design
    - Unified Job model (replaces separate job + job_logs tables)

    Usage:
        # In app, provide unified Job model
        from cara.queues.tracking import JobTracker

        # Configure with app's Job model
        tracker = JobTracker(job_model=Job)

        # Or use without persistence (logs only)
        tracker = JobTracker()
    """

    # Default retry configuration (apps can override)
    DEFAULT_MAX_RETRIES = {"default": 3}

    DEFAULT_RETRY_DELAYS = [60, 300, 900]  # 1min, 5min, 15min

    def __init__(
        self,
        job_model=None,
        max_retries: Dict[str, int] = None,
        retry_delays: List[int] = None,
        job_log_model=None,  # DEPRECATED - kept for backwards compatibility
    ):
        """
        Initialize JobTracker with dependency injection.

        IMPORTANT: Models MUST be injected via ApplicationProvider.
        Framework never imports app-specific models directly.

        Args:
            job_model: Unified Job model class (REQUIRED for tracking)
            max_retries: Dict of job_name -> max_retry_count
            retry_delays: List of delay seconds for retries
            job_log_model: DEPRECATED - separate JobLog table no longer used

        Example (ApplicationProvider):
            # In your app's ApplicationProvider:
            from cara.queues.tracking import JobTracker
            from app.models import Job  # App imports here, not in framework

            tracker = JobTracker(job_model=Job)
            self.application.bind("JobTracker", tracker)
        """
        self.job_model = job_model
        self.max_retries = max_retries or self.DEFAULT_MAX_RETRIES
        self.retry_delays = retry_delays or self.DEFAULT_RETRY_DELAYS

        # DEPRECATED: Backwards compatibility warning
        if job_log_model:
            Log.warning(
                "⚠️ JobLog model is deprecated - unified Job model is now used for tracking. "
                "Please remove job_log_model parameter."
            )

        # Log warning if model not injected
        if not self.job_model:
            Log.warning(
                "⚠️ Job model not injected - job tracking disabled. "
                "Register JobTracker in ApplicationProvider with Job model."
            )

    def track_job_started(
        self,
        job_uid: str,
        job_name: str,
        job_id: int = None,
        entity_id: str = None,
        queue: str = "default",
        metadata: Dict = None,
    ) -> str:
        """
        Track job start - updates existing job record with tracking info.

        Args:
            job_uid: Unique job UUID identifier for tracking
            job_name: Job class name
            job_id: Job.id to update
            entity_id: Optional entity ID (product_id, user_id, etc.)
            queue: Queue name
            metadata: Additional metadata

        Returns:
            str: The job_uid for tracking
        """
        try:
            # Cancel conflicting jobs for same entity
            if entity_id:
                self._cancel_conflicting_jobs(job_name, entity_id, job_uid)

            # Update job record if job_id provided (unified table approach)
            if self.job_model and job_id:
                job_record = self.job_model.find(job_id)
                if job_record:
                    # Update with tracking info
                    job_record.job_uid = job_uid
                    if entity_id:
                        job_record.entity_id = entity_id

                    # Merge metadata with pipeline_id
                    from cara.context import ExecutionContext

                    enriched_metadata = job_record.metadata or {}
                    enriched_metadata.update(metadata or {})
                    pipeline_id = ExecutionContext.get_job_id()
                    if pipeline_id:
                        enriched_metadata["pipeline_id"] = pipeline_id

                    job_record.metadata = enriched_metadata
                    job_record.save()

            Log.info(
                f"🚀 Job started: {job_name}[{job_uid}] for entity {entity_id}",
                category="cara.queue.jobs",
            )
            return job_uid

        except Exception as e:
            Log.warning(f"Failed to track job start: {str(e)}")
            return job_uid

    def track_job_processing(self, job_uid: str) -> None:
        """Mark job as actively processing."""
        if self.job_model:
            job_record = self.job_model.where("job_uid", job_uid).first()
            if job_record:
                job_record.status = self.job_model.STATUS_PROCESSING
                job_record.processed_at = pendulum.now()
                job_record.save()

    def track_job_success(self, job_uid: str, result_data: Dict = None) -> None:
        """Track successful job completion."""
        if not self.job_model:
            return

        job_record = self.job_model.where("job_uid", job_uid).first()
        if job_record:
            job_record.status = self.job_model.STATUS_SUCCESS
            job_record.finished_at = pendulum.now()

            # Store result metadata if provided
            if result_data:
                metadata = job_record.metadata or {}
                metadata["result"] = result_data
                job_record.metadata = metadata

            job_record.save()

    def track_job_failed(
        self, job_uid: str, error: str, should_retry: bool = True
    ) -> Optional[str]:
        """
        Track job failure and handle retry logic.

        Args:
            job_uid: Current job UID
            error: Error message
            should_retry: Whether to attempt retry

        Returns:
            Optional[str]: New job_uid if retry scheduled, None if max retries exceeded
        """
        try:
            if not self.job_model:
                Log.error(
                    f"💥 Job failed: {job_uid} - {error}", category="cara.queue.jobs"
                )
                return None

            # Get current job info
            job_record = self.job_model.where("job_uid", job_uid).first()
            if not job_record:
                Log.error(f"Job not found for {job_uid}", category="cara.queue.jobs")
                return None

            # Mark current attempt as failed
            job_record.status = self.job_model.STATUS_FAILED
            job_record.error = error
            job_record.finished_at = pendulum.now()
            job_record.save()

            # Check if we should retry
            max_retries = self.max_retries.get(
                job_record.name, self.max_retries["default"]
            )

            if should_retry and job_record.attempt < max_retries:
                return self._schedule_retry(job_record, error)
            else:
                self._move_to_dead_letter(job_record, error)
                Log.error(
                    f"💀 Job failed permanently: {job_uid} after {job_record.attempt} attempts",
                    category="cara.queue.jobs",
                )
                return None

        except Exception as e:
            Log.warning(f"Failed to track job failure: {str(e)}")
            return None

    def should_job_continue(self, job_uid: str, entity_id: str = None) -> bool:
        """
        Check if job should continue processing based on job_uid.

        Args:
            job_uid: Current job UID (UUID string)
            entity_id: Optional entity ID for conflict checking

        Returns:
            bool: True if job should continue
        """
        try:
            if not self.job_log_model:
                return True

            job_log = self.job_log_model.where("job_uid", job_uid).first()
            if not job_log:
                return True

            # Check if job was cancelled or failed
            if job_log.status in [self.job_log_model.STATUS_CANCELLED]:
                return False

            return True

        except Exception as e:
            Log.warning(f"Error checking job status {job_uid}: {str(e)}")
            return True

    def validate_job_or_cancel(
        self, job_uid: str, entity_id: str = None, operation: str = "operation"
    ) -> None:
        """
        Validate job should continue or raise JobCancelledException.

        Args:
            job_uid: Current job UID (UUID string)
            entity_id: Optional entity ID
            operation: Operation name for logging

        Raises:
            JobCancelledException: If job should not continue
        """
        if not self.should_job_continue(job_uid, entity_id):
            # Lazy import to avoid circular dependency
            from cara.queues.contracts.CancellableJob import JobCancelledException

            raise JobCancelledException(
                f"Job {job_uid} cancelled during {operation} for entity {entity_id}"
            )

    def get_job_analytics(
        self, entity_id: str = None, job_name: str = None, hours: int = 24
    ) -> Dict[str, Any]:
        """
        Get job performance analytics.

        Args:
            entity_id: Optional entity filter
            job_name: Optional job name filter
            hours: Time window in hours

        Returns:
            Dict with analytics data
        """
        if not self.job_log_model:
            return {"total_jobs": 0, "message": "No JobLog model configured"}

        try:
            query = self.job_log_model.query()

            if entity_id:
                query = query.where("product_id", entity_id)
            if job_name:
                query = query.where("job_name", job_name)

            # Time window
            since = pendulum.now().subtract(hours=hours)
            jobs = query.where("created_at", ">=", since).get()

            total_jobs = len(jobs)
            if total_jobs == 0:
                return {"total_jobs": 0}

            # Status counts
            status_counts = {}
            for job in jobs:
                status = getattr(job, "status", "unknown")
                status_counts[status] = status_counts.get(status, 0) + 1

            # Average processing time for successful jobs
            successful_jobs = [
                j
                for j in jobs
                if getattr(j, "status", None) == "success"
                and hasattr(j, "finished_at")
                and hasattr(j, "processed_at")
                and j.finished_at
                and j.processed_at
            ]

            avg_processing_time = 0
            if successful_jobs:
                total_time = sum(
                    [
                        (j.finished_at - j.processed_at).total_seconds()
                        for j in successful_jobs
                    ]
                )
                avg_processing_time = total_time / len(successful_jobs)

            success_count = status_counts.get("success", 0)

            return {
                "total_jobs": total_jobs,
                "status_counts": status_counts,
                "success_count": success_count,
                "success_rate": (success_count / total_jobs * 100)
                if total_jobs > 0
                else 0,
                "avg_processing_time_seconds": avg_processing_time,
                "period_hours": hours,
            }

        except Exception as e:
            Log.error(f"Failed to get job analytics: {str(e)}")
            return {"error": str(e)}

    def _cancel_conflicting_jobs(
        self, job_name: str, entity_id: str, current_job_uid: str
    ) -> int:
        """
        Cancel conflicting jobs for same entity.

        Args:
            job_name: Job class name
            entity_id: Entity ID to check conflicts for
            current_job_uid: Current job UID to exclude

        Returns:
            int: Number of jobs cancelled
        """
        try:
            if not self.job_model:
                return 0

            # Find pending/processing jobs for same entity and job type
            conflicting_jobs = (
                self.job_model.where("name", job_name)
                .where("entity_id", entity_id)
                .where("job_uid", "!=", current_job_uid)
                .where_in("status", ["pending", "processing"])
                .get()
            )

            cancelled_count = 0
            for job_record in conflicting_jobs:
                job_record.status = self.job_model.STATUS_CANCELLED
                job_record.cancelled_at = pendulum.now()
                job_record.save()
                cancelled_count += 1
                Log.info(
                    f"Cancelled conflicting job: {job_record.job_uid} for entity {entity_id}"
                )

            return cancelled_count

        except Exception as e:
            Log.warning(f"Failed to cancel conflicting jobs: {str(e)}")
            return 0

    def _schedule_retry(self, job_record, error: str) -> str:
        """Schedule job retry with exponential backoff - updates existing job."""
        try:
            next_attempt = job_record.attempt + 1
            delay_seconds = self.retry_delays[
                min(next_attempt - 1, len(self.retry_delays) - 1)
            ]

            # Update existing job for retry
            retry_job_uid = str(uuid.uuid4())

            metadata = job_record.metadata or {}
            metadata["retry_reason"] = error
            metadata["original_job_uid"] = job_record.job_uid
            metadata["scheduled_for"] = (
                pendulum.now().add(seconds=delay_seconds).to_iso8601_string()
            )

            job_record.job_uid = retry_job_uid
            job_record.status = self.job_model.STATUS_RETRYING
            job_record.attempt = next_attempt
            job_record.error = None  # Clear previous error
            job_record.metadata = metadata
            job_record.save()

            Log.info(
                f"🔄 Retry scheduled: {job_record.name}[{retry_job_uid}] attempt {next_attempt} in {delay_seconds}s"
            )
            return retry_job_uid

        except Exception as e:
            Log.error(f"Failed to schedule retry: {str(e)}")
            return None

    def _move_to_dead_letter(self, job_record, final_error: str) -> None:
        """Move permanently failed job to dead letter queue."""
        try:
            metadata = job_record.metadata or {}
            metadata["dead_letter_reason"] = final_error
            metadata["moved_to_dlq_at"] = pendulum.now().to_iso8601_string()
            job_record.metadata = metadata
            job_record.save()

            Log.error(
                f"💀 Job moved to dead letter: {job_record.name}[{job_record.job_uid}] - {final_error}"
            )

        except Exception as e:
            Log.warning(f"Failed to move job to dead letter: {str(e)}")

    def create_sync_job_record(
        self,
        job_name: str,
        job_class: str,
        queue: str,
        payload: Dict = None,
        metadata: Dict = None,
        job_uid: str = None,
        entity_id: str = None,
    ) -> Optional[int]:
        """
        Create unified job record with tracking fields.

        Args:
            job_name: Job class name
            job_class: Full job class path (module.ClassName)
            queue: Queue name
            payload: Job parameters (for replay/debugging)
            metadata: Additional metadata
            job_uid: Unique job UUID (generated if not provided)
            entity_id: Optional entity ID (product_id, user_id, etc.)

        Returns:
            int: job.id, or None if no Job model
        """
        try:
            if not self.job_model:
                return None

            # Generate job_uid if not provided
            if not job_uid:
                job_uid = str(uuid.uuid4())

            # Enrich metadata with pipeline_id from ExecutionContext
            from cara.context import ExecutionContext

            enriched_metadata = metadata or {}
            pipeline_id = ExecutionContext.get_job_id()
            if pipeline_id:
                enriched_metadata["pipeline_id"] = pipeline_id
            enriched_metadata["sync_execution"] = True

            job_record = self.job_model.create(
                {
                    "public_id": self.job_model.generate_public_id(),
                    "name": job_name,
                    "job_class": job_class,
                    "payload": payload or {},
                    "queue": queue,
                    "available_at": pendulum.now(),
                    "status": self.job_model.STATUS_PENDING,
                    "attempts": 0,
                    "attempt": 1,
                    "job_uid": job_uid,
                    "entity_id": entity_id,
                    "metadata": enriched_metadata,
                }
            )

            return job_record.id

        except Exception as e:
            Log.warning(f"Failed to create sync job record: {str(e)}")
            return None

    def update_job_status(self, job_id: int, status: str) -> None:
        """
        Update job record status.

        Args:
            job_id: Job ID to update
            status: New status (pending, processing, completed, failed)
        """
        if not self.job_model:
            return

        # Get job instance and update directly (immediate persistence)
        job_record = self.job_model.find(job_id)
        if not job_record:
            return

        # Update job attributes
        job_record.status = status

        # Use model constants for conditional updates
        if status == self.job_model.STATUS_PROCESSING:
            job_record.started_at = pendulum.now()
        elif status == self.job_model.STATUS_COMPLETED:
            job_record.completed_at = pendulum.now()
        elif status == self.job_model.STATUS_FAILED:
            job_record.completed_at = pendulum.now()

        # Save immediately (persists to database)
        job_record.save()
