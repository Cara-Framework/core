"""
Advanced Job Tracker for Cara Framework.

App-agnostic job tracking service with smart retry logic, conflict resolution,
and performance analytics. Similar to Laravel's job tracking but enhanced.
"""

from __future__ import annotations

import uuid
from typing import Any

import pendulum

from cara.facades import Log


class JobTracker:
    """
    Unified job tracking and management service for Cara Framework.

    Features:
    - Smart retry with exponential backoff
    - Conflict resolution (prevent duplicate jobs)
    - Performance analytics
    - Dead letter queue management
    - App-agnostic design
    - Unified Job model (replaces separate job + job_logs tables)

    Usage:
        # In app, provide unified Job model
        from cara.queues.tracking.JobTracker import JobTracker

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
        max_retries: dict[str, int] | None = None,
        retry_delays: list[int] | None = None,
    ):
        """
        Initialize JobTracker with dependency injection.

        IMPORTANT: Models MUST be injected via ApplicationProvider.
        Framework never imports app-specific models directly.

        Args:
            job_model: Unified Job model class (REQUIRED for tracking)
            max_retries: Dict of job_name -> max_retry_count
            retry_delays: List of delay seconds for retries

        Example (ApplicationProvider):
            from cara.queues.tracking.JobTracker import JobTracker
            from app.models import JobModel

            tracker = JobTracker(job_model=JobModel)
            self.application.bind("JobTracker", tracker)
        """
        self.job_model = job_model
        self.max_retries = max_retries or self.DEFAULT_MAX_RETRIES
        self.retry_delays = retry_delays or self.DEFAULT_RETRY_DELAYS

        if not self.job_model:
            Log.warning(
                "⚠️ Job model not injected - job tracking disabled. "
                "Register JobTracker in ApplicationProvider with Job model."
            )

    def track_job_started(
        self,
        job_uid: str,
        job_name: str,
        job_id: int | None = None,
        entity_id: str | None = None,
        queue: str = "default",
        metadata: dict | None = None,
    ) -> str:
        """
        Track job start - updates existing job record with tracking info.

        Args:
            job_uid: Unique job UUID identifier for tracking
            job_name: Job class name
            job_id: Job.id to update
            entity_id: Optional domain entity identifier for conflict detection
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

            Log.debug(
                "🚀 Job started: %s[%s] for entity %s",
                job_name,
                job_uid,
                entity_id,
                category="cara.queue.jobs",
            )
            return job_uid

        except Exception as e:
            Log.warning("Failed to track job start: %s", str(e))
            return job_uid

    def track_job_processing(self, job_uid: str) -> None:
        """Mark job as actively processing."""
        if self.job_model:
            job_record = self.job_model.where("job_uid", job_uid).first()
            if job_record:
                self._transition(job_record, self.job_model.STATUS_PROCESSING)
                job_record.save()

    def track_job_success(self, job_uid: str, result_data: dict | None = None) -> None:
        """Track successful job completion."""
        if not self.job_model:
            return

        job_record = self.job_model.where("job_uid", job_uid).first()
        if job_record:
            self._transition(job_record, self.job_model.STATUS_SUCCESS)

            # Store result metadata if provided
            if result_data:
                metadata = job_record.metadata or {}
                metadata["result"] = result_data
                job_record.metadata = metadata

            job_record.save()

    def track_job_failed(
        self, job_uid: str, error: str, should_retry: bool = True
    ) -> str | None:
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
                    "💥 Job failed: %s - %s", job_uid, error, category="cara.queue.jobs"
                )
                return None

            # Get current job info
            job_record = self.job_model.where("job_uid", job_uid).first()
            if not job_record:
                Log.error("Job not found for %s", job_uid, category="cara.queue.jobs")
                return None

            # Mark current attempt as failed
            self._transition(job_record, self.job_model.STATUS_FAILED)
            job_record.error = error
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
                    "💀 Job failed permanently: %s after %s attempts",
                    job_uid,
                    job_record.attempt,
                    category="cara.queue.jobs",
                )
                return None

        except Exception as e:
            Log.warning("Failed to track job failure: %s", str(e))
            return None

    def should_job_continue(self, job_uid: str, entity_id: str | None = None) -> bool:
        """
        Check if job should continue processing based on job_uid.

        Args:
            job_uid: Current job UID (UUID string)
            entity_id: Optional entity ID for conflict checking

        Returns:
            bool: True if job should continue
        """
        try:
            if not self.job_model:
                return True

            job_record = self.job_model.where("job_uid", job_uid).first()
            if not job_record:
                return True

            return job_record.status not in [self.job_model.STATUS_CANCELLED]

        except Exception as e:
            Log.warning("Error checking job status %s: %s", job_uid, str(e))
            return True

    def validate_job_or_cancel(
        self, job_uid: str, entity_id: str | None = None, operation: str = "operation"
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
        self, entity_id: str | None = None, job_name: str | None = None, hours: int = 24
    ) -> dict[str, Any]:
        """
        Get job performance analytics.

        Args:
            entity_id: Optional entity filter
            job_name: Optional job name filter
            hours: Time window in hours

        Returns:
            Dict with analytics data
        """
        if not self.job_model:
            return {"total_jobs": 0, "message": "No Job model configured"}

        try:
            query = self.job_model.query()

            if entity_id:
                query = query.where("entity_id", entity_id)
            if job_name:
                query = query.where("name", job_name)

            # Time window
            since = pendulum.now("UTC").subtract(hours=hours)
            jobs = query.where("created_at", ">=", since).get()

            total_jobs = len(jobs)
            if total_jobs == 0:
                return {"total_jobs": 0}

            # Status counts
            status_counts = {}
            for job in jobs:
                status = getattr(job, "status", "unknown")
                status_counts[status] = status_counts.get(status, 0) + 1

            # Average processing time over the ONE lifecycle contract
            # (started_at → completed_at) that ``_transition`` owns.
            succeeded = self._success_statuses()
            successful_jobs = [
                j
                for j in jobs
                if getattr(j, "status", None) in succeeded
                and getattr(j, "completed_at", None)
                and getattr(j, "started_at", None)
            ]

            avg_processing_time = 0
            if successful_jobs:
                total_time = sum(
                    [
                        (j.completed_at - j.started_at).total_seconds()
                        for j in successful_jobs
                    ]
                )
                avg_processing_time = total_time / len(successful_jobs)

            success_count = sum(
                count for status, count in status_counts.items() if status in succeeded
            )

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
            Log.error("Failed to get job analytics: %s", str(e))
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
                job_record.cancelled_at = pendulum.now("UTC")
                job_record.save()
                cancelled_count += 1
                Log.debug(
                    "Cancelled conflicting job: %s for entity %s",
                    job_record.job_uid,
                    entity_id,
                )

            return cancelled_count

        except Exception as e:
            Log.warning("Failed to cancel conflicting jobs: %s", str(e))
            return 0

    def _schedule_retry(self, job_record, error: str) -> str | None:
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
                pendulum.now("UTC").add(seconds=delay_seconds).to_iso8601_string()
            )

            job_record.job_uid = retry_job_uid
            job_record.status = self.job_model.STATUS_RETRYING
            job_record.attempt = next_attempt
            job_record.error = None  # Clear previous error
            job_record.metadata = metadata
            job_record.save()

            Log.debug(
                "🔄 Retry scheduled: %s[%s] attempt %s in %ss",
                job_record.name,
                retry_job_uid,
                next_attempt,
                delay_seconds,
            )
            return retry_job_uid

        except Exception as e:
            Log.error("Failed to schedule retry: %s", str(e))
            return None

    def _move_to_dead_letter(self, job_record, final_error: str) -> None:
        """Move permanently failed job to dead letter queue."""
        try:
            metadata = job_record.metadata or {}
            metadata["dead_letter_reason"] = final_error
            metadata["moved_to_dlq_at"] = pendulum.now("UTC").to_iso8601_string()
            job_record.metadata = metadata
            job_record.save()

            Log.error(
                "💀 Job moved to dead letter: %s[%s] - %s",
                job_record.name,
                job_record.job_uid,
                final_error,
            )

        except Exception as e:
            Log.warning("Failed to move job to dead letter: %s", str(e))

    def create_job_record(
        self,
        job_name: str,
        job_class: str,
        queue: str,
        *,
        execution_mode: str,
        payload: dict | None = None,
        metadata: dict | None = None,
        job_uid: str | None = None,
        entity_id: str | None = None,
    ) -> int | None:
        """
        Create a unified job record with an explicit execution mode.

        Args:
            job_name: Job class name
            job_class: Full job class path (module.ClassName)
            queue: Queue name
            execution_mode: One of ``sync``, ``queued`` or ``scheduler``
            payload: Job parameters (for replay/debugging)
            metadata: Additional metadata
            job_uid: Unique job UUID (generated if not provided)
            entity_id: Optional domain entity identifier for conflict detection

        Returns:
            int: job.id, or None if no Job model
        """
        allowed_modes = {"sync", "queued", "scheduler"}
        if execution_mode not in allowed_modes:
            allowed = ", ".join(sorted(allowed_modes))
            raise ValueError(
                f"execution_mode must be one of {allowed}; got {execution_mode!r}."
            )
        try:
            if not self.job_model:
                return None

            # Generate job_uid if not provided
            if not job_uid:
                job_uid = str(uuid.uuid4())

            # Enrich metadata with pipeline_id from ExecutionContext
            from cara.context import ExecutionContext

            enriched_metadata = dict(metadata or {})
            pipeline_id = ExecutionContext.get_job_id()
            if pipeline_id:
                enriched_metadata["pipeline_id"] = pipeline_id
            enriched_metadata["execution_mode"] = execution_mode

            job_record = self.job_model.create(
                {
                    "public_id": self.job_model.generate_public_id(),
                    "name": job_name,
                    "job_class": job_class,
                    "payload": payload or {},
                    "queue": queue,
                    "available_at": pendulum.now("UTC"),
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
            Log.warning("Failed to create job record: %s", str(e))
            return None

    def update_job_status(self, job_id: int, status: str) -> None:
        """
        Update job record status.

        Args:
            job_id: Job ID to update
            status: New status (pending, processing, completed, failed)
        """
        self._update_job_status(job_id, status, strict=False)

    def update_job_status_strict(self, job_id: int, status: str) -> None:
        """Persist an AMQP settlement fence or fail without ambiguity."""
        self._update_job_status(job_id, status, strict=True)

    def require_job_status_strict(self, job_id: int, status: str) -> None:
        """Verify durable workflow state without mutating it."""
        if not self.job_model:
            raise RuntimeError(
                "JobTracker requires an injected job model for AMQP settlement."
            )
        job_record = self.job_model.find(job_id)
        if not job_record:
            raise RuntimeError(f"Tracked queue job {job_id} does not exist.")
        if str(job_record.status) != str(status):
            raise RuntimeError(
                f"Tracked queue job {job_id} is {job_record.status!r}; "
                f"expected {status!r}."
            )

    def ensure_retry_progress_strict(self, job_id: int) -> None:
        """Accept monotonic retry progress and repair only stale ``pending``.

        The retry source is terminal once its child delivery commits. A source
        redelivery may therefore observe the shared tracker after that child
        already moved to processing or terminal state. Exact equality with the
        intermediate ``retrying`` value would poison-loop the broker message.
        """
        if not self.job_model:
            raise RuntimeError(
                "JobTracker requires an injected job model for AMQP settlement."
            )

        accepted = {
            str(getattr(self.job_model, "STATUS_RETRYING", "retrying")),
            str(getattr(self.job_model, "STATUS_PROCESSING", "processing")),
            str(getattr(self.job_model, "STATUS_COMPLETED", "completed")),
            str(getattr(self.job_model, "STATUS_SUCCESS", "success")),
            str(getattr(self.job_model, "STATUS_FAILED", "failed")),
            str(getattr(self.job_model, "STATUS_CANCELLED", "cancelled")),
        }
        pending = str(getattr(self.job_model, "STATUS_PENDING", "pending"))
        job_record = self.job_model.find(job_id)
        if not job_record:
            raise RuntimeError(f"Tracked queue job {job_id} does not exist.")
        current = str(job_record.status)
        if current in accepted:
            return
        if current != pending:
            raise RuntimeError(
                f"Tracked queue job {job_id} is {current!r}; "
                "expected monotonic retry progress."
            )

        self.job_model.where("id", job_id).where("status", pending).update(
            {
                "status": getattr(
                    self.job_model,
                    "STATUS_RETRYING",
                    "retrying",
                ),
                "updated_at": pendulum.now("UTC"),
            }
        )
        persisted = self.job_model.find(job_id)
        if not persisted or str(persisted.status) not in accepted:
            raise RuntimeError(
                f"Tracked queue job {job_id} did not converge to retry progress."
            )

    def _success_statuses(self) -> set[str]:
        """Statuses that mean "the job finished its work".

        ``completed`` (worker settlement) and ``success`` (Trackable hook) are
        the same outcome under two names — collapsing that duality is a
        separate change, so read BOTH here rather than silently under-counting
        one of the two writers.
        """
        return {
            str(getattr(self.job_model, "STATUS_COMPLETED", "completed")),
            str(getattr(self.job_model, "STATUS_SUCCESS", "success")),
        }

    def _transition(self, job_record, status: str, *, now=None) -> None:
        """Apply a lifecycle status and stamp its timestamp.

        SINGLE OWNER of the status → timestamp mapping for the ``job`` table.
        The lifecycle vocabulary is ``started_at`` / ``completed_at``; no other
        code path may write those columns. Callers still ``save()`` so they can
        batch other field writes into the same round-trip.

        Args:
            job_record: Job model instance to mutate (NOT persisted here)
            status: New status value
            now: Override the stamped instant (tests / replay)
        """
        moment = now or pendulum.now("UTC")
        job_record.status = status

        if status == str(getattr(self.job_model, "STATUS_PROCESSING", "processing")):
            job_record.started_at = moment
        elif status in self._terminal_statuses():
            job_record.completed_at = moment

    def _terminal_statuses(self) -> set[str]:
        """Statuses that end the job's run and therefore stamp ``completed_at``."""
        return self._success_statuses() | {
            str(getattr(self.job_model, "STATUS_FAILED", "failed")),
        }

    def _update_job_status(
        self,
        job_id: int,
        status: str,
        *,
        strict: bool,
    ) -> None:
        if not self.job_model:
            if strict:
                raise RuntimeError(
                    "JobTracker requires an injected job model for AMQP settlement."
                )
            return

        job_record = self.job_model.find(job_id)
        if not job_record:
            if strict:
                raise RuntimeError(f"Tracked queue job {job_id} does not exist.")
            return

        self._transition(job_record, status)
        job_record.save()
        if strict:
            persisted = self.job_model.find(job_id)
            if not persisted or getattr(persisted, "status", None) != status:
                raise RuntimeError(
                    f"Tracked queue job {job_id} did not persist {status!r} status."
                )

    def is_job_completed(self, job_id: int) -> bool:
        """Return the durable completion fence for stale AMQP redelivery."""
        if not self.job_model:
            raise RuntimeError(
                "JobTracker requires an injected job model for AMQP recovery."
            )
        job_record = self.job_model.find(job_id)
        if not job_record:
            raise RuntimeError(f"Tracked queue job {job_id} does not exist.")
        return str(getattr(job_record, "status", None)) in self._success_statuses()
