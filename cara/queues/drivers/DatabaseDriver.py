"""
Database Queue Driver for the Cara framework.

Modern, clean implementation for database-backed job queue management.
"""

import base64
import inspect
import json
import pickle
import time
import uuid
from typing import Any, Dict, List, Union

import pendulum

from cara.exceptions import QueueException
from cara.queues.contracts import JobCancelledException, Queue
from cara.queues.JobStateManager import get_job_state_manager
from cara.support.Console import HasColoredOutput
from cara.support.Time import parse_human_time


class DatabaseDriver(HasColoredOutput, Queue):
    """
    Database-based queue driver.

    Features:
    - Persistent job storage in database
    - Job state tracking and cancellation
    - Automatic retry with failure handling
    - Schedule support with delay parsing
    """

    driver_name = "database"

    def __init__(self, application, options: Dict[str, Any]):
        super().__init__(module="queue.database")
        self.application = application
        self.options = options

    def push(self, *jobs: Any, options: Dict[str, Any]) -> Union[str, List[str]]:
        """Push jobs to database queue and return job ID(s) for tracking."""
        merged = {**self.options, **options}
        builder = self._get_builder(merged)
        delay = merged.get("delay", "now")
        available_at = parse_human_time(delay)
        job_ids = []

        for job in jobs:
            # Generate unique job ID for tracking
            job_id = str(uuid.uuid4())
            job_ids.append(job_id)

            # Serialize payload
            payload = base64.b64encode(
                pickle.dumps(
                    {
                        "obj": job,
                        "callback": merged.get("callback", "handle"),
                        "args": merged.get("args", ()),
                        "job_id": job_id,
                    }
                )
            ).decode("utf-8")

            # Create database record
            record = {
                "name": str(job),
                "payload": payload,
                "available_at": available_at.to_datetime_string(),
                "attempts": 0,
                "queue": merged.get("queue", "default"),
                "job_id": job_id,
            }
            builder.create(record)

        return job_ids[0] if len(job_ids) == 1 else job_ids

    def consume(self, options: Dict[str, Any]) -> None:
        """Continuously fetch and process jobs from database."""
        merged = {**self.options, **options}
        table = merged.get("table")
        failed_table = merged.get("failed_table")
        attempts = int(merged.get("attempts", 1))
        poll_interval = int(merged.get("poll", 1))
        tz = merged.get("tz", "UTC")
        builder = self._get_builder(merged, table)

        while True:
            time.sleep(poll_interval)
            self.info(f"Checking for available jobs on '{table}'...")

            now = pendulum.now(tz=tz).to_datetime_string()
            jobs = (
                builder.where("queue", merged.get("queue", "default"))
                .where("available_at", "<=", now)
                .limit(10)
                .order_by("id")
                .get()
            )

            if not jobs:
                continue

            # Reserve jobs
            ids = [job["id"] for job in jobs]
            builder.where_in("id", ids).update({"reserved_at": now})

            # Process each job
            for job in jobs:
                try:
                    self._process_job(job, merged)
                    job_id = self._extract_job_id(job)
                    self._log_success(job["id"], tz, job_id)
                    builder.where("id", job["id"]).delete()

                except Exception as e:
                    self._handle_failed_job(job, merged, e)
                    attempts_done = int(job.get("attempts", 0))

                    if attempts_done + 1 < attempts and not failed_table:
                        # Retry
                        builder.where("id", job["id"]).update(
                            {"attempts": attempts_done + 1}
                        )
                    elif failed_table:
                        # Move to failed table
                        self._move_to_failed(builder.new(), job, merged, str(e), tz)
                        builder.where("id", job["id"]).delete()
                    else:
                        # Max retries reached
                        builder.where("id", job["id"]).update(
                            {"attempts": attempts_done + 1}
                        )

    def retry(self, options: Dict[str, Any]) -> None:
        """Move failed jobs back to main queue for retry."""
        merged = {**self.options, **options}
        failed_table = merged.get("failed_table")
        queue_name = merged.get("queue", "default")
        builder = self._get_builder(merged, failed_table)

        jobs = builder.where("queue", queue_name).get()
        if not jobs:
            self.info("No failed jobs found.")
            return

        # Move jobs back to main queue
        main_table = merged.get("table")
        main_builder = self._get_builder(merged, main_table)

        for job in jobs:
            record = {
                "name": job["name"],
                "payload": job["payload"],
                "available_at": pendulum.now(
                    tz=merged.get("tz", "UTC")
                ).to_datetime_string(),
                "attempts": 0,
                "queue": job["queue"],
            }
            main_builder.create(record)

        self.info(f"Added {len(jobs)} failed job(s) back to the queue")
        builder.where_in("id", [j["id"] for j in jobs]).delete()

    def chain(self, jobs: list, options: Dict[str, Any]) -> None:
        """Chain jobs by scheduling sequentially with incremental delay."""
        if not jobs:
            return

        delay_seconds = 0
        for job in jobs:
            self.push(
                job,
                options={**options, "delay": f"{delay_seconds} seconds"},
            )
            delay_seconds += 1

    def batch(self, *jobs: Any, options: Dict[str, Any]) -> None:
        """Batch push: push all jobs at once."""
        self.push(*jobs, options=options)

    def schedule(self, job: Any, when: Any, options: Dict[str, Any]) -> None:
        """Schedule job for future execution."""
        merged = {**self.options, **options}
        available_at = parse_human_time(when)
        builder = self._get_builder(merged)

        payload = base64.b64encode(
            pickle.dumps(
                {
                    "obj": job,
                    "callback": merged.get("callback", "handle"),
                    "args": merged.get("args", ()),
                }
            )
        ).decode("utf-8")

        record = {
            "name": str(job),
            "payload": payload,
            "available_at": available_at.to_datetime_string(),
            "attempts": 0,
            "queue": merged.get("queue", "default"),
        }
        builder.create(record)

    def _get_builder(self, opts: Dict[str, Any], table: str = None):
        """Get database query builder for specified table."""
        tbl = table or opts.get("table")
        return self.application.make("DB").query(opts.get("connection")).table(tbl)

    def _process_job(self, job: Dict[str, Any], opts: Dict[str, Any]):
        """Unpickle payload, instantiate job, and execute callback."""
        job_state_manager = get_job_state_manager()
        job_db_id = str(job["id"])

        # Update job status to processing
        self._update_job_status(
            job_db_id,
            "processing",
            {"started_at": pendulum.now().to_datetime_string()},
        )

        # Unpickle payload
        try:
            decoded_payload = base64.b64decode(job["payload"])
            data = pickle.loads(decoded_payload)
        except Exception as e:
            self._update_job_status(job_db_id, "failed", {"error": str(e)})
            raise QueueException(f"Invalid payload for job id {job['id']}: {e}")

        raw = data.get("obj")
        callback = data.get("callback", "handle")
        init_args = data.get("args", ())

        # Instantiate if raw is a class
        if inspect.isclass(raw):
            if hasattr(self.application, "make") and not init_args:
                try:
                    instance = self.application.make(raw)
                except Exception:
                    instance = raw(*init_args)
            else:
                instance = raw(*init_args)
        else:
            instance = raw

        # Update job_class in database
        job_class_name = instance.__class__.__name__ if instance else raw.__name__
        self._update_job_status(job_db_id, "processing", {"job_class": job_class_name})

        # Set up job tracking
        if hasattr(instance, "set_tracking_id"):
            instance.set_tracking_id(job_db_id)

            # Register job context for cancellation
            if hasattr(instance, "get_cancellation_context"):
                try:
                    context = instance.get_cancellation_context()
                    job_state_manager.register_job(job_db_id, context)
                    self._update_job_status(
                        job_db_id, "processing", {"context": context}
                    )
                except Exception:
                    pass

        try:
            # Execute job callback
            method_to_call = getattr(instance, callback, None)
            if not callable(method_to_call):
                raise QueueException(
                    f"Callback '{callback}' not found on job object {instance!r}"
                )

            result = method_to_call()

            # Job completed successfully
            self._update_job_status(
                job_db_id,
                "completed",
                {"completed_at": pendulum.now().to_datetime_string()},
            )

            # Call completion handler if available
            if hasattr(instance, "on_job_complete"):
                instance.on_job_complete()

            return result

        except JobCancelledException as e:
            # Job was cancelled
            self._update_job_status(
                job_db_id,
                "cancelled",
                {
                    "cancelled_at": pendulum.now().to_datetime_string(),
                    "cancel_reason": str(e),
                },
            )
            self.info(f"Job {job_db_id} was cancelled: {e}")
            if hasattr(instance, "unregister_job"):
                instance.unregister_job()
            return

        except Exception as e:
            # Job failed
            self._update_job_status(
                job_db_id,
                "failed",
                {
                    "failed_at": pendulum.now().to_datetime_string(),
                    "error": str(e),
                },
            )
            if hasattr(instance, "unregister_job"):
                instance.unregister_job()
            raise

    def _update_job_status(self, job_id: str, status: str, metadata: dict = None):
        """Update job status and metadata in database."""
        try:
            update_data = {"status": status}

            if metadata:
                # Merge with existing metadata
                current_job = self._get_builder({}).where("id", job_id).first()
                if current_job:
                    current_metadata = current_job.get("metadata", {})
                    if isinstance(current_metadata, str):
                        try:
                            current_metadata = json.loads(current_metadata)
                        except:
                            current_metadata = {}
                    elif not isinstance(current_metadata, dict):
                        current_metadata = {}

                    current_metadata.update(metadata)
                    update_data["metadata"] = current_metadata
                else:
                    update_data["metadata"] = metadata

            # Add timestamp updates
            if status == "processing" and metadata and "started_at" in metadata:
                update_data["started_at"] = metadata["started_at"]
            elif status == "completed" and metadata and "completed_at" in metadata:
                update_data["completed_at"] = metadata["completed_at"]
            elif status == "cancelled" and metadata and "cancelled_at" in metadata:
                update_data["cancelled_at"] = metadata["cancelled_at"]

            # Update job_class if provided
            if metadata and "job_class" in metadata:
                update_data["job_class"] = metadata["job_class"]

            self._get_builder({}).where("id", job_id).update(update_data)

        except Exception:
            # Don't fail job processing if status update fails
            pass

    def _handle_failed_job(
        self,
        job: Dict[str, Any],
        opts: Dict[str, Any],
        exception: Exception,
    ):
        """Handle failed job execution."""
        self.danger(
            f"[{job['id']}][{pendulum.now(tz=opts.get('tz', 'UTC')).to_datetime_string()}] "
            f"Job Failed: {exception}"
        )

        # Attempt to call failed() on the instance
        try:
            decoded_payload = base64.b64decode(job["payload"])
            data = pickle.loads(decoded_payload)
        except Exception:
            return

        raw = data.get("obj")
        init_args = data.get("args", ())

        # Instantiate again
        if inspect.isclass(raw):
            if hasattr(self.application, "make") and not init_args:
                try:
                    instance = self.application.make(raw)
                except Exception:
                    instance = raw(*init_args)
            else:
                instance = raw(*init_args)
        else:
            instance = raw

        if hasattr(instance, "failed"):
            try:
                instance.failed(job, str(exception))
            except Exception as inner:
                self.danger(f"Exception in failed(): {inner}")

    def _move_to_failed(
        self,
        builder,
        job: Dict[str, Any],
        opts: Dict[str, Any],
        exception: str,
        tz: str,
    ):
        """Move failed job to failed table."""
        builder.table(opts.get("failed_table")).create(
            {
                "driver": DatabaseDriver.driver_name,
                "queue": job["queue"],
                "name": job["name"],
                "connection": opts.get("connection"),
                "created_at": pendulum.now(tz=tz).to_datetime_string(),
                "exception": exception,
                "payload": job["payload"],
                "failed_at": pendulum.now(tz=tz).to_datetime_string(),
            }
        )

    def _log_success(self, job_id: int, tz: str, job_id_from_payload: str) -> None:
        """Log successful job completion."""
        self.success(
            f"[{job_id}][{pendulum.now(tz=tz).to_datetime_string()}] "
            f"Job Successfully Processed (Job ID: {job_id_from_payload})"
        )

    def _extract_job_id(self, job: Dict[str, Any]) -> str:
        """Extract job ID from payload."""
        decoded_payload = base64.b64decode(job["payload"])
        data = pickle.loads(decoded_payload)
        return data.get("job_id", "unknown")
