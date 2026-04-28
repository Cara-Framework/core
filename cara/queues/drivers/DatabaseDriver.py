"""
Database Queue Driver for the Cara framework.

Modern, clean implementation for database-backed job queue management.
"""

import base64
import json
import pickle
import re
import time
import uuid
from typing import Any, Dict, List, Union, Optional

import pendulum

from cara.exceptions import QueueException
from cara.queues.contracts import JobCancelledException, Queue
from cara.queues.JobStateManager import get_job_state_manager
from cara.queues.job_instantiation import instantiate_job
from cara.support.Console import HasColoredOutput
from cara.support.Time import parse_human_time


def _release_unique_lock_if_any(instance) -> None:
    """Release the UniqueJob lock for a finished job, if any.

    Centralised so every driver's job-execution path can call the
    same helper without re-importing UniqueJob inside a hot loop.
    Defensive: a job that doesn't implement ``UniqueJob``, or whose
    ``unique_id()`` raises, must not crash the worker.
    """
    if instance is None:
        return
    try:
        from cara.queues.contracts import UniqueJob

        if isinstance(instance, UniqueJob):
            UniqueJob.release_unique_lock(instance.unique_id())
    except Exception:
        pass


def _dispatch_batch_completion(instance, exception=None) -> None:
    """Forward batch lifecycle to the Batch helper.

    Ensures every batched job's ``then()`` / ``catch()`` callbacks
    fire without the job author having to call ``batch_completed``
    by hand. Without this wiring, ``Batch().then(...)`` was
    effectively dead code — the counter never reached zero so the
    callback never ran.
    """
    if instance is None:
        return
    try:
        from cara.queues.Batch import auto_dispatch_batch_completion

        auto_dispatch_batch_completion(instance, exception)
    except Exception:
        pass


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
        """Continuously fetch and process jobs from database.

        Two correctness fixes vs. the previous implementation:

        1. ``SELECT ... FOR UPDATE SKIP LOCKED`` claims rows
           atomically inside Postgres / MySQL 8+ instead of doing a
           plain SELECT followed by per-row CAS UPDATEs. Under high
           worker concurrency the old approach burned CPU on
           contended UPDATEs that all hit the same candidate set;
           with SKIP LOCKED the broker hands each worker a disjoint
           batch of rows in O(1).

        2. A periodic visibility-timeout reaper resets
           ``reserved_at`` rows older than ``visibility_timeout``
           seconds (default 600). Without this, a worker that
           crashed mid-process left its reserved row pinned forever
           and the job never re-ran.
        """
        merged = {**self.options, **options}
        table = merged.get("table")
        failed_table = merged.get("failed_table")
        attempts = int(merged.get("attempts", 1))
        poll_interval = int(merged.get("poll", 1))
        tz = merged.get("tz", "UTC")
        visibility_timeout = int(merged.get("visibility_timeout", 600))
        builder = self._get_builder(merged, table)

        last_reaper_run = 0.0
        reaper_interval = max(30, int(merged.get("reaper_interval", 60)))

        while True:
            time.sleep(poll_interval)
            self.info(f"Checking for available jobs on '{table}'...")

            # Visibility-timeout reaper: free any row whose
            # ``reserved_at`` is older than the cutoff. Runs at most
            # once per ``reaper_interval`` seconds so it doesn't
            # dominate the polling loop.
            now_ts = time.time()
            if now_ts - last_reaper_run > reaper_interval:
                self._reap_stuck_reservations(merged, table, visibility_timeout, tz)
                last_reaper_run = now_ts

            claimed_jobs = self._claim_batch(merged, table, tz)
            if not claimed_jobs:
                continue

            # Process each job
            for job in claimed_jobs:
                try:
                    self._process_job(job, merged)
                    job_id = self._extract_job_id(job)
                    self._log_success(job["id"], tz, job_id)
                    builder.where("id", job["id"]).delete()

                except Exception as e:
                    self._handle_failed_job(job, merged, e)
                    attempts_done = int(job.get("attempts", 0))

                    if attempts_done + 1 < attempts and not failed_table:
                        # Retry with exponential backoff delay
                        # Delay: 2^attempt * base_delay (5s, 10s, 20s, 40s...)
                        base_delay = int(merged.get("retry_base_delay", 5))
                        backoff_seconds = base_delay * (2 ** attempts_done)
                        retry_at = pendulum.now(tz=tz).add(seconds=backoff_seconds)
                        builder.where("id", job["id"]).update(
                            {
                                "attempts": attempts_done + 1,
                                "available_at": retry_at.to_datetime_string(),
                            }
                        )
                    elif attempts_done + 1 >= attempts and failed_table:
                        # Move to failed table (exhausted retries)
                        self._move_to_failed(builder.new(), job, merged, str(e), tz)
                        builder.where("id", job["id"]).delete()
                    elif failed_table:
                        # Still has retries but failed_table configured — retry with backoff
                        base_delay = int(merged.get("retry_base_delay", 5))
                        backoff_seconds = base_delay * (2 ** attempts_done)
                        retry_at = pendulum.now(tz=tz).add(seconds=backoff_seconds)
                        builder.where("id", job["id"]).update(
                            {
                                "attempts": attempts_done + 1,
                                "available_at": retry_at.to_datetime_string(),
                            }
                        )
                    else:
                        # Max retries reached, no failed_table — leave in place
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

    def _get_builder(self, opts: Dict[str, Any], table: Optional[str] = None):
        """Get database query builder for specified table."""
        tbl = table or opts.get("table")
        return self.application.make("DB").query(opts.get("connection")).table(tbl)

    def _claim_batch(
        self,
        merged: Dict[str, Any],
        table: str,
        tz: str,
    ) -> list:
        """Claim a batch of jobs atomically using SKIP LOCKED.

        Postgres ≥ 9.5 and MySQL ≥ 8.0 support ``FOR UPDATE SKIP
        LOCKED``, which is the canonical pattern for "queue table"
        workloads. Each worker SELECTs its claim window inside a
        transaction with a row-level lock that other workers skip
        instead of waiting on, then UPDATEs ``reserved_at`` and
        commits — losers see different rows, no two workers ever
        contend for the same row.

        Drivers that don't support SKIP LOCKED (SQLite, MSSQL <
        2019) fall back to the legacy CAS-UPDATE path.
        """
        connection_name = merged.get("connection")
        queue = merged.get("queue", "default")
        now = pendulum.now(tz=tz).to_datetime_string()
        batch_size = int(merged.get("batch_size", 10))

        db = self.application.make("DB")
        try:
            # Detect driver via the resolver — Postgres / MySQL get
            # SKIP LOCKED, others get the CAS fallback.
            connection_info = db.get_connection_info(connection_name)
            driver = (connection_info.get("driver") or "").lower()
        except Exception:
            driver = ""

        if driver in ("postgres", "postgresql", "mysql"):
            return self._claim_batch_skip_locked(
                merged, table, queue, now, batch_size, driver, connection_name
            )

        # Fallback path — legacy CAS UPDATE.
        builder = self._get_builder(merged, table)
        jobs = (
            builder.where("queue", queue)
            .where("available_at", "<=", now)
            .where_null("reserved_at")
            .limit(batch_size)
            .order_by("id")
            .get()
        )
        if not jobs:
            return []
        claimed = []
        for job in jobs:
            affected = (
                builder.new()
                .table(table)
                .where("id", job["id"])
                .where_null("reserved_at")
                .update({"reserved_at": now})
            )
            try:
                won = bool(int(affected)) if affected is not None else False
            except (TypeError, ValueError):
                won = bool(affected)
            if won:
                claimed.append(job)
        return claimed

    def _claim_batch_skip_locked(
        self,
        merged: Dict[str, Any],
        table: str,
        queue: str,
        now: str,
        batch_size: int,
        driver: str,
        connection_name: Optional[str],
    ) -> list:
        """Claim a batch via SELECT ... FOR UPDATE SKIP LOCKED.

        Wrapped in a transaction so the row-level locks are held for
        exactly the duration of the claim, not longer. Returns the
        claimed job rows (including their decoded payload).
        """
        # Quote the table name conservatively — most callers pass a
        # config-supplied identifier, so the risk surface is small,
        # but we still defend against weird configurations.
        if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", table):
            raise QueueException(f"Invalid queue table identifier: {table!r}")

        select_sql = (
            f"SELECT id, name, payload, attempts, queue, available_at, reserved_at "
            f"FROM {table} "
            f"WHERE queue = %s AND available_at <= %s AND reserved_at IS NULL "
            f"ORDER BY id ASC "
            f"LIMIT %s "
            f"FOR UPDATE SKIP LOCKED"
        )
        update_sql = (
            f"UPDATE {table} SET reserved_at = %s WHERE id = ANY(%s)"
            if driver in ("postgres", "postgresql")
            else f"UPDATE {table} SET reserved_at = %s WHERE id IN %s"
        )

        db = self.application.make("DB")
        try:
            with db.transaction(connection_name):
                rows = db.select(
                    select_sql, [queue, now, batch_size], connection_name
                ) or []
                if not rows:
                    return []
                ids = [r["id"] for r in rows]
                if driver in ("postgres", "postgresql"):
                    db.statement(update_sql, [now, ids], connection_name)
                else:
                    db.statement(update_sql, [now, tuple(ids)], connection_name)
                # Stamp the in-memory rows with the claim time so the
                # caller can use them like a freshly-fetched record.
                for r in rows:
                    r["reserved_at"] = now
                return rows
        except Exception as e:
            self.danger(f"SKIP LOCKED claim failed: {e}")
            return []

    def _reap_stuck_reservations(
        self,
        merged: Dict[str, Any],
        table: str,
        visibility_timeout: int,
        tz: str,
    ) -> None:
        """Free reserved rows whose worker died mid-process.

        A reservation older than ``visibility_timeout`` seconds is
        treated as evidence the worker holding it is gone (crashed,
        OOM-killed, deploy rotation that didn't drain). Reset
        ``reserved_at`` to NULL so the row is re-pickable on the
        next poll.
        """
        cutoff = (
            pendulum.now(tz=tz)
            .subtract(seconds=visibility_timeout)
            .to_datetime_string()
        )
        try:
            builder = self._get_builder(merged, table)
            freed = (
                builder.where("reserved_at", "<", cutoff)
                .where_not_null("reserved_at")
                .update({"reserved_at": None})
            )
            if freed:
                self.info(
                    f"Reaped {freed} stuck reservation(s) on '{table}' "
                    f"(older than {visibility_timeout}s)"
                )
        except Exception as e:
            # The reaper is best-effort — never let it kill the
            # worker loop.
            self.danger(f"Reaper sweep failed: {e}")

    def _process_job(self, job: Dict[str, Any], opts: Dict[str, Any]):
        """Unpickle payload, instantiate job, and execute callback."""
        job_state_manager = get_job_state_manager()
        job_db_id = str(job["id"])

        # Update job status to processing
        self._update_job_status(
            job_db_id,
            "processing",
            {"started_at": pendulum.now("UTC").to_datetime_string()},
        )

        # Unpickle payload
        try:
            decoded_payload = base64.b64decode(job["payload"])
            data = pickle.loads(decoded_payload)
        except Exception as e:
            self._update_job_status(job_db_id, "failed", {"error": str(e)})
            raise QueueException(f"Invalid payload for job id {job['id']}: {e}") from e

        raw = data.get("obj")
        callback = data.get("callback", "handle")
        init_args = data.get("args", ())

        instance = instantiate_job(self.application, raw, init_args)

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
                except Exception as exc:
                    import logging
                    logging.getLogger("cara.queue.database").warning(
                        "Failed to register cancellation context for job %s: %s",
                        job_db_id, exc,
                    )

        try:
            # Execute job callback
            method_to_call = getattr(instance, callback, None)
            if not callable(method_to_call):
                raise QueueException(
                    f"Callback '{callback}' not found on job object {instance!r}"
                )

            if hasattr(self.application, "call"):
                result = self.application.call(method_to_call)
            else:
                result = method_to_call()

            # Job completed successfully
            self._update_job_status(
                job_db_id,
                "completed",
                {"completed_at": pendulum.now("UTC").to_datetime_string()},
            )

            # Call completion handler if available
            if hasattr(instance, "on_job_complete"):
                instance.on_job_complete()

            # Batch lifecycle — fires ``batch_completed`` on the
            # job's batch so the pending counter actually moves and
            # ``Batch.then()`` callbacks fire when the last sibling
            # finishes.
            _dispatch_batch_completion(instance, None)

            return result

        except JobCancelledException as e:
            # Job was cancelled
            self._update_job_status(
                job_db_id,
                "cancelled",
                {
                    "cancelled_at": pendulum.now("UTC").to_datetime_string(),
                    "cancel_reason": str(e),
                },
            )
            self.info(f"Job {job_db_id} was cancelled: {e}")
            if hasattr(instance, "unregister_job"):
                instance.unregister_job()
            # Treat cancellation as a batch failure so ``catch()`` /
            # the failed counter still see it.
            _dispatch_batch_completion(instance, e)
            return

        except Exception as e:
            # Job failed
            self._update_job_status(
                job_db_id,
                "failed",
                {
                    "failed_at": pendulum.now("UTC").to_datetime_string(),
                    "error": str(e),
                },
            )
            if hasattr(instance, "unregister_job"):
                instance.unregister_job()
            _dispatch_batch_completion(instance, e)
            raise
        finally:
            # Always release the UniqueJob lock so subsequent
            # legitimate dispatches for the same ``unique_id`` can
            # proceed. Without this release the lock survives until
            # ``unique_for`` (default 1h) expires, silently dropping
            # every retry / re-dispatch in the meantime.
            _release_unique_lock_if_any(instance)

    def _update_job_status(self, job_id: str, status: str, metadata: Optional[dict] = None):
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
                        except Exception:
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

        instance = instantiate_job(self.application, raw, init_args)

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
