"""
Queue Worker Command for the Cara framework.

This module provides a CLI command to process jobs from the queue with enhanced UX.
"""

import asyncio
import builtins
import concurrent.futures
import inspect
import os
import pickle
import threading
import time
from typing import Any, Dict, Optional

from cara.commands import CommandBase
from cara.commands.AutoReloadMixin import AutoReloadMixin
from cara.configuration import config
from cara.decorators import command
from cara.facades import Log
from cara.queues.contracts import UniqueJob


class AMQPConnectionManager:
    """Manages AMQP connections for queue workers (Single Responsibility)."""

    def __init__(self, config_func):
        self.config = config_func
        self.connection = None

    def ensure_connection(self) -> bool:
        """Ensure AMQP connection is alive."""
        try:
            if self.connection is None or self.connection.is_closed:
                self.connection = self._create_connection()
            return True
        except Exception as e:
            try:
                from cara.facades import Log

                Log.error(f"Failed to connect to RabbitMQ: {e}")
            except ImportError:
                pass
            return False

    def _create_connection(self):
        """Create new AMQP connection."""
        import pika

        credentials = pika.PlainCredentials(
            self.config("queue.drivers.amqp.username"),
            self.config("queue.drivers.amqp.password"),
        )
        parameters = pika.ConnectionParameters(
            host=self.config("queue.drivers.amqp.host"),
            port=self.config("queue.drivers.amqp.port", 5672),
            virtual_host=self.config("queue.drivers.amqp.vhost", "/"),
            credentials=credentials,
        )
        return pika.BlockingConnection(parameters)

    def create_channel(self):
        """Create fresh channel for queue operations."""
        if not self.ensure_connection():
            return None
        return self.connection.channel()

    def close(self):
        """Clean up connection."""
        if self.connection and not self.connection.is_closed:
            try:
                self.connection.close()
            except Exception:
                pass


class JobProcessor:
    """Processes individual jobs from queue messages (Single Responsibility)."""

    # Class-level constants for job execution
    DEFAULT_JOB_TIMEOUT = 3600  # 1 hour in seconds
    MAX_PAYLOAD_SIZE = 50 * 1024 * 1024  # 50 MB

    @staticmethod
    def _execute_job_with_timeout(method_to_call, init_args, timeout_seconds):
        """Execute job with timeout enforcement using ThreadPoolExecutor."""
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        future = executor.submit(method_to_call, *init_args)
        try:
            future.result(timeout=timeout_seconds)
        finally:
            executor.shutdown(wait=False)

    @staticmethod
    def _execute_async_job_with_timeout(method_to_call, init_args, timeout_seconds):
        """Execute async job with timeout enforcement."""
        try:
            asyncio.run(method_to_call(*init_args))
        except asyncio.TimeoutError as e:
            raise TimeoutError(f"Async job exceeded timeout of {timeout_seconds}s") from e

    @staticmethod
    def _should_retry_job(msg, attempts_count):
        """Determine if job should be retried based on attempt configuration."""
        if not msg or "attempts" not in msg:
            return False
        max_attempts = msg.get("attempts", 1)
        current_attempt = msg.get("attempt", 1)
        return current_attempt < max_attempts

    @staticmethod
    def _nack_with_requeue(channel, method_frame, msg):
        """NACK message with requeue to put it back in queue."""
        try:
            channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=True)
            Log.info(f"↻ Job requeued for retry (attempt {msg.get('attempt', 1)}/{msg.get('attempts', 1)})")
        except Exception as e:
            Log.error(f"Failed to NACK with requeue: {e}")
            # Fallback: ACK to prevent infinite loop
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)

    @staticmethod
    def _ack_to_dlq(channel, method_frame, msg, error_msg):
        """ACK message and log to dead letter pattern for failed jobs."""
        try:
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            # Log to DLQ-style pattern for monitoring
            dlq_queue = f"{msg.get('queue', 'unknown')}.dlq"
            job_id = msg.get("job_id", "unknown")
            Log.error(f"💀 Job moved to DLQ: {job_id} | Queue: {dlq_queue} | Error: {error_msg}")
        except Exception as e:
            Log.error(f"Failed to ACK message: {e}")

    @staticmethod
    def process_message(channel, method_frame, body) -> bool:
        """Process a single queue message and return success status."""
        # CRITICAL FIX #4: Validate payload size before unpickling
        if len(body) > JobProcessor.MAX_PAYLOAD_SIZE:
            Log.error(f"❌ Payload exceeds max size ({len(body)} > {JobProcessor.MAX_PAYLOAD_SIZE})")
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            return False

        # Resolve app and tracker outside try block for exception handler access
        app_instance = builtins.app() if hasattr(builtins, "app") else None
        tracker = None
        if app_instance and app_instance.has("JobTracker"):
            tracker = app_instance.make("JobTracker")

        msg = None
        instance = None
        db_job_id = None

        try:
            # Unpickle message
            msg = pickle.loads(body)
            instance = msg.get("obj")
            callback = msg.get("callback", "handle")
            init_args = msg.get("args", ())
            db_job_id = msg.get("db_job_id")
            job_timeout = msg.get("timeout", JobProcessor.DEFAULT_JOB_TIMEOUT)

            # Set up job tracking
            job_id = msg.get("job_id")
            if hasattr(instance, "set_tracking_id") and job_id:
                instance.set_tracking_id(job_id)

            if db_job_id and hasattr(instance, "__dict__"):
                instance._db_job_id = db_job_id

            # Start tracking (Trackable trait tracks entity_id)
            if hasattr(instance, "_start_tracking"):
                instance._start_tracking()

            # Update job table status to processing
            if tracker and db_job_id:
                tracker.update_job_status(db_job_id, "processing")

            # Mark as processing in unified job table
            if hasattr(instance, "_mark_processing"):
                instance._mark_processing()

            # CRITICAL FIX #1: Execute job with timeout enforcement
            method_to_call = getattr(instance, callback, None)
            if callable(method_to_call):
                if inspect.iscoroutinefunction(method_to_call):
                    # Async job with timeout
                    try:
                        asyncio.run(asyncio.wait_for(
                            method_to_call(*init_args),
                            timeout=job_timeout
                        ))
                    except asyncio.TimeoutError as e:
                        raise TimeoutError(f"Job exceeded timeout of {job_timeout}s") from e
                else:
                    # Sync job with timeout using ThreadPoolExecutor
                    JobProcessor._execute_job_with_timeout(
                        method_to_call, init_args, job_timeout
                    )

            # Mark success in unified job table
            if hasattr(instance, "_mark_success"):
                instance._mark_success()

            # Update job table status to completed
            if tracker and db_job_id:
                tracker.update_job_status(db_job_id, "completed")

            # Release UniqueJob lock if applicable
            if isinstance(instance, UniqueJob):
                UniqueJob.release_unique_lock(instance.unique_id())

            # Acknowledge message
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            return True

        except TimeoutError as timeout_error:
            Log.error(f"⏱️ Job timeout: {str(timeout_error)}")

            # Mark as failed in unified job table
            if instance and hasattr(instance, "_mark_failed"):
                instance._mark_failed(str(timeout_error), should_retry=True)

            # Update job table status to failed
            if tracker and db_job_id:
                tracker.update_job_status(db_job_id, "failed")

            # CRITICAL FIX #2: Handle retry logic for timeout failures
            if msg and JobProcessor._should_retry_job(msg, msg.get("attempt", 1)):
                JobProcessor._nack_with_requeue(channel, method_frame, msg)
            else:
                JobProcessor._ack_to_dlq(channel, method_frame, msg, str(timeout_error))

            # Release UniqueJob lock on timeout failure
            if instance and isinstance(instance, UniqueJob):
                UniqueJob.release_unique_lock(instance.unique_id())

            # Try to call failed method
            try:
                if instance and hasattr(instance, "failed"):
                    failed_method = getattr(instance, "failed")
                    if inspect.iscoroutinefunction(failed_method):
                        asyncio.run(failed_method(msg, str(timeout_error)))
                    else:
                        failed_method(msg, str(timeout_error))
            except Exception:
                pass

            return True

        except Exception as job_error:
            Log.error(f"❌ Job failed: {str(job_error)}")

            # Mark as failed in unified job table
            if instance and hasattr(instance, "_mark_failed"):
                instance._mark_failed(str(job_error), should_retry=False)

            # Update job table status to failed
            if tracker and db_job_id:
                tracker.update_job_status(db_job_id, "failed")

            # CRITICAL FIX #2: Implement smart NACK/DLQ handling
            if msg and JobProcessor._should_retry_job(msg, msg.get("attempt", 1)):
                # Requeue for retry if attempts remain
                JobProcessor._nack_with_requeue(channel, method_frame, msg)
            else:
                # Move to DLQ if no retries or attempts exhausted
                JobProcessor._ack_to_dlq(channel, method_frame, msg, str(job_error))

            # Release UniqueJob lock on failure
            if instance and isinstance(instance, UniqueJob):
                UniqueJob.release_unique_lock(instance.unique_id())

            # Try to call failed method
            try:
                if instance and hasattr(instance, "failed"):
                    failed_method = getattr(instance, "failed")
                    if inspect.iscoroutinefunction(failed_method):
                        asyncio.run(failed_method(msg, str(job_error)))
                    else:
                        failed_method(msg, str(job_error))
            except Exception:
                pass

            return True  # Still processed (failed gracefully)


@command(
    name="queue:work",
    help="Run the queue worker to consume jobs with enhanced UX.",
    options={
        "--driver=?": "Queue driver to use (overrides default configuration)",
        "--queue=?": "Queue name(s) to process (comma-separated for priority: high,default,low)",
        "--timeout=?": "Poll timeout in seconds (default: 5)",
        "--max-jobs=?": "Maximum number of jobs to process before stopping",
        "--max-time=?": "Maximum runtime in seconds before stopping",
        "--reload": "Enable auto-reload on file changes",
    },
)
class QueueWorkCommand(AutoReloadMixin, CommandBase):
    """Run queue worker with enhanced monitoring and graceful shutdown."""

    def __init__(self, application=None):
        super().__init__(application)
        self.start_time = None
        self.jobs_processed = 0
        self.jobs_failed = 0
        self.memory_limit_bytes = 512 * 1024 * 1024  # 512 MB default
        # Queues that don't exist yet — skipped until the retry TTL expires.
        # A passive queue_declare for a missing queue closes the channel
        # (RabbitMQ returns 404) which triggers expensive reconnects every
        # poll tick. Cache the miss to avoid the loop and retry periodically
        # so newly-published queues are picked up.
        self._missing_queues: Dict[str, float] = {}
        self._missing_queue_retry_s: float = 30.0

    def handle(
        self,
        driver: Optional[str] = None,
        queue: Optional[str] = None,
        timeout: Optional[str] = None,
        max_jobs: Optional[str] = None,
        max_time: Optional[str] = None,
    ):
        """Handle queue worker execution with enhanced monitoring."""
        self.console.print()  # Empty line for spacing
        self.console.print("[bold #e5c07b]╭─ Queue Worker ─╮[/bold #e5c07b]")
        self.console.print()

        # Store parameters for restart
        self.store_restart_params(driver, queue, timeout, max_jobs, max_time)

        # Setup auto-reload if enabled (default: true for development)
        if self.option("reload") or config("app.debug", True):
            self.enable_auto_reload()

        # Start main worker loop
        try:
            self._run_main_loop(driver, queue, timeout, max_jobs, max_time)
        except Exception as e:
            import traceback

            self.error(f"× Worker error: {e}")
            self.error(f"× Stack trace: {traceback.format_exc()}")
        finally:
            self.cleanup_auto_reload()
            self._show_final_stats()

    def _prepare_config(
        self,
        driver: Optional[str],
        queue: Optional[str],
        timeout: Optional[str],
        max_jobs: Optional[str],
        max_time: Optional[str],
    ) -> Dict[str, Any]:
        """Prepare and validate worker configuration."""
        # Determine driver
        driver_name = driver or config("queue.default")
        if not driver_name:
            raise Exception(
                "No driver specified and no default 'queue.default' configured"
            )

        drivers = config("queue.drivers", {})
        if driver_name not in drivers:
            raise Exception(f"Driver '{driver_name}' is not configured")

        # Parse timeout
        timeout_val = 5
        if timeout:
            try:
                timeout_val = int(timeout)
                if timeout_val < 1:
                    raise ValueError("Timeout must be at least 1 second")
            except ValueError as e:
                raise Exception(f"Invalid timeout value: {e}") from e
        else:
            # Get from driver config
            driver_config = config(f"queue.drivers.{driver_name}", {})
            timeout_val = driver_config.get("poll", 5)

        # Parse limits
        max_jobs_val = None
        if max_jobs:
            try:
                max_jobs_val = int(max_jobs)
                if max_jobs_val <= 0:
                    raise ValueError("max-jobs must be positive")
            except ValueError as e:
                raise Exception(f"Invalid max-jobs value: {e}") from e

        max_time_val = None
        if max_time:
            try:
                max_time_val = int(max_time)
                if max_time_val <= 0:
                    raise ValueError("max-time must be positive")
            except ValueError as e:
                raise Exception(f"Invalid max-time value: {e}") from e

        return {
            "driver_name": driver_name,
            "queue_names": self._parse_queue_names(queue),
            "timeout": timeout_val,
            "max_jobs": max_jobs_val,
            "max_time": max_time_val,
        }

    def _parse_queue_names(self, queue: Optional[str]) -> list:
        """Parse queue names from comma-separated string with wildcard support."""
        if not queue:
            return ["default"]

        # Split by comma and clean up
        queue_patterns = [q.strip() for q in queue.split(",")]
        queue_patterns = [q for q in queue_patterns if q]  # Remove empty strings

        if not queue_patterns:
            return ["default"]

        # Expand wildcard patterns
        expanded_queues = []
        for pattern in queue_patterns:
            if "*" in pattern:
                expanded_queues.extend(self._expand_wildcard_pattern(pattern))
            else:
                expanded_queues.append(pattern)

        return expanded_queues if expanded_queues else ["default"]

    def _expand_wildcard_pattern(self, pattern: str) -> list:
        """Expand wildcard pattern to actual queue names."""
        # Standard priority levels for all queue types
        priority_levels = ["critical", "high", "default", "low"]

        if pattern.endswith(".*"):
            # Pattern like "enrichment.*" or "discovery.*"
            prefix = pattern[:-2]  # Remove ".*"
            return [f"{prefix}.{level}" for level in priority_levels]
        elif pattern.endswith("*"):
            # Pattern like "enrichment*"
            prefix = pattern[:-1]  # Remove "*"
            return [f"{prefix}.{level}" for level in priority_levels]
        else:
            # No wildcard, return as-is
            return [pattern]

    def _show_config(self, config: Dict[str, Any]):
        """Display worker configuration in ServeCommand style."""
        self.console.print("[bold #e5c07b]┌─ Configuration[/bold #e5c07b]")

        # Driver info
        self.console.print(
            f"[#e5c07b]│[/#e5c07b] [white]Driver:[/white] [bold white]{config['driver_name'].upper()}[/bold white]"
        )

        # Queue info
        queue_names = config["queue_names"]
        if len(queue_names) > 1:
            self.console.print(
                f"[#e5c07b]│[/#e5c07b] [white]Queues:[/white] [dim]{len(queue_names)} queues in priority order[/dim]"
            )
            for i, queue in enumerate(queue_names, 1):  # Show all queues
                priority_color = (
                    "#E21102"
                    if "critical" in queue
                    else "#e5c07b"
                    if "high" in queue
                    else "#30e047"
                    if "default" in queue
                    else "dim"
                )
                self.console.print(
                    f"[#e5c07b]│[/#e5c07b]   [white]{i}.[/white] [{priority_color}]{queue}[/{priority_color}]"
                )
        else:
            queue_color = (
                "#E21102"
                if "critical" in queue_names[0]
                else "#e5c07b"
                if "high" in queue_names[0]
                else "#30e047"
            )
            self.console.print(
                f"[#e5c07b]│[/#e5c07b] [white]Queue:[/white] [{queue_color}]{queue_names[0]}[/{queue_color}]"
            )

        # Timing and limits
        self.console.print(
            f"[#e5c07b]│[/#e5c07b] [white]Poll Timeout:[/white] [dim]{config['timeout']}s[/dim]"
        )

        if config.get("max_jobs"):
            self.console.print(
                f"[#e5c07b]│[/#e5c07b] [white]Max Jobs:[/white] [dim]{config['max_jobs']}[/dim]"
            )
        if config.get("max_time"):
            self.console.print(
                f"[#e5c07b]│[/#e5c07b] [white]Max Runtime:[/white] [dim]{config['max_time']}s[/dim]"
            )

        # Auto-reload status (default: enabled in development)
        from cara.configuration import config as global_config

        auto_reload = self.option("reload") or global_config("app.debug", True)
        self.console.print(
            f"[#e5c07b]│[/#e5c07b] [white]Auto-reload:[/white] [{'#30e047' if auto_reload else '#E21102'}]{'✓' if auto_reload else '×'}[/{'#30e047' if auto_reload else '#E21102'}]"
        )

        self.console.print("[#e5c07b]└─[/#e5c07b]")
        self.console.print()

    def _run_worker(self, config: Dict[str, Any]) -> None:
        """Run the queue worker with multiple queue priority support."""
        queue_names = config["queue_names"]

        self._show_worker_startup_info(queue_names)
        self.start_time = time.time()

        # Initialize connection manager and job processor
        from cara.configuration import config as global_config

        connection_manager = AMQPConnectionManager(global_config)
        job_processor = JobProcessor()

        try:
            while not self.shutdown_requested:
                job_processed = self._process_queue_cycle(
                    queue_names, connection_manager, job_processor, config
                )

                # Sleep if no jobs found
                if not job_processed:
                    time.sleep(config["timeout"])

        finally:
            connection_manager.close()

    def _check_memory_usage(self) -> bool:
        """
        Check worker memory usage and exit gracefully if limit exceeded.
        CRITICAL FIX #3: Enforce memory limit to prevent unbounded growth.
        Returns True if memory is within limits, False if exceeded.
        """
        try:
            import psutil
            process = psutil.Process(os.getpid())
            rss_bytes = process.memory_info().rss

            if rss_bytes > self.memory_limit_bytes:
                limit_mb = self.memory_limit_bytes / (1024 * 1024)
                current_mb = rss_bytes / (1024 * 1024)
                Log.warning(
                    f"⚠️ Memory limit exceeded: {current_mb:.1f}MB > {limit_mb:.1f}MB. "
                    f"Initiating graceful shutdown for supervisor restart."
                )
                self.shutdown_requested = True
                return False

            return True
        except ImportError:
            # psutil not available, fall back to /proc on Linux
            try:
                with open(f"/proc/{os.getpid()}/status", "r") as f:
                    for line in f:
                        if line.startswith("VmRSS:"):
                            rss_kb = int(line.split()[1])
                            rss_bytes = rss_kb * 1024

                            if rss_bytes > self.memory_limit_bytes:
                                limit_mb = self.memory_limit_bytes / (1024 * 1024)
                                current_mb = rss_bytes / (1024 * 1024)
                                Log.warning(
                                    f"⚠️ Memory limit exceeded: {current_mb:.1f}MB > {limit_mb:.1f}MB. "
                                    f"Initiating graceful shutdown for supervisor restart."
                                )
                                self.shutdown_requested = True
                                return False
                            break
            except Exception:
                pass

            return True

    def _show_worker_startup_info(self, queue_names: list) -> None:
        """Display worker startup information in ServeCommand style."""
        self.console.print("[bold #e5c07b]┌─ Worker Status[/bold #e5c07b]")

        if len(queue_names) > 1:
            self.console.print(
                f"[#e5c07b]│[/#e5c07b] [white]Processing:[/white] [dim]{len(queue_names)} queues in priority order[/dim]"
            )
        else:
            queue_color = (
                "#E21102"
                if "critical" in queue_names[0]
                else "#e5c07b"
                if "high" in queue_names[0]
                else "#30e047"
            )
            self.console.print(
                f"[#e5c07b]│[/#e5c07b] [white]Monitoring:[/white] [{queue_color}]{queue_names[0]}[/{queue_color}]"
            )

        self.console.print(
            "[#e5c07b]│[/#e5c07b] [white]Status:[/white] [#30e047]✓ Active - Waiting for jobs[/#30e047]"
        )

        self.console.print("[#e5c07b]└─[/#e5c07b]")
        self.console.print()

        # Simple ready message
        self.console.print("[dim]Press Ctrl+C to stop the worker[/dim]")
        self.console.print()

    def _process_queue_cycle(
        self,
        queue_names: list,
        connection_manager: AMQPConnectionManager,
        job_processor: JobProcessor,
        config: Dict[str, Any],
    ) -> bool:
        """Process one cycle through all queues in priority order."""
        # CRITICAL FIX #3: Check memory usage after each job
        if not self._check_memory_usage():
            return False  # Memory limit exceeded, signal shutdown

        for queue_name in queue_names:
            if self.shutdown_requested:
                break

            try:
                if self._process_single_queue(
                    queue_name, connection_manager, job_processor
                ):
                    # Memory check after successful job
                    self._check_memory_usage()
                    return True  # Job processed, restart from highest priority

            except Exception as e:
                self._handle_queue_error(queue_name, e, connection_manager)
                continue

        return False  # No jobs processed

    def _process_single_queue(
        self,
        queue_name: str,
        connection_manager: AMQPConnectionManager,
        job_processor: JobProcessor,
    ) -> bool:
        """Process a single queue and return True if job was processed."""
        # Skip queues we've recently seen as missing. A failed passive
        # declare closes the channel, so without this cache every poll
        # tick triggers a reconnect storm.
        now = time.time()
        missed_at = self._missing_queues.get(queue_name)
        if missed_at is not None and (now - missed_at) < self._missing_queue_retry_s:
            return False

        channel = connection_manager.create_channel()
        if not channel:
            return False

        try:
            # Passive declare: only verify queue exists without asserting
            # arguments. Publisher side owns queue creation with proper
            # x-message-ttl / dead-letter args; redeclaring here with a
            # different arg set raises PRECONDITION_FAILED (406).
            try:
                channel.queue_declare(queue=queue_name, durable=True, passive=True)
            except Exception:
                # Queue doesn't exist yet — cache the miss and retry later.
                self._missing_queues[queue_name] = now
                return False

            # Queue exists — drop from miss cache if we had marked it.
            self._missing_queues.pop(queue_name, None)

            # Non-blocking message retrieval
            method_frame, header_frame, body = channel.basic_get(queue=queue_name)

            if method_frame:
                # Process the job
                return job_processor.process_message(channel, method_frame, body)

            return False  # No message

        finally:
            # Always close channel
            try:
                channel.close()
            except Exception:
                pass

    def _handle_queue_error(
        self,
        queue_name: str,
        error: Exception,
        connection_manager: AMQPConnectionManager,
    ) -> None:
        """Handle queue processing errors."""
        error_msg = str(error)

        # Skip queues that don't exist
        if "NOT_FOUND" not in error_msg:
            if "connection" in error_msg.lower() or "closed" in error_msg.lower():
                # Connection issue, reset connection
                connection_manager.connection = None
            else:
                self.error(f"Error checking queue {queue_name}: {error_msg}")

    def _should_stop(self, config: Dict[str, Any]) -> bool:
        """Check if worker should stop due to configured limits."""
        if config["max_jobs"] and self.jobs_processed >= config["max_jobs"]:
            self.info(f"🎯 Reached maximum job limit ({config['max_jobs']})")
            return True

        if config["max_time"] and (time.time() - self.start_time) >= config["max_time"]:
            self.info(f"⏰ Reached maximum runtime ({config['max_time']} seconds)")
            return True

        return False

    def _get_runtime(self) -> str:
        """Get formatted runtime duration."""
        if not self.start_time:
            return "00:00:00"

        runtime_seconds = int(time.time() - self.start_time)
        hours, remainder = divmod(runtime_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

    def _show_final_stats(self):
        """Show enhanced worker statistics with job status breakdown."""
        total_jobs = self.jobs_processed + self.jobs_failed
        success_rate = (
            (self.jobs_processed / total_jobs * 100) if total_jobs > 0 else 100.0
        )

        self.info("\n📊 Final Worker Statistics:")
        self.info(f"   Runtime: {self._get_runtime()}")
        self.info(f"   Jobs Processed: {self.jobs_processed}")
        self.info(f"   Jobs Failed: {self.jobs_failed}")
        self.info(f"   Success Rate: {success_rate:.1f}%")

        # Show enhanced queue stats if available
        try:
            # Try to resolve Job model from container (framework agnostic)
            job_model = self._resolve_job_model()
            if job_model and hasattr(job_model, "get_queue_stats"):
                stats = job_model.get_queue_stats(self.queue_name)
                self.info(f"\n📈 Current Queue Status ({self.queue_name}):")
                self.info(f"   Pending: {stats.get('pending_jobs', 0)}")
                self.info(f"   Processing: {stats.get('processing_jobs', 0)}")
                self.info(f"   Completed: {stats.get('completed_jobs', 0)}")
                self.info(f"   Cancelled: {stats.get('cancelled_jobs', 0)}")
                self.info(f"   Failed: {stats.get('failed_jobs', 0)}")
        except Exception:
            # If Job model not available or DB error, skip enhanced stats
            pass

    def _resolve_job_model(self):
        """Resolve Job model from JobTracker."""
        import builtins

        if hasattr(builtins, "app"):
            app_instance = builtins.app()
            if app_instance and app_instance.has("JobTracker"):
                tracker = app_instance.make("JobTracker")
                return getattr(tracker, "job_model", None)
        return None

    def _run_main_loop(self, *args, **kwargs):
        """Main worker loop - called by AutoReloadMixin on restart."""
        # Use stored parameters from store_restart_params
        if hasattr(self, "_restart_params") and self._restart_params:
            driver, queue, timeout, max_jobs, max_time = self._restart_params
        else:
            driver, queue, timeout, max_jobs, max_time = (
                args if args else (None, None, None, None, None)
            )

        # Prepare config with current parameters
        try:
            worker_config = self._prepare_config(
                driver, queue, timeout, max_jobs, max_time
            )
        except Exception as e:
            self.error(f"❌ Configuration error: {e}")
            return

        # Show worker configuration
        self._show_config(worker_config)

        # Clean up connections before starting
        self._cleanup_connections_for_restart()

        # Reset counters for fresh start
        self.jobs_processed = 0
        self.jobs_failed = 0

        # Run the worker
        self._run_worker(worker_config)

    def _cleanup_connections_for_restart(self):
        """Clean up connections specifically for restart - simple and effective."""
        try:
            from cara.facades.Queue import Queue

            # Simple approach: Just clear all references without trying to close broken connections
            drivers = config("queue.drivers", {})
            for driver_name in drivers.keys():
                try:
                    driver = Queue.driver(driver_name)

                    # Just clear references - don't try to close broken connections
                    if hasattr(driver, "channel"):
                        driver.channel = None

                    if hasattr(driver, "connection"):
                        driver.connection = None

                    # Reset driver state
                    if hasattr(driver, "_connected"):
                        driver._connected = False

                except Exception:
                    continue

            # Force a small delay to let any pending operations complete
            import time

            time.sleep(0.1)

        except Exception:
            pass

    def _cleanup_watching(self):
        """Cleanup file watching resources."""
        if hasattr(self, "command_watcher") and self.command_watcher:
            try:
                self.command_watcher.shutdown()
            except Exception:
                pass
