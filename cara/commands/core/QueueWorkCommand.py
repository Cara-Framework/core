"""
Queue Worker Command for the Cara framework.

This module provides a CLI command to process jobs from the queue with enhanced UX.
"""

import importlib
import sys
import time
from typing import Any, Dict, Optional

from cara.commands import CommandBase
from cara.commands.ReloadableMixin import ReloadableMixin
from cara.configuration import config
from cara.decorators import command


@command(
    name="queue:work",
    help="Run the queue worker to consume jobs with enhanced UX.",
    options={
        "--driver=?": "Queue driver to use (overrides default configuration)",
        "--queue=?": "Specific queue name to process",
        "--timeout=?": "Poll timeout in seconds (default: 5)",
        "--max-jobs=?": "Maximum number of jobs to process before stopping",
        "--max-time=?": "Maximum runtime in seconds before stopping",
        "--reload": "Enable auto-reload on file changes",
    },
)
class QueueWorkCommand(ReloadableMixin, CommandBase):
    """Run queue worker with enhanced monitoring and graceful shutdown."""

    def __init__(self, application=None):
        super().__init__(application)
        self.start_time = None
        self.jobs_processed = 0
        self.jobs_failed = 0

    def handle(
        self,
        driver: Optional[str] = None,
        queue: Optional[str] = None,
        timeout: Optional[str] = None,
        max_jobs: Optional[str] = None,
        max_time: Optional[str] = None,
    ):
        """Handle queue worker execution with enhanced monitoring."""
        self.info("⚡ Queue Worker Starting")

        # Store parameters for restart
        self._store_restart_params(driver, queue, timeout, max_jobs, max_time)

        # Setup file watching if reload is enabled
        if self.option("reload"):
            self._setup_file_watching()

        # Validate and prepare configuration
        try:
            worker_config = self._prepare_config(
                driver, queue, timeout, max_jobs, max_time
            )
        except Exception as e:
            self.error(f"❌ Configuration error: {e}")
            return

        # Show worker configuration
        self._show_config(worker_config)

        # Start main worker loop
        try:
            self._run_main_loop(worker_config)
        except Exception as e:
            self.error(f"❌ Worker error: {e}")
        finally:
            self._cleanup_watching()
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
                raise Exception(f"Invalid timeout value: {e}")
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
                raise Exception(f"Invalid max-jobs value: {e}")

        max_time_val = None
        if max_time:
            try:
                max_time_val = int(max_time)
                if max_time_val <= 0:
                    raise ValueError("max-time must be positive")
            except ValueError as e:
                raise Exception(f"Invalid max-time value: {e}")

        return {
            "driver_name": driver_name,
            "queue_name": queue,
            "timeout": timeout_val,
            "max_jobs": max_jobs_val,
            "max_time": max_time_val,
        }

    def _show_config(self, config: Dict[str, Any]):
        """Display worker configuration."""
        self.info("🔧 Worker Configuration:")
        self.info(f"   Driver: {config['driver_name']}")
        self.info(f"   Queue: {config['queue_name'] or 'default'}")
        self.info(f"   Poll Timeout: {config['timeout']} seconds")

        if self.option("reload"):
            self.info("   Auto-reload: ✅ Enabled")
        else:
            self.info("   Auto-reload: ❌ Disabled")

        if config["max_jobs"]:
            self.info(f"   Max Jobs: {config['max_jobs']}")
        if config["max_time"]:
            self.info(f"   Max Runtime: {config['max_time']} seconds")

    def _run_main_loop(self, worker_config: Dict[str, Any]):
        """Main worker loop - called by ReloadableMixin on restart."""
        # Clean up connections before starting
        self._cleanup_connections_for_restart()

        # Reset counters for fresh start
        self.jobs_processed = 0
        self.jobs_failed = 0

        # Run the worker
        self._run_worker(worker_config)

    def _run_worker(self, config: Dict[str, Any]):
        """Run the queue worker with monitoring."""
        self.info("🚀 Worker started - Press Ctrl+C for graceful shutdown")

        # Create custom work callback that shows job names
        self._create_custom_callback()

        consume_opts = {"poll": config["timeout"]}
        if config["queue_name"]:
            consume_opts["queue"] = config["queue_name"]

        self.start_time = time.time()

        try:
            while not self.shutdown_requested:
                # Check limits
                if self._should_stop(config):
                    break

                    # Process jobs using actual queue consume with timeout
                try:
                    from cara.facades.Queue import Queue

                    # Use shorter timeout to allow Ctrl+C to work
                    consume_opts_with_timeout = {**consume_opts, "poll": 1}

                    # Try to consume one job
                    driver = Queue.driver(config["driver_name"])

                    if hasattr(driver, "channel") and driver.channel:
                        # AMQP specific: check if there are messages
                        method_frame, header_frame, body = driver.channel.basic_get(
                            queue=consume_opts.get("queue", "cara_default")
                        )

                        if method_frame:
                            # Process the message
                            driver._work_callback(
                                driver.channel, method_frame, header_frame, body
                            )
                        else:
                            # No jobs, short sleep
                            time.sleep(0.5)
                    else:
                        # Fallback: start connection and try consume with timeout
                        Queue.consume(
                            driver_name=config["driver_name"], **consume_opts_with_timeout
                        )

                except KeyboardInterrupt:
                    raise  # Re-raise to be caught by outer handler
                except Exception as e:
                    self.jobs_failed += 1
                    self.warning(f"⚠️  Queue processing error: {str(e)}")
                    # Also print the full traceback for debugging
                    import traceback

                    self.error(f"Full error details: {traceback.format_exc()}")
                    time.sleep(1)  # Wait before retrying

        except KeyboardInterrupt:
            self.info("\n🛑 Graceful shutdown initiated by user")

        # Close any open connections
        self._cleanup_connections(config)
        self.info("✅ Worker stopped gracefully")

    def _cleanup_connections(self, config: Dict[str, Any]):
        """Clean up any open queue connections."""
        try:
            from cara.facades.Queue import Queue

            driver = Queue.driver(config["driver_name"])

            # Close AMQP connections if they exist
            if hasattr(driver, "channel") and driver.channel:
                try:
                    driver.channel.stop_consuming()
                    driver.channel.close()
                except Exception:
                    pass

            if hasattr(driver, "connection") and driver.connection:
                try:
                    driver.connection.close()
                except Exception:
                    pass
        except Exception:
            pass

    def _create_custom_callback(self):
        """Create a custom callback for the AMQP driver to track job names."""
        try:
            from cara.facades.Queue import Queue

            # Get the queue driver
            driver = Queue.driver()

            if hasattr(driver, "_work_callback"):
                # Store original callback
                original_callback = driver._work_callback

                def enhanced_callback(ch, method, properties, body):
                    """Enhanced callback that shows job names."""
                    try:
                        import pickle

                        msg = pickle.loads(body)
                        raw = msg.get("obj")

                        # Extract job name
                        job_name = "UnknownJob"
                        if hasattr(raw, "__name__"):  # Class
                            job_name = raw.__name__
                        elif hasattr(raw, "__class__"):  # Instance
                            job_name = raw.__class__.__name__

                        self.jobs_processed += 1
                        self.line(f"  ✓ {job_name}")

                        # Call original processing
                        original_callback(ch, method, properties, body)

                        # Show stats every 10 jobs
                        if self.jobs_processed % 10 == 0:
                            self.info(
                                f"📊 Processed {self.jobs_processed} jobs, {self._get_runtime()}"
                            )

                    except Exception:
                        # If our enhancement fails, fallback to original
                        self.jobs_failed += 1
                        original_callback(ch, method, properties, body)

                # Replace the callback
                driver._work_callback = enhanced_callback

        except Exception:
            # If we can't enhance the callback, just proceed normally
            pass

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
            from app.models import Job

            stats = Job.get_queue_stats(self.queue_name)
            self.info(f"\n📈 Current Queue Status ({self.queue_name}):")
            self.info(f"   Pending: {stats.get('pending_jobs', 0)}")
            self.info(f"   Processing: {stats.get('processing_jobs', 0)}")
            self.info(f"   Completed: {stats.get('completed_jobs', 0)}")
            self.info(f"   Cancelled: {stats.get('cancelled_jobs', 0)}")
            self.info(f"   Failed: {stats.get('failed_jobs', 0)}")
        except Exception:
            # If Job model not available or DB error, skip enhanced stats
            pass

    def _setup_file_watching(self):
        """Setup file watching for auto-reload using existing Command system."""
        self.info("🔄 Auto-reload enabled - watching for file changes...")

        # Import the existing Command class with file watching
        from cara.commands.Command import Command

        # Create a Command instance with watch=True
        self.command_watcher = Command(self.application, watch=True)

        # Override the reload method to restart the worker
        original_reload = self.command_watcher.reload

        def worker_reload():
            self.info("🔄 File change detected, restarting worker...")
            self.shutdown_requested = True
            # Give worker time to finish current job gracefully
            time.sleep(0.5)
            # Purge loaded app.* modules so new code loads on next import
            self._purge_code_modules()
            # Restart the worker loop instead of exiting
            self._restart_worker()

        self.command_watcher.reload = worker_reload

    def _restart_worker(self):
        """Restart the worker internally without exiting the process."""
        try:
            # Clean up any existing connections first
            self._cleanup_connections_for_restart()

            # Clean up old file watchers before setting up new ones
            self._cleanup_watching()

            # Reset counters
            self.jobs_processed = 0
            self.jobs_failed = 0
            self.shutdown_requested = False

            # Re-prepare config (in case config files changed)
            worker_config = self._prepare_config(None, None, None, None, None)

            self.info("🔄 Worker restarted successfully")

            # Re-setup file watching for next restart
            if self.option("reload"):
                self._setup_file_watching()

            # Restart the worker loop
            self._run_worker(worker_config)

        except Exception as e:
            self.error(f"❌ Failed to restart worker: {e}")
            self.shutdown_requested = True

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

    def _get_reload_message(self):
        """Custom reload message for queue worker."""
        return "🔄 File change detected, restarting worker..."

    def _purge_code_modules(self):
        """Force-reload app code by purging app.* modules from sys.modules and invalidating caches."""
        try:
            importlib.invalidate_caches()
            to_delete = []
            for name in list(sys.modules.keys()):
                if name == "app" or name.startswith("app."):
                    to_delete.append(name)
            for name in to_delete:
                try:
                    del sys.modules[name]
                except Exception:
                    pass
        except Exception:
            pass
