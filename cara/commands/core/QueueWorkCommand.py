"""
Queue Worker Command for the Cara framework.

This module provides a CLI command to process jobs from the queue with enhanced UX.
"""

import time
from typing import Any, Dict, Optional

from cara.commands import CommandBase
from cara.commands.AutoReloadMixin import AutoReloadMixin
from cara.configuration import config
from cara.decorators import command


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
            self.config("queue.drivers.amqp.password")
        )
        parameters = pika.ConnectionParameters(
            host=self.config("queue.drivers.amqp.host"),
            port=self.config("queue.drivers.amqp.port", 5672),
            virtual_host=self.config("queue.drivers.amqp.vhost", "/"),
            credentials=credentials
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
            except:
                pass


class JobProcessor:
    """Processes individual jobs from queue messages (Single Responsibility)."""
    
    @staticmethod
    def process_message(channel, method_frame, body) -> bool:
        """Process a single queue message and return success status."""
        try:
            import asyncio
            import inspect
            import pickle

            # Unpickle message
            msg = pickle.loads(body)
            instance = msg.get("obj")
            callback = msg.get("callback", "handle")
            init_args = msg.get("args", ())
            
            # Set up job tracking
            job_id = msg.get("job_id")
            if hasattr(instance, "set_tracking_id") and job_id:
                instance.set_tracking_id(job_id)
            
            db_job_id = msg.get("db_job_id")
            if db_job_id and hasattr(instance, '__dict__'):
                instance._db_job_id = db_job_id
            
            # Execute job
            method_to_call = getattr(instance, callback, None)
            if callable(method_to_call):
                if inspect.iscoroutinefunction(method_to_call):
                    asyncio.run(method_to_call(*init_args))
                else:
                    method_to_call(*init_args)
            
            # Acknowledge message
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            return True
            
        except Exception as job_error:
            # Job failed, still ack to avoid redelivery loop
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            
            # Try to call failed method
            try:
                if hasattr(instance, "failed"):
                    failed_method = getattr(instance, "failed")
                    if inspect.iscoroutinefunction(failed_method):
                        asyncio.run(failed_method(msg, str(job_error)))
                    else:
                        failed_method(msg, str(job_error))
            except:
                pass
            
            print(f"Job processing failed: {job_error}")
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
            if '*' in pattern:
                expanded_queues.extend(self._expand_wildcard_pattern(pattern))
            else:
                expanded_queues.append(pattern)
        
        return expanded_queues if expanded_queues else ["default"]
    
    def _expand_wildcard_pattern(self, pattern: str) -> list:
        """Expand wildcard pattern to actual queue names."""
        # Standard priority levels for all queue types
        priority_levels = ['critical', 'high', 'default', 'low']
        
        if pattern.endswith('.*'):
            # Pattern like "enrichment.*" or "discovery.*"
            prefix = pattern[:-2]  # Remove ".*"
            return [f"{prefix}.{level}" for level in priority_levels]
        elif pattern.endswith('*'):
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
        queue_names = config['queue_names']
        if len(queue_names) > 1:
            self.console.print(
                f"[#e5c07b]│[/#e5c07b] [white]Queues:[/white] [dim]{len(queue_names)} queues in priority order[/dim]"
            )
            for i, queue in enumerate(queue_names, 1):  # Show all queues
                priority_color = "#E21102" if "critical" in queue else "#e5c07b" if "high" in queue else "#30e047" if "default" in queue else "dim"
                self.console.print(
                    f"[#e5c07b]│[/#e5c07b]   [white]{i}.[/white] [{priority_color}]{queue}[/{priority_color}]"
                )
        else:
            queue_color = "#E21102" if "critical" in queue_names[0] else "#e5c07b" if "high" in queue_names[0] else "#30e047"
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
    
    def _show_worker_startup_info(self, queue_names: list) -> None:
        """Display worker startup information in ServeCommand style."""
        self.console.print("[bold #e5c07b]┌─ Worker Status[/bold #e5c07b]")
        
        if len(queue_names) > 1:
            self.console.print(
                f"[#e5c07b]│[/#e5c07b] [white]Processing:[/white] [dim]{len(queue_names)} queues in priority order[/dim]"
            )
        else:
            queue_color = "#E21102" if "critical" in queue_names[0] else "#e5c07b" if "high" in queue_names[0] else "#30e047"
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
        config: Dict[str, Any]
    ) -> bool:
        """Process one cycle through all queues in priority order."""
        for queue_name in queue_names:
            if self.shutdown_requested:
                break
                
            try:
                if self._process_single_queue(queue_name, connection_manager, job_processor):
                    return True  # Job processed, restart from highest priority
                    
            except Exception as e:
                self._handle_queue_error(queue_name, e, connection_manager)
                continue
        
        return False  # No jobs processed
    
    def _process_single_queue(
        self, 
        queue_name: str, 
        connection_manager: AMQPConnectionManager,
        job_processor: JobProcessor
    ) -> bool:
        """Process a single queue and return True if job was processed."""
        channel = connection_manager.create_channel()
        if not channel:
            return False
            
        try:
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
            except:
                pass
    
    def _handle_queue_error(
        self, 
        queue_name: str, 
        error: Exception, 
        connection_manager: AMQPConnectionManager
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

    def _run_main_loop(self, *args, **kwargs):
        """Main worker loop - called by AutoReloadMixin on restart."""
        # Use stored parameters from store_restart_params
        if hasattr(self, '_restart_params') and self._restart_params:
            driver, queue, timeout, max_jobs, max_time = self._restart_params
        else:
            driver, queue, timeout, max_jobs, max_time = args if args else (None, None, None, None, None)
            
        # Prepare config with current parameters
        try:
            worker_config = self._prepare_config(driver, queue, timeout, max_jobs, max_time)
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




