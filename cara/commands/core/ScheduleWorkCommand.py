"""
Schedule Worker Command for the Cara framework.

This module provides a CLI command to process scheduled jobs with enhanced UX.
"""

import time
import traceback
import uuid
from typing import Any, Dict, List, Optional

from cara.commands import CommandBase
from cara.commands.AutoReloadMixin import AutoReloadMixin
from cara.configuration import config
from cara.decorators import command
from cara.facades import Queue, Schedule
from cara.scheduling.contracts import ShouldSchedule


@command(
    name="schedule:work",
    help="Run the schedule worker to register and execute scheduled tasks.",
    options={
        "--driver=?": "Scheduler driver to use (overrides default)",
        "--once": "Run scheduled tasks once and exit",
        "--stats": "Show scheduling statistics",
        "--reload": "Enable auto-reload on file changes",
    },
)
class ScheduleWorkCommand(AutoReloadMixin, CommandBase):
    """Run schedule worker with enhanced monitoring and task registration."""

    def __init__(self, application=None):
        super().__init__(application)
        self.start_time = None

    def handle(self, driver: Optional[str] = None):
        """Handle schedule worker execution with enhanced monitoring."""
        self.console.print()  # Empty line for spacing
        self.console.print("[bold #e5c07b]╭─ Schedule Worker ─╮[/bold #e5c07b]")
        self.console.print()

        # Store parameters for restart
        self.store_restart_params(driver)

        # Setup auto-reload if enabled (default: true for development)
        if self.option("reload") or config("app.debug", True):
            self.enable_auto_reload()

        # Start main scheduler loop
        try:
            self._run_main_loop(driver)
        except Exception as e:
            import traceback
            self.error(f"× Scheduler error: {e}")
            self.error(f"× Stack trace: {traceback.format_exc()}")
        finally:
            self.cleanup_auto_reload()
            self._show_final_stats()

    def _run_main_loop(self, *args, **kwargs):
        """Main scheduler loop - called by AutoReloadMixin on restart."""
        # Use stored parameters from store_restart_params
        if hasattr(self, '_restart_params') and self._restart_params:
            driver = self._restart_params[0] if self._restart_params else None
        else:
            driver = args[0] if args else None

        # Prepare configuration
        try:
            scheduler_config = self._prepare_config(driver)
        except Exception as e:
            self.error(f"× Configuration error: {e}")
            return

        # Show scheduler configuration
        self._show_config(scheduler_config)

        # Register and run scheduled jobs
        try:
            job_entries = self._register_jobs()
            if not job_entries:
                self.warning("⚠️  No scheduled jobs found to register")
                return

            self._show_jobs(job_entries)
            self._start_scheduler(scheduler_config)

        except KeyboardInterrupt:
            self.info("\n⏸️  Schedule worker stopped by user")
        except Exception as e:
            self.error(f"× Scheduler error: {e}")
            if config("app.debug", False):
                self.error(f"Stack trace: {traceback.format_exc()}")

    def _prepare_config(self, driver: Optional[str]) -> Dict[str, Any]:
        """Prepare and validate scheduler configuration."""
        driver_name = driver or config("scheduling.default")
        if not driver_name:
            raise Exception("No scheduler driver specified and no default configured")

        return {
            "driver_name": driver_name,
            "run_once": self.option("once"),
            "show_stats": self.option("stats"),
            "debug": config("app.debug", False),
        }

    def _show_config(self, scheduler_config: Dict[str, Any]):
        """Display scheduler configuration in ServeCommand style."""
        self.console.print("[bold #e5c07b]┌─ Configuration[/bold #e5c07b]")
        
        # Driver info
        self.console.print(
            f"[#e5c07b]│[/#e5c07b] [white]Driver:[/white] [bold white]{scheduler_config['driver_name'].upper()}[/bold white]"
        )
        
        # Run mode
        run_mode = "Once" if scheduler_config['run_once'] else "Continuous"
        mode_color = "#e5c07b" if scheduler_config['run_once'] else "#30e047"
        self.console.print(
            f"[#e5c07b]│[/#e5c07b] [white]Run Mode:[/white] [{mode_color}]{run_mode}[/{mode_color}]"
        )
        
        # Statistics
        self.console.print(
            f"[#e5c07b]│[/#e5c07b] [white]Statistics:[/white] [{'#30e047' if scheduler_config['show_stats'] else '#E21102'}]{'✓' if scheduler_config['show_stats'] else '×'}[/{'#30e047' if scheduler_config['show_stats'] else '#E21102'}]"
        )
        
        # Auto-reload status (default: enabled in development)
        from cara.configuration import config as global_config
        auto_reload = self.option("reload") or global_config("app.debug", True)
        self.console.print(
            f"[#e5c07b]│[/#e5c07b] [white]Auto-reload:[/white] [{'#30e047' if auto_reload else '#E21102'}]{'✓' if auto_reload else '×'}[/{'#30e047' if auto_reload else '#E21102'}]"
        )
        
        self.console.print("[#e5c07b]└─[/#e5c07b]")
        self.console.print()

    def _register_jobs(self) -> List[Dict[str, Any]]:
        """Register all scheduled jobs and return summary."""
        jobs = config("scheduling.jobs", []) or []
        if not jobs:
            return []

        self.info("📋 Registering scheduled jobs...")
        job_entries = []

        for job_target in jobs:
            try:
                job_name = getattr(job_target, "__name__", str(job_target))

                # Handle ShouldSchedule interface
                if isinstance(job_target, type) and issubclass(
                    job_target, ShouldSchedule
                ):
                    job_target.schedule(Schedule)
                    job_entries.append(
                        {
                            "name": job_name,
                            "id": "self-scheduled",
                            "type": "ShouldSchedule",
                            "schedule": "See schedule() method",
                        }
                    )
                    continue

                # Handle decorator-based scheduling
                specs = getattr(job_target, "_schedule_specs", None)
                if specs:
                    for spec in specs:
                        entry = self._register_spec_job(job_target, job_name, spec)
                        if entry:
                            job_entries.append(entry)
                else:
                    self.warning(f"⚠️  No schedule metadata found for '{job_name}'")

            except Exception as e:
                job_name = getattr(job_target, "__name__", str(job_target))
                self.warning(f"⚠️  Failed to register job '{job_name}': {e}")

        return job_entries

    def _register_spec_job(
        self, job_target: Any, job_name: str, spec: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Register a job from schedule specification."""
        mode = spec["mode"]
        job_id = spec.get("identifier") or f"{job_name}_{uuid.uuid4().hex[:8]}"

        # Create schedule builder
        if mode == "call":
            builder = Schedule.call(job_target)
        elif mode == "job":
            builder = Schedule.call(lambda: self.application.make(job_target).handle())
        elif mode == "command":
            driver_name = spec.get("driver_name")
            builder = Schedule.call(lambda: self._queue_command(job_target, driver_name))
        else:
            self.warning(f"⚠️  Unsupported schedule mode '{mode}' for {job_name}")
            return None

        # Configure builder
        builder.identifier = job_id
        builder.options.update({"silent": True})

        # Apply schedule based on type
        schedule_type = spec["type"]
        args = spec.get("args", ())
        kwargs = dict(spec.get("kwargs", {}))

        if spec.get("timezone"):
            kwargs["timezone"] = spec["timezone"]

        try:
            if schedule_type == "cron":
                builder.cron(*args, **kwargs)
            elif schedule_type == "daily":
                builder.daily(**kwargs)
            elif schedule_type == "hourly":
                builder.hourly(**kwargs)
            elif schedule_type == "interval":
                builder.interval(**kwargs)
            elif schedule_type == "at":
                builder.at(*args)
            elif schedule_type == "weekly":
                day = args[0]
                builder.weekly(
                    day,
                    hour=kwargs.get("hour", 0),
                    minute=kwargs.get("minute", 0),
                    timezone=kwargs.get("timezone"),
                )
            else:
                raise Exception(f"Unknown schedule type '{schedule_type}'")

            return {
                "name": job_name,
                "id": job_id,
                "type": mode,
                "schedule": self._describe_schedule(spec),
            }

        except Exception as e:
            raise Exception(f"Failed to configure schedule: {e}")

    def _queue_command(self, command_target: Any, driver_name: str = None):
        """Queue command for execution."""
        instance = (
            self.application.make(command_target)
            if isinstance(command_target, type)
            else command_target
        )

        if driver_name:
            Queue.push(instance, driver_name=driver_name)
        else:
            Queue.push(instance)

    def _describe_schedule(self, spec: Dict[str, Any]) -> str:
        """Create human-readable schedule description."""
        schedule_type = spec["type"]
        args = spec.get("args", ())
        kwargs = spec.get("kwargs", {})
        tz = kwargs.get("timezone", "")
        tz_str = f" ({tz})" if tz else ""

        if schedule_type == "cron":
            expr = args[0] if args else kwargs.get("expression", "")
            return f"Cron: {expr}{tz_str}"
        elif schedule_type == "daily":
            hour = kwargs.get("hour", 0)
            minute = kwargs.get("minute", 0)
            return f"Daily at {hour:02d}:{minute:02d}{tz_str}"
        elif schedule_type == "hourly":
            minute = kwargs.get("minute", 0)
            return f"Hourly at :{minute:02d}{tz_str}"
        elif schedule_type == "interval":
            parts = []
            if kwargs.get("hours"):
                parts.append(f"{kwargs['hours']}h")
            if kwargs.get("minutes"):
                parts.append(f"{kwargs['minutes']}m")
            if kwargs.get("seconds"):
                parts.append(f"{kwargs['seconds']}s")
            interval = " ".join(parts) if parts else "0s"
            return f"Every {interval}{tz_str}"
        elif schedule_type == "weekly":
            day = args[0] if args else "?"
            hour = kwargs.get("hour", 0)
            minute = kwargs.get("minute", 0)
            return f"Weekly {day} at {hour:02d}:{minute:02d}{tz_str}"
        else:
            return schedule_type

    def _show_jobs(self, job_entries: List[Dict[str, Any]]):
        """Display registered jobs in ServeCommand style."""
        self.console.print("[bold #e5c07b]┌─ Scheduled Jobs[/bold #e5c07b]")
        
        for i, job in enumerate(job_entries[:5], 1):  # Show first 5
            job_type_color = "#30e047" if job['type'] == 'command' else "#e5c07b"
            self.console.print(
                f"[#e5c07b]│[/#e5c07b]   [white]{i}.[/white] [{job_type_color}]{job['name']}[/{job_type_color}] [dim]({job['schedule']})[/dim]"
            )
            
        if len(job_entries) > 5:
            self.console.print(
                f"[#e5c07b]│[/#e5c07b]   [dim]... and {len(job_entries) - 5} more jobs[/dim]"
            )
            
        self.console.print("[#e5c07b]└─[/#e5c07b]")
        self.console.print()

    def _show_scheduler_status(self):
        """Display scheduler status in ServeCommand style."""
        self.console.print("[bold #e5c07b]┌─ Scheduler Status[/bold #e5c07b]")
        self.console.print(
            "[#e5c07b]│[/#e5c07b] [white]Status:[/white] [#30e047]✓ Active - Processing scheduled tasks[/#30e047]"
        )
        self.console.print("[#e5c07b]└─[/#e5c07b]")
        self.console.print()
        
        # Simple ready message
        self.console.print("[dim]Press Ctrl+C to stop the scheduler[/dim]")
        self.console.print()

    def _start_scheduler(self, scheduler_config: Dict[str, Any]):
        """Start the scheduler with the specified configuration."""
        self._show_scheduler_status()
        self.start_time = time.time()

        try:
            driver = Schedule.driver(scheduler_config["driver_name"])

            # Start scheduler
            try:
                driver.start()
            except Exception as e:
                if "already running" not in str(e).lower():
                    raise

            if scheduler_config["run_once"]:
                self.console.print("[#30e047]✅ Scheduled tasks executed once[/#30e047]")
            else:
                # Keep running until interrupted
                while not self.shutdown_requested:
                    time.sleep(1)

        except Exception as e:
            raise Exception(f"Failed to start scheduler: {e}")

    def _show_final_stats(self):
        """Show final scheduler statistics."""
        if not hasattr(self, 'start_time') or not self.start_time:
            return
            
        runtime_seconds = int(time.time() - self.start_time)
        hours, remainder = divmod(runtime_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        runtime = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        
        self.console.print()
        self.console.print("[bold #e5c07b]📊 Final Scheduler Statistics:[/bold #e5c07b]")
        self.console.print(f"   Runtime: {runtime}")
        self.console.print(f"   Tasks Executed: {getattr(self, 'tasks_executed', 0)}")
        self.console.print()
