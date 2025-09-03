"""
Schedule Worker Command for the Cara framework.

This module provides a CLI command to process scheduled jobs with enhanced UX.
"""

import time
import traceback
import uuid
from typing import Any, Dict, List, Optional

from cara.commands import CommandBase
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
class ScheduleWorkCommand(CommandBase):
    """Run schedule worker with enhanced monitoring and task registration."""

    def handle(self, driver: Optional[str] = None):
        """Handle schedule worker execution with enhanced monitoring."""
        self.info("⏰ Schedule Worker Starting")

        # Setup file watching if reload is enabled
        if self.option("reload"):
            self._setup_file_watching()

        # Determine driver
        driver_name = driver or config("scheduling.default")
        if not driver_name:
            self.error("❌ No scheduler driver specified and no default configured")
            return

        # Show configuration
        self.info("🔧 Scheduler Configuration:")
        self.info(f"   Driver: {driver_name}")
        self.info(f"   Run Mode: {'Once' if self.option('once') else 'Continuous'}")
        self.info(f"   Statistics: {'✅' if self.option('stats') else '❌'}")
        if self.option("reload"):
            self.info("   Auto-reload: ✅ Enabled")
        else:
            self.info("   Auto-reload: ❌ Disabled")

        # Register and run scheduled jobs
        try:
            job_entries = self._register_jobs()
            if not job_entries:
                self.warning("⚠️  No scheduled jobs found to register")
                return

            self._show_jobs(job_entries)
            self._start_scheduler(driver_name)

        except KeyboardInterrupt:
            self.info("\n⏸️  Schedule worker stopped by user")
        except Exception as e:
            self.error(f"❌ Scheduler error: {e}")
            if config("app.debug", False):
                self.error(f"Stack trace: {traceback.format_exc()}")
        finally:
            self._cleanup_watching()

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
        """Display registered jobs."""
        headers = ["Job Name", "ID", "Type", "Schedule"]
        rows = [
            [entry["name"], entry["id"], entry["type"], entry["schedule"]]
            for entry in job_entries
        ]

        self.info(f"✅ Registered {len(job_entries)} scheduled job(s):")
        self.table(headers, rows)

        if self.option("stats"):
            type_counts = {}
            for entry in job_entries:
                job_type = entry["type"]
                type_counts[job_type] = type_counts.get(job_type, 0) + 1

            self.info("\n📊 Job Statistics:")
            self.info(f"   Total jobs: {len(job_entries)}")
            for job_type, count in sorted(type_counts.items()):
                self.info(f"   {job_type}: {count} job(s)")

    def _start_scheduler(self, driver_name: str):
        """Start the scheduler with the specified driver."""
        self.info("🚀 Starting scheduler...")

        try:
            driver = Schedule.driver(driver_name)

            # Start scheduler
            try:
                driver.start()
            except Exception as e:
                if "already running" not in str(e).lower():
                    raise

            if self.option("once"):
                self.info("✅ Scheduled tasks executed once")
            else:
                self.info("✅ Scheduler started successfully")
                self.info("📝 Press Ctrl+C to stop the scheduler")

        except Exception as e:
            raise Exception(f"Failed to start scheduler: {e}")

    def _setup_file_watching(self):
        """Setup file watching for auto-reload using existing Command system."""
        self.info("🔄 Auto-reload enabled - watching for file changes...")

        # Import the existing Command class with file watching
        from cara.commands.Command import Command

        # Create a Command instance with watch=True
        self.command_watcher = Command(self.application, watch=True)

        # Override the reload method to restart the scheduler
        original_reload = self.command_watcher.reload

        def scheduler_reload():
            self.info("🔄 File change detected, restarting scheduler...")
            # Give scheduler time to finish current tasks
            time.sleep(0.5)
            # Restart the scheduler instead of exiting
            self._restart_scheduler()

        self.command_watcher.reload = scheduler_reload

    def _restart_scheduler(self):
        """Restart the scheduler internally without exiting the process."""
        try:
            self.info("🔄 Scheduler restarted successfully")

            # Re-register and restart jobs
            job_entries = self._register_jobs()
            if job_entries:
                self._show_jobs(job_entries)

                # Determine driver again (in case config changed)
                driver_name = config("scheduling.default")
                if driver_name:
                    self._start_scheduler(driver_name)

        except Exception as e:
            self.error(f"❌ Failed to restart scheduler: {e}")

    def _cleanup_watching(self):
        """Cleanup file watching resources."""
        if hasattr(self, "command_watcher") and self.command_watcher:
            try:
                self.command_watcher.shutdown()
            except Exception:
                pass
