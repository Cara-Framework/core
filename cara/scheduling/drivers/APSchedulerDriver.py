"""
APScheduler Driver for the Cara framework.

This module provides a scheduling driver that integrates APScheduler for background job scheduling
and execution.
"""

from typing import Any, Dict, Iterable

from cara.facades import Log
from cara.scheduling.contracts import Scheduling


class APSchedulerDriver(Scheduling):
    """
    APScheduler driver supporting both blocking and background modes.

    Reads settings dict for:
      - mode: "blocking" or "background" (default "blocking")
      - jobstores, executors, job_defaults, timezone, etc.
    """

    driver_name = "apscheduler"

    def __init__(self, settings: Dict[str, Any] = None):
        settings = settings or {}

        # Configure APScheduler logging to reduce noise
        self._configure_logging()

        # Determine mode
        self.mode = self._get_mode(settings)

        # Create scheduler
        self.scheduler = self._create_scheduler(settings)

    def _configure_logging(self) -> None:
        """Configure APScheduler logging to reduce noise."""
        import logging

        logging.getLogger("apscheduler").setLevel(logging.WARNING)
        logging.getLogger("apscheduler.executors").setLevel(logging.ERROR)
        logging.getLogger("apscheduler.schedulers").setLevel(logging.ERROR)

    def _get_mode(self, settings: Dict[str, Any]) -> str:
        """Get and validate scheduler mode."""
        mode = settings.get("mode", "blocking").lower()
        if mode not in ("blocking", "background"):
            Log.warning(
                f"APSchedulerDriver: unknown mode '{mode}', defaulting to 'blocking'"
            )
            mode = "blocking"
        return mode

    def _create_scheduler(self, settings: Dict[str, Any]):
        """Create and configure the appropriate scheduler."""
        try:
            from apscheduler.schedulers.background import BackgroundScheduler
            from apscheduler.schedulers.blocking import BlockingScheduler
        except ImportError:
            raise ImportError(
                "APScheduler is required for scheduling. Please install it with: pip install apscheduler"
            )

        # Prepare scheduler kwargs
        sched_kwargs = self._extract_scheduler_kwargs(settings)

        # Create scheduler
        try:
            if self.mode == "background":
                scheduler = BackgroundScheduler(**sched_kwargs)
                Log.info(
                    f"APSchedulerDriver initialized in background mode with settings: {sched_kwargs}"
                )
            else:
                scheduler = BlockingScheduler(**sched_kwargs)
                Log.info(
                    f"APSchedulerDriver initialized in blocking mode with settings: {sched_kwargs}"
                )

            return scheduler
        except Exception as e:
            Log.error(f"Failed to create APScheduler scheduler ({self.mode}): {e}")
            raise

    def _extract_scheduler_kwargs(self, settings: Dict[str, Any]) -> Dict[str, Any]:
        """Extract scheduler-specific kwargs from settings."""
        sched_kwargs = {}
        if hasattr(settings, "get"):
            for key in ("jobstores", "executors", "job_defaults", "timezone"):
                if key in settings:
                    sched_kwargs[key] = settings[key]
        return sched_kwargs

    def schedule_job(
        self,
        identifier: str,
        callback: Any,
        schedule_spec: Dict[str, Any],
        options: Dict[str, Any],
    ) -> None:
        """Register a job according to schedule_spec."""
        silent = bool(options.get("silent", False))

        # Remove existing job if present
        self._remove_existing_job(identifier, silent)

        # Build trigger
        try:
            trigger = self._build_trigger(schedule_spec)
        except Exception as e:
            if not silent:
                Log.error(f"Failed to build trigger for job '{identifier}': {e}")
            raise

        # Add job
        self._add_job(identifier, callback, trigger, options, silent)

    def _remove_existing_job(self, identifier: str, silent: bool) -> None:
        """Remove existing job if present."""
        try:
            existing = self.scheduler.get_job(job_id=identifier)
            if existing:
                try:
                    self.scheduler.remove_job(job_id=identifier)
                    if not silent:
                        Log.info(
                            f"Removed existing scheduled job '{identifier}' before re-adding."
                        )
                except Exception:
                    if not silent:
                        Log.warning(f"Failed to remove existing job '{identifier}'.")
        except Exception:
            pass

    def _add_job(
        self,
        identifier: str,
        callback: Any,
        trigger: Any,
        options: Dict[str, Any],
        silent: bool,
    ) -> None:
        """Add job to scheduler."""
        job_opts = {}
        if hasattr(options, "get") and "apscheduler_job_options" in options:
            job_opts = options.get("apscheduler_job_options", {})

        try:
            self.scheduler.add_job(
                func=callback,
                trigger=trigger,
                id=identifier,
                **job_opts,
            )
            if not silent:
                Log.info(f"Scheduled job '{identifier}'.")
        except Exception as e:
            if not silent:
                Log.error(f"Failed to schedule job '{identifier}': {e}")
            raise

    def _build_trigger(self, schedule_spec: Dict[str, Any]):
        """Build trigger based on schedule specification."""
        if "type" not in schedule_spec:
            raise ValueError("schedule_spec must include 'type' key.")

        sched_type = schedule_spec["type"]

        # Import trigger classes when needed
        try:
            from apscheduler.triggers.cron import CronTrigger
            from apscheduler.triggers.date import DateTrigger
            from apscheduler.triggers.interval import IntervalTrigger
        except ImportError:
            raise ImportError(
                "APScheduler is required for scheduling. Please install it with: pip install apscheduler"
            )

        if sched_type == "cron":
            return self._build_cron_trigger(schedule_spec, CronTrigger)
        elif sched_type == "daily":
            return self._build_daily_trigger(schedule_spec, CronTrigger)
        elif sched_type == "hourly":
            return self._build_hourly_trigger(schedule_spec, CronTrigger)
        elif sched_type == "weekly":
            return self._build_weekly_trigger(schedule_spec, CronTrigger)
        elif sched_type == "interval":
            return self._build_interval_trigger(schedule_spec, IntervalTrigger)
        elif sched_type == "date":
            return self._build_date_trigger(schedule_spec, DateTrigger)
        else:
            raise ValueError(f"Unknown schedule type: {sched_type}")

    def _build_cron_trigger(self, schedule_spec: Dict[str, Any], CronTrigger):
        """Build cron trigger."""
        expr = schedule_spec.get("expression")
        if not isinstance(expr, str):
            raise ValueError("For cron type, 'expression' string is required.")

        tz = schedule_spec.get("timezone", None)
        parts = expr.split()
        if len(parts) != 5:
            raise ValueError(
                "Cron expression must have 5 fields: minute hour day month day_of_week"
            )

        minute, hour, day, month, day_of_week = parts
        return CronTrigger(
            minute=minute,
            hour=hour,
            day=day,
            month=month,
            day_of_week=day_of_week,
            timezone=tz,
        )

    def _build_daily_trigger(self, schedule_spec: Dict[str, Any], CronTrigger):
        """Build daily trigger."""
        hour = schedule_spec.get("hour", 0)
        minute = schedule_spec.get("minute", 0)
        tz = schedule_spec.get("timezone", None)
        return CronTrigger(hour=hour, minute=minute, timezone=tz)

    def _build_hourly_trigger(self, schedule_spec: Dict[str, Any], CronTrigger):
        """Build hourly trigger."""
        minute = schedule_spec.get("minute", 0)
        tz = schedule_spec.get("timezone", None)
        return CronTrigger(minute=minute, timezone=tz)

    def _build_weekly_trigger(self, schedule_spec: Dict[str, Any], CronTrigger):
        """Build weekly trigger."""
        day_of_week = schedule_spec.get("day_of_week")
        hour = schedule_spec.get("hour", 0)
        minute = schedule_spec.get("minute", 0)
        tz = schedule_spec.get("timezone", None)
        return CronTrigger(
            minute=minute,
            hour=hour,
            day="*",
            month="*",
            day_of_week=day_of_week,
            timezone=tz,
        )

    def _build_interval_trigger(self, schedule_spec: Dict[str, Any], IntervalTrigger):
        """Build interval trigger."""
        return IntervalTrigger(
            seconds=schedule_spec.get("seconds", 0),
            minutes=schedule_spec.get("minutes", 0),
            hours=schedule_spec.get("hours", 0),
            days=schedule_spec.get("days", 0),
            weeks=schedule_spec.get("weeks", 0),
            timezone=schedule_spec.get("timezone", None),
        )

    def _build_date_trigger(self, schedule_spec: Dict[str, Any], DateTrigger):
        """Build date trigger."""
        run_date = schedule_spec.get("run_date")
        if not run_date:
            raise ValueError("For date type, 'run_date' is required.")
        tz = schedule_spec.get("timezone", None)
        return DateTrigger(run_date=run_date, timezone=tz)

    def start(self) -> None:
        """Start the scheduler according to mode."""
        try:
            if self.mode == "background":
                Log.info("Starting APScheduler BackgroundScheduler in background mode...")
                self.scheduler.start()
                Log.info(
                    "APScheduler BackgroundScheduler started (running in background)."
                )
            else:
                Log.info(
                    "Starting APScheduler BlockingScheduler (will block main thread)..."
                )
                self.scheduler.start()
                Log.info("APScheduler BlockingScheduler has exited.")
        except (KeyboardInterrupt, SystemExit):
            Log.info("APScheduler scheduler interrupted, shutting down.")
            try:
                self.scheduler.shutdown(wait=False)
            except Exception:
                pass
        except Exception as e:
            Log.error(f"Failed to start APScheduler scheduler ({self.mode}): {e}")
            raise

    def shutdown(self, wait: bool = True) -> None:
        """Shutdown the scheduler gracefully."""
        try:
            self.scheduler.shutdown(wait=wait)
            Log.info("APScheduler scheduler shut down.")
        except Exception as e:
            Log.warning(f"Error shutting down APScheduler scheduler: {e}")

    def remove_job(self, identifier: str) -> None:
        """Remove a scheduled job."""
        try:
            self.scheduler.remove_job(job_id=identifier)
            Log.info(f"Removed scheduled job '{identifier}'.")
        except Exception:
            Log.warning(f"Failed to remove job '{identifier}' or job not found.")

    def list_jobs(self) -> Iterable[Any]:
        """List all scheduled jobs."""
        try:
            return self.scheduler.get_jobs()
        except Exception as e:
            Log.warning(f"Failed to list scheduled jobs: {e}")
            return []
