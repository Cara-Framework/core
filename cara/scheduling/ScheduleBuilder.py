"""
Schedule Builder for the Cara framework.

This module provides utilities for building and configuring job schedules in a fluent interface.
Supports Laravel-style schedule features including overlapping prevention and maintenance mode.
"""

from typing import Callable


class ScheduleBuilder:
    def __init__(self, driver, identifier, callback, options):
        self.driver = driver
        self.identifier = identifier
        self.callback = callback
        self.options = options or {}
        # Schedule configuration
        self._without_overlapping = False
        self._skip_if_maintenance = False
        self._timezone = None

    def cron(self, expression, timezone=None):
        spec = {"type": "cron", "expression": expression}
        if timezone:
            spec["timezone"] = timezone
        self.driver.schedule_job(
            self.identifier,
            self.callback,
            spec,
            self.options,
        )
        return self

    def daily(self, hour=0, minute=0, timezone=None):
        spec = {
            "type": "daily",
            "hour": hour,
            "minute": minute,
        }
        if timezone:
            spec["timezone"] = timezone
        self.driver.schedule_job(
            self.identifier,
            self.callback,
            spec,
            self.options,
        )
        return self

    def hourly(self, minute=0, timezone=None):
        spec = {"type": "hourly", "minute": minute}
        if timezone:
            spec["timezone"] = timezone
        self.driver.schedule_job(
            self.identifier,
            self.callback,
            spec,
            self.options,
        )
        return self

    def interval(self, seconds=0, minutes=0, hours=0):
        spec = {
            "type": "interval",
            "seconds": seconds,
            "minutes": minutes,
            "hours": hours,
        }
        self.driver.schedule_job(
            self.identifier,
            self.callback,
            spec,
            self.options,
        )
        return self

    def at(self, when):
        spec = {"type": "at", "run_date": when}
        self.driver.schedule_job(
            self.identifier,
            self.callback,
            spec,
            self.options,
        )
        return self

    def weekly(self, day_of_week, hour=0, minute=0, timezone=None):
        spec = {
            "type": "weekly",
            "day_of_week": day_of_week,
            "hour": hour,
            "minute": minute,
        }
        if timezone:
            spec["timezone"] = timezone
        self.driver.schedule_job(
            self.identifier,
            self.callback,
            spec,
            self.options,
        )
        return self

    def without_overlapping(self, timeout: int = 1440) -> "ScheduleBuilder":
        """
        Prevent this scheduled task from overlapping executions (Laravel-style).

        Uses a lock file or cache lock to ensure only one instance runs at a time.
        If a previous execution is still running, the current execution is skipped.

        Args:
            timeout: Lock timeout in seconds (default: 24 hours)

        Example:
            scheduler.command("send:emails").daily().without_overlapping()

        Returns:
            self for fluent interface
        """
        self._without_overlapping = True
        self.options["lock_timeout"] = timeout
        return self

    def skip_if_maintenance(self) -> "ScheduleBuilder":
        """
        Skip this scheduled task during maintenance mode (Laravel-style).

        Useful for preventing long-running tasks from interfering with maintenance windows.

        Example:
            scheduler.command("queue:work").everyMinute().skip_if_maintenance()

        Returns:
            self for fluent interface
        """
        self._skip_if_maintenance = True
        self.options["skip_if_maintenance"] = True
        return self

    def timezone(self, tz: str) -> "ScheduleBuilder":
        """
        Set the timezone for this scheduled task (Laravel-style).

        The task will be executed at the specified time in this timezone.

        Args:
            tz: Timezone string (e.g., "America/New_York", "Europe/London")

        Example:
            scheduler.command("report:daily").daily(hour=9).timezone("America/New_York")

        Returns:
            self for fluent interface
        """
        self._timezone = tz
        self.options["timezone"] = tz
        return self

    def on_success(self, callback: Callable) -> "ScheduleBuilder":
        """
        Register a callback to run if the task succeeds (Laravel-style).

        Args:
            callback: Function to call on success, receives the task result

        Example:
            scheduler.command("send:emails").daily().on_success(lambda: Log.info("Emails sent"))

        Returns:
            self for fluent interface
        """
        self.options["on_success"] = callback
        return self

    def on_failure(self, callback: Callable) -> "ScheduleBuilder":
        """
        Register a callback to run if the task fails (Laravel-style).

        Args:
            callback: Function to call on failure, receives the exception

        Example:
            scheduler.command("send:emails").daily().on_failure(lambda e: Log.error(str(e)))

        Returns:
            self for fluent interface
        """
        self.options["on_failure"] = callback
        return self
