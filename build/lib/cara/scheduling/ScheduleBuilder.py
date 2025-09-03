"""
Schedule Builder for the Cara framework.

This module provides utilities for building and configuring job schedules in a fluent interface.
"""


class ScheduleBuilder:
    def __init__(self, driver, identifier, callback, options):
        self.driver = driver
        self.identifier = identifier
        self.callback = callback
        self.options = options or {}

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
