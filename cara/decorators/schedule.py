"""
Schedule Decorator for the Cara framework.

This module provides a decorator for registering scheduled jobs in the application.
"""

from typing import Any


def scheduled(
    *,
    mode: str = "job",
    identifier: str = None,
    driver_name: str = None,
    cron: str = None,
    daily: tuple[int, int] = None,
    hourly: int = None,
    interval: tuple[int, int, int] = None,
    at: Any = None,
    weekly: tuple[Any, int, int] = None,
    timezone: str = None,
):
    """Decorator to attach scheduling metadata to a class or function."""

    def decorator(target: Any):
        specs = getattr(target, "_schedule_specs", [])
        provided = [
            cron is not None,
            daily is not None,
            hourly is not None,
            interval is not None,
            at is not None,
            weekly is not None,
        ]
        if sum(provided) != 1:
            raise ValueError(
                "Exactly one of cron/daily/hourly/interval/at/weekly must be provided."
            )

        spec: dict[str, Any] = {
            "mode": mode,
            "identifier": identifier,
            "driver_name": driver_name,
            "timezone": timezone,
        }
        if cron is not None:
            spec.update(
                {
                    "type": "cron",
                    "args": (cron,),
                    "kwargs": {},
                }
            )
        elif daily is not None:
            hour, minute = daily
            spec.update(
                {
                    "type": "daily",
                    "args": (),
                    "kwargs": {
                        "hour": hour,
                        "minute": minute,
                    },
                }
            )
        elif hourly is not None:
            spec.update(
                {
                    "type": "hourly",
                    "args": (),
                    "kwargs": {"minute": hourly},
                }
            )
        elif interval is not None:
            hours, minutes, seconds = interval
            spec.update(
                {
                    "type": "interval",
                    "args": (),
                    "kwargs": {
                        "hours": hours,
                        "minutes": minutes,
                        "seconds": seconds,
                    },
                }
            )
        elif at is not None:
            spec.update({"type": "at", "args": (at,), "kwargs": {}})
        elif weekly is not None:
            day_of_week, hour, minute = weekly
            spec.update(
                {
                    "type": "weekly",
                    "args": (day_of_week,),
                    "kwargs": {
                        "hour": hour,
                        "minute": minute,
                    },
                }
            )
        specs.append(spec)
        target._schedule_specs = specs
        return target

    return decorator
