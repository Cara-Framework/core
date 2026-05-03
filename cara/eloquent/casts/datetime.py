"""
DateTime Cast Types for Cara ORM

Provides comprehensive date and time handling with timezone support.
"""

from datetime import datetime
from typing import Optional

import pendulum

from .base import BaseCast


class DateCast(BaseCast):
    """Cast for date values."""

    def get(self, value):
        """Get as date string."""
        if value is None:
            return None
        try:
            return pendulum.parse(str(value)).to_date_string()
        except Exception:
            return str(value) if value else None

    def set(self, value):
        """Set date value."""
        if value is None:
            return None
        try:
            return pendulum.parse(str(value)).to_date_string()
        except Exception:
            return str(value) if value else None


class DateTimeCast(BaseCast):
    """
    Advanced DateTime cast with timezone support and custom formats.

    Format codes (Pendulum compatible):
    - YYYY: 4-digit year
    - MM: 2-digit month
    - DD: 2-digit day
    - HH: 2-digit hour (24h)
    - mm: 2-digit minute
    - ss: 2-digit second
    """

    # Format mapping from common formats to Pendulum
    FORMAT_MAP = {
        "Y-m-d H:i:s": "YYYY-MM-DD HH:mm:ss",
        "Y-m-d": "YYYY-MM-DD",
        "d/m/Y": "DD/MM/YYYY",
        "m/d/Y": "MM/DD/YYYY",
        "H:i:s": "HH:mm:ss",
        "H:i": "HH:mm",
        "c": None,  # ISO 8601 format
        # Additional formats
        "d/m/Y H:i:s": "DD/MM/YYYY HH:mm:ss",  # 26/06/2025 21:53:03
        "H:i:s d.m.Y": "HH:mm:ss DD.MM.YYYY",  # 21:53:03 26.06.2025
    }

    def __init__(self, format_string: Optional[str] = None, timezone: str = "UTC"):
        self.format_string = self._convert_format(format_string)
        self.timezone = timezone

    def _convert_format(self, format_str: Optional[str]) -> Optional[str]:
        """Convert PHP-style format to Pendulum format."""
        if not format_str:
            return None
        return self.FORMAT_MAP.get(format_str, format_str)

    def get(self, value):
        """Get datetime value as datetime object in application timezone."""
        if value is None:
            return None

        try:
            if isinstance(value, str):
                dt = pendulum.parse(value, tz="UTC")  # Database always UTC
            elif isinstance(value, datetime):
                dt = pendulum.instance(value, tz="UTC")  # Database always UTC
            else:
                dt = pendulum.parse(str(value), tz="UTC")  # Database always UTC

            # Convert to application timezone from config
            from cara.environment import env

            app_timezone = env("APP_TIMEZONE", "UTC")
            dt = dt.in_timezone(app_timezone)

            # ``pendulum.DateTime`` IS a ``datetime.datetime`` subclass,
            # so returning it directly satisfies any ``isinstance(x,
            # datetime)`` check downstream and keeps the timezone
            # attached. The legacy ``hasattr(dt, "to_datetime")`` fallback
            # to ``datetime.fromtimestamp(dt.timestamp())`` produced a
            # NAIVE LOCAL-TIME datetime — TypeError when compared against
            # ``pendulum.now("UTC")`` and silent local/UTC drift.
            return dt
        except Exception:
            # If parsing fails, try to return as string for backwards compatibility
            return str(value) if value else None

    def set(self, value):
        """Set datetime value from various input formats.

        Naive datetimes (no tzinfo) are interpreted in
        ``APP_TIMEZONE`` — Pendulum defaults a bare ``datetime(...)``
        to UTC, so a Spain-local timestamp passed in naive used to be
        stored as if it was already UTC, off by 1-2 hours. The DB
        column is always written in UTC; only the *interpretation* of
        a naive input changes.
        """
        if value is None:
            return None

        try:
            from cara.environment import env

            app_timezone = env("APP_TIMEZONE", "UTC")

            if isinstance(value, datetime):
                if value.tzinfo is None:
                    # Naive — interpret in APP_TIMEZONE.
                    dt = pendulum.instance(value, tz=app_timezone)
                else:
                    dt = pendulum.instance(value)
            else:
                s = str(value)
                # Cheap heuristic for "this string carries timezone
                # info" — if it ends in Z, +HH:MM, -HH:MM, etc.
                has_tz = bool(s) and (
                    s.endswith("Z")
                    or any(ch in s[10:] for ch in ("+", "-"))
                )
                dt = pendulum.parse(s, tz=None if has_tz else app_timezone)

            return dt.in_timezone("UTC").to_datetime_string()
        except Exception:
            return str(value) if value else None


class TimestampCast(BaseCast):
    """Cast to Unix timestamp."""

    def get(self, value):
        """Get as Unix timestamp integer."""
        if value is None:
            return None

        try:
            if isinstance(value, (int, float)):
                return int(value)

            if isinstance(value, str):
                dt = pendulum.parse(value)
            elif isinstance(value, datetime):
                dt = pendulum.instance(value)
            else:
                dt = pendulum.parse(str(value))

            return int(dt.timestamp())
        except Exception:
            return None

    def set(self, value):
        """Set from timestamp or datetime."""
        if value is None:
            return None

        try:
            if isinstance(value, (int, float)):
                return pendulum.from_timestamp(value).to_datetime_string()
            return self.get(value)
        except Exception:
            return None


class TimeCast(BaseCast):
    """Cast for time values."""

    def get(self, value):
        """Get time value as time string in HH:MM:SS format."""
        if value is None:
            return None

        try:
            from datetime import time as dt_time

            if isinstance(value, dt_time):
                # Convert time object to string
                return value.strftime("%H:%M:%S")
            elif isinstance(value, str):
                # Parse string and return as formatted time string
                if ":" in value:
                    # Already a time string, validate and reformat
                    time_obj = pendulum.parse(f"2000-01-01 {value}").time()
                    return time_obj.strftime("%H:%M:%S")
                else:
                    # Try to parse as full datetime and extract time
                    dt = pendulum.parse(value)
                    return dt.time().strftime("%H:%M:%S")
            elif isinstance(value, datetime):
                # Extract time from datetime
                return value.time().strftime("%H:%M:%S")
            else:
                # Try to parse as string
                return self.get(str(value))
        except Exception:
            # If all parsing fails, return as string
            return str(value) if value else None

    def set(self, value):
        """Set time value."""
        if value is None:
            return None

        try:
            from datetime import time as dt_time

            if isinstance(value, dt_time):
                return value.strftime("%H:%M:%S")
            elif isinstance(value, str):
                # Parse string to validate and reformat
                if ":" in value:
                    # Parse as time string
                    time_obj = pendulum.parse(f"2000-01-01 {value}").time()
                    return time_obj.strftime("%H:%M:%S")
                else:
                    # Try to parse as full datetime and extract time
                    dt = pendulum.parse(value)
                    return dt.time().strftime("%H:%M:%S")
            elif isinstance(value, datetime):
                # Extract time from datetime
                return value.time().strftime("%H:%M:%S")
            else:
                # Try to parse as string
                return self.set(str(value))
        except Exception:
            return str(value) if value else None
