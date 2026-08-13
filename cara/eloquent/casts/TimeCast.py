"""Canonical definition of ``TimeCast``."""

from __future__ import annotations

from datetime import datetime, time

import pendulum

from .BaseCast import BaseCast


class TimeCast(BaseCast):
    """Cast for time values."""

    def get(self, value):
        """Get time value as time string in HH:MM:SS format."""
        if value is None:
            return None

        try:
            if isinstance(value, time):
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
        except ValueError, TypeError, OverflowError, AttributeError:
            return None

    def set(self, value):
        """Set time value."""
        if value is None:
            return None

        try:
            if isinstance(value, time):
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
        except ValueError, TypeError, OverflowError, AttributeError:
            return None
