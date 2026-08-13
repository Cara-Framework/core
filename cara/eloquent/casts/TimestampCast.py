"""Canonical definition of ``TimestampCast``."""

from __future__ import annotations

from datetime import datetime

import pendulum

from .BaseCast import BaseCast


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
        except ValueError, TypeError, OverflowError:
            return None

    def set(self, value):
        """Set from timestamp or datetime."""
        if value is None:
            return None

        try:
            if isinstance(value, (int, float)):
                return pendulum.from_timestamp(value).to_datetime_string()
            return self.get(value)
        except ValueError, TypeError, OverflowError:
            return None
