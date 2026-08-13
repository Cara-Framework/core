"""Canonical definition of ``DateCast``."""

from __future__ import annotations

import pendulum

from .BaseCast import BaseCast


class DateCast(BaseCast):
    """Cast for date values."""

    def get(self, value):
        """Get as a canonical date string; invalid stored data is unknown."""
        if value is None:
            return None
        try:
            return pendulum.parse(str(value)).to_date_string()
        except ValueError, TypeError, OverflowError:
            return None

    def set(self, value):
        """Set a canonical date value; invalid input never reaches storage."""
        if value is None:
            return None
        try:
            return pendulum.parse(str(value)).to_date_string()
        except ValueError, TypeError, OverflowError:
            return None
