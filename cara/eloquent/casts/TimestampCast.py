"""Canonical definition of ``TimestampCast``."""

from __future__ import annotations

from datetime import datetime

import pendulum

from .BaseCast import BaseCast
from .DateTimeCast import DateTimeCast


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
        """The UTC wall string of an epoch number or an AWARE date-time.

        An epoch number is an instant by definition. Anything else goes
        through ``DateTimeCast.instant`` — the one write rule — so a naive
        value is refused (``NaiveDateTimeException``) instead of being
        read as UTC, and every input lands as the same ``timestamptz``
        literal (it used to return the epoch INTEGER for a datetime).
        """
        if value is None:
            return None

        try:
            if isinstance(value, (int, float)):
                return DateTimeCast.sql_instant(pendulum.from_timestamp(value))
            return DateTimeCast.sql_instant(DateTimeCast.instant(value))
        except ValueError, TypeError, OverflowError:
            return None
