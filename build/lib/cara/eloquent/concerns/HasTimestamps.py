"""
HasTimestamps Concern

Single Responsibility: Handle timestamp operations for Eloquent models.
Clean and simple date management following DRY and KISS principles.
"""

from datetime import datetime
from typing import Optional

import pendulum

from cara.environment import env


class HasTimestamps:
    """
    Mixin for handling model timestamps.

    This concern handles:
    - Automatic timestamp creation/updating
    - Touch functionality
    - Clean date formatting
    - Timezone management
    """

    # Default timestamp fields
    date_created_at = "created_at"
    date_updated_at = "updated_at"
    __timestamps__ = True
    __timezone__ = "UTC"

    def touch(self, date: Optional[datetime] = None, query: bool = True) -> bool:
        """Touch the model's timestamps."""
        if not self.__timestamps__:
            return False

        self._update_timestamps(date)

        if query:
            return self.save()

        return True

    def _update_timestamps(self, date: Optional[datetime] = None) -> None:
        """Update the model's timestamps."""
        current_time = date or self._current_timestamp()

        if hasattr(self, self.date_updated_at):
            setattr(self, self.date_updated_at, current_time)

    def _current_timestamp(self) -> str:
        """Get current timestamp in UTC."""
        return pendulum.now("UTC").to_datetime_string()

    def get_new_datetime_string(self, _datetime: Optional[datetime] = None) -> str:
        """Get a new datetime string in the correct format."""
        if _datetime is None:
            _datetime = pendulum.now("UTC")
        elif isinstance(_datetime, str):
            _datetime = pendulum.parse(_datetime)
        elif isinstance(_datetime, datetime):
            _datetime = pendulum.instance(_datetime)

        return _datetime.in_timezone("UTC").to_datetime_string()

    def get_new_serialized_date(self, _datetime: datetime) -> str:
        """Get a serialized date string."""
        if isinstance(_datetime, str):
            return _datetime

        if isinstance(_datetime, datetime):
            _datetime = pendulum.instance(_datetime)

        # Get application timezone for display
        app_timezone = env("APP_TIMEZONE", "UTC")
        return _datetime.in_timezone(app_timezone).to_datetime_string()

    def freshTimestamp(self) -> str:
        """Get a fresh timestamp."""
        return self._current_timestamp()

    def get_dates(self) -> list:
        """Get the attributes that should be converted to dates."""
        dates = getattr(self, "__dates__", [])

        if self.__timestamps__:
            dates.extend([self.date_created_at, self.date_updated_at])

        return list(set(dates))  # Remove duplicates

    def is_date_field(self, attribute: str) -> bool:
        """Check if an attribute is a date field."""
        return attribute in self.get_dates()

    def set_created_at(self, value: datetime) -> None:
        """Set the created_at timestamp."""
        if self.__timestamps__:
            setattr(self, self.date_created_at, self.get_new_datetime_string(value))

    def set_updated_at(self, value: datetime) -> None:
        """Set the updated_at timestamp."""
        if self.__timestamps__:
            setattr(self, self.date_updated_at, self.get_new_datetime_string(value))

    def _should_update_timestamps(self) -> bool:
        """Determine if timestamps should be updated."""
        return self.__timestamps__ and not getattr(self, "_skip_timestamps", False)

    def skip_timestamps(self) -> "HasTimestamps":
        """Skip timestamp updates for this operation."""
        self._skip_timestamps = True
        return self

    def enable_timestamps(self) -> "HasTimestamps":
        """Enable timestamp updates."""
        self._skip_timestamps = False
        return self
