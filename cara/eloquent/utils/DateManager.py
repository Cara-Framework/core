"""
DateManager - Clean, centralized date handling for Eloquent ORM

Single responsibility: Handle all date-related operations cleanly and efficiently.
Follows DRY and KISS principles for date management.
"""

from datetime import datetime
from typing import Any, Optional, Union

import pendulum


class DateManager:
    """
    Centralized date management for Eloquent ORM.

    Handles:
    - Timezone conversions
    - Date formatting
    - Timestamp generation
    - Date parsing
    - Clean date operations
    """

    DEFAULT_TIMEZONE = "UTC"
    DEFAULT_FORMAT = "Y-m-d H:i:s"

    # Supported date formats for parsing
    SUPPORTED_FORMATS = [
        "%Y-%m-%d %H:%M:%S",  # 2023-12-01 15:30:45
        "%Y-%m-%d %H:%M:%S.%f",  # 2023-12-01 15:30:45.123456
        "%Y-%m-%d",  # 2023-12-01
        "%Y/%m/%d",  # 2023/12/01
        "%d/%m/%Y",  # 01/12/2023
        "%d-%m-%Y",  # 01-12-2023
        "%Y-%m-%dT%H:%M:%S",  # ISO format without timezone
        "%Y-%m-%dT%H:%M:%S.%f",  # ISO format with microseconds
        "%Y-%m-%dT%H:%M:%SZ",  # ISO format with Z timezone
    ]

    @classmethod
    def now(cls, timezone_str: Optional[str] = None) -> pendulum.DateTime:
        """
        Get current timestamp in specified timezone.

        Args:
            timezone_str: Timezone string (defaults to DEFAULT_TIMEZONE)

        Returns:
            Current timestamp as pendulum DateTime
        """
        timezone_str = timezone_str or cls.DEFAULT_TIMEZONE
        return pendulum.now(timezone_str)

    @classmethod
    def parse(
        cls, date_value: Any, timezone_str: Optional[str] = None
    ) -> Optional[pendulum.DateTime]:
        """
        Parse a date value into a pendulum DateTime.

        Args:
            date_value: Value to parse (string, datetime, timestamp, etc.)
            timezone_str: Target timezone

        Returns:
            Parsed pendulum DateTime or None if parsing fails
        """
        if date_value is None:
            return None

        timezone_str = timezone_str or cls.DEFAULT_TIMEZONE

        # Already a pendulum DateTime
        if isinstance(date_value, pendulum.DateTime):
            return date_value.in_timezone(timezone_str)

        # Standard datetime object
        if isinstance(date_value, datetime):
            return pendulum.instance(date_value).in_timezone(timezone_str)

        # String parsing
        if isinstance(date_value, str):
            return cls._parse_string(date_value, timezone_str)

        # Numeric timestamp
        if isinstance(date_value, (int, float)):
            return pendulum.from_timestamp(date_value, tz=timezone_str)

        return None

    @classmethod
    def format(
        cls,
        date_value: Union[pendulum.DateTime, datetime, str, int, float],
        format_str: str = None,
        timezone_str: Optional[str] = None,
    ) -> Optional[str]:
        """
        Format a date value to string.

        Args:
            date_value: Date to format
            format_str: Format string (Laravel style: Y-m-d H:i:s)
            timezone_str: Target timezone

        Returns:
            Formatted date string or None
        """
        parsed_date = cls.parse(date_value, timezone_str)
        if not parsed_date:
            return None

        format_str = format_str or cls.DEFAULT_FORMAT

        # Convert Laravel format to Python format
        python_format = cls._laravel_to_python_format(format_str)

        return parsed_date.strftime(python_format)

    @classmethod
    def to_database_format(
        cls, date_value: Any, timezone_str: Optional[str] = None
    ) -> Optional[str]:
        """
        Convert date to database storage format.

        Args:
            date_value: Date to convert
            timezone_str: Source timezone

        Returns:
            Database-formatted string
        """
        return cls.format(date_value, "Y-m-d H:i:s", timezone_str)

    @classmethod
    def to_iso_format(
        cls, date_value: Any, timezone_str: Optional[str] = None
    ) -> Optional[str]:
        """
        Convert date to ISO format.

        Args:
            date_value: Date to convert
            timezone_str: Source timezone

        Returns:
            ISO-formatted string
        """
        parsed_date = cls.parse(date_value, timezone_str)
        if not parsed_date:
            return None

        return parsed_date.to_iso8601_string()

    @classmethod
    def add_days(
        cls, date_value: Any, days: int, timezone_str: Optional[str] = None
    ) -> Optional[pendulum.DateTime]:
        """
        Add days to a date.

        Args:
            date_value: Base date
            days: Number of days to add
            timezone_str: Timezone

        Returns:
            New date with days added
        """
        parsed_date = cls.parse(date_value, timezone_str)
        if not parsed_date:
            return None

        return parsed_date.add(days=days)

    @classmethod
    def subtract_days(
        cls, date_value: Any, days: int, timezone_str: Optional[str] = None
    ) -> Optional[pendulum.DateTime]:
        """
        Subtract days from a date.

        Args:
            date_value: Base date
            days: Number of days to subtract
            timezone_str: Timezone

        Returns:
            New date with days subtracted
        """
        parsed_date = cls.parse(date_value, timezone_str)
        if not parsed_date:
            return None

        return parsed_date.subtract(days=days)

    @classmethod
    def diff_in_days(
        cls, date1: Any, date2: Any, timezone_str: Optional[str] = None
    ) -> Optional[int]:
        """
        Get difference between two dates in days.

        Args:
            date1: First date
            date2: Second date
            timezone_str: Timezone

        Returns:
            Difference in days
        """
        parsed_date1 = cls.parse(date1, timezone_str)
        parsed_date2 = cls.parse(date2, timezone_str)

        if not parsed_date1 or not parsed_date2:
            return None

        return parsed_date1.diff(parsed_date2).in_days()

    @classmethod
    def is_past(
        cls, date_value: Any, timezone_str: Optional[str] = None
    ) -> Optional[bool]:
        """
        Check if date is in the past.

        Args:
            date_value: Date to check
            timezone_str: Timezone

        Returns:
            True if in past, False if future, None if invalid
        """
        parsed_date = cls.parse(date_value, timezone_str)
        if not parsed_date:
            return None

        return parsed_date < cls.now(timezone_str)

    @classmethod
    def is_future(
        cls, date_value: Any, timezone_str: Optional[str] = None
    ) -> Optional[bool]:
        """
        Check if date is in the future.

        Args:
            date_value: Date to check
            timezone_str: Timezone

        Returns:
            True if in future, False if past, None if invalid
        """
        parsed_date = cls.parse(date_value, timezone_str)
        if not parsed_date:
            return None

        return parsed_date > cls.now(timezone_str)

    @classmethod
    def is_today(
        cls, date_value: Any, timezone_str: Optional[str] = None
    ) -> Optional[bool]:
        """
        Check if date is today.

        Args:
            date_value: Date to check
            timezone_str: Timezone

        Returns:
            True if today, False otherwise, None if invalid
        """
        parsed_date = cls.parse(date_value, timezone_str)
        if not parsed_date:
            return None

        today = cls.now(timezone_str).start_of("day")
        check_date = parsed_date.start_of("day")

        return today == check_date

    # ===== Private Helper Methods =====

    @classmethod
    def _parse_string(
        cls, date_string: str, timezone_str: str
    ) -> Optional[pendulum.DateTime]:
        """Parse a string date using multiple format attempts."""
        date_string = date_string.strip()

        # Try pendulum's built-in parsing first
        try:
            return pendulum.parse(date_string, tz=timezone_str)
        except (ValueError, TypeError):
            pass

        # Try each supported format
        for fmt in cls.SUPPORTED_FORMATS:
            try:
                dt = datetime.strptime(date_string, fmt)
                return pendulum.instance(dt).in_timezone(timezone_str)
            except ValueError:
                continue

        return None

    @classmethod
    def _laravel_to_python_format(cls, laravel_format: str) -> str:
        """
        Convert Laravel date format to Python strftime format.

        Laravel format reference:
        Y = 4-digit year
        m = month with leading zero
        d = day with leading zero
        H = 24-hour format hour
        i = minutes
        s = seconds
        """
        format_map = {
            "Y": "%Y",  # 4-digit year
            "y": "%y",  # 2-digit year
            "m": "%m",  # Month with leading zero
            "n": "%m",  # Month without leading zero (not directly supported)
            "d": "%d",  # Day with leading zero
            "j": "%d",  # Day without leading zero (not directly supported)
            "H": "%H",  # 24-hour format
            "h": "%I",  # 12-hour format
            "i": "%M",  # Minutes
            "s": "%S",  # Seconds
            "A": "%p",  # AM/PM
            "a": "%p",  # am/pm (will be uppercase)
        }

        python_format = laravel_format
        for laravel_char, python_char in format_map.items():
            python_format = python_format.replace(laravel_char, python_char)

        return python_format

    # ===== Timezone Conversion Methods =====

    @classmethod
    def to_user_timezone(
        cls, date_value: Any, user_timezone: str = None
    ) -> Optional[pendulum.DateTime]:
        """
        Convert UTC date to user timezone for display.

        Args:
            date_value: UTC date from database
            user_timezone: User's timezone (from config/request)

        Returns:
            Date converted to user timezone
        """
        if date_value is None:
            return None

        # Import here to avoid circular imports
        try:
            from config.app import APP_TIMEZONE

            user_timezone = user_timezone or APP_TIMEZONE
        except ImportError:
            user_timezone = user_timezone or "UTC"

        # Parse as UTC first (database format)
        parsed_date = cls.parse(date_value, "UTC")
        if not parsed_date:
            return None

        # Convert to user timezone
        return parsed_date.in_timezone(user_timezone)

    @classmethod
    def to_utc_for_database(
        cls, date_value: Any, user_timezone: str = None
    ) -> Optional[pendulum.DateTime]:
        """
        Convert user timezone date to UTC for database storage.

        Args:
            date_value: Date in user timezone
            user_timezone: User's timezone (from config/request)

        Returns:
            Date converted to UTC
        """
        if date_value is None:
            return None

        # Import here to avoid circular imports
        try:
            from config.app import APP_TIMEZONE

            user_timezone = user_timezone or APP_TIMEZONE
        except ImportError:
            user_timezone = user_timezone or "UTC"

        # Parse in user timezone
        parsed_date = cls.parse(date_value, user_timezone)
        if not parsed_date:
            return None

        # Convert to UTC for database
        return parsed_date.in_timezone("UTC")

    @classmethod
    def format_for_api(
        cls, date_value: Any, user_timezone: str = None, format_str: str = None
    ) -> Optional[str]:
        """
        Format date for API response in user timezone.

        Args:
            date_value: UTC date from database
            user_timezone: User's timezone
            format_str: Format string

        Returns:
            Formatted date string in user timezone
        """
        user_date = cls.to_user_timezone(date_value, user_timezone)
        if not user_date:
            return None

        format_str = format_str or cls.DEFAULT_FORMAT
        python_format = cls._laravel_to_python_format(format_str)
        return user_date.strftime(python_format)
