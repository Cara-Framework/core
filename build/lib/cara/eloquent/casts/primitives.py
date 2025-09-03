"""
Primitive Cast Types for Cara ORM

Handles basic data types like bool, int, float, decimal.
"""

import json
from decimal import Decimal

from .base import BaseCast


class BoolCast(BaseCast):
    """Cast to boolean."""

    def get(self, value):
        """Get as boolean."""
        return bool(value)

    def set(self, value):
        """Set as boolean."""
        return bool(value)


class IntCast(BaseCast):
    """Cast to integer."""

    def get(self, value):
        """Get as integer."""
        try:
            return int(value)
        except (ValueError, TypeError):
            return 0

    def set(self, value):
        """Set as integer."""
        try:
            return int(value)
        except (ValueError, TypeError):
            return 0


class FloatCast(BaseCast):
    """Cast to float."""

    def get(self, value):
        """Get as float."""
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0

    def set(self, value):
        """Set as float."""
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0


class DecimalCast(BaseCast):
    """Cast to Decimal for high precision arithmetic."""

    def __init__(self, precision: int = 2):
        """Initialize with precision."""
        self.precision = int(precision)

    def get(self, value):
        """Get as string representation of Decimal with precision."""
        if value is None or value == 0:
            return None  # Return null for 0 values

        if isinstance(value, Decimal):
            formatted = f"{float(value):.{self.precision}f}"
        else:
            try:
                decimal_val = Decimal(str(value))
                formatted = f"{float(decimal_val):.{self.precision}f}"
            except (ValueError, TypeError):
                return None

        # Return as float for JSON compatibility
        return float(formatted)

    def set(self, value):
        """Set as Decimal."""
        try:
            return Decimal(str(value))
        except (ValueError, TypeError):
            return Decimal("0")


class JsonCast(BaseCast):
    """Cast to/from JSON."""

    def get(self, value):
        """Get as parsed JSON."""
        if isinstance(value, str):
            try:
                return json.loads(value)
            except (ValueError, TypeError):
                return None
        return value

    def set(self, value):
        """Set as JSON string."""
        if value is None:
            return None

        # Always convert to JSON string, even if value is already a string
        # This ensures consistent behavior with PostgreSQL TEXT fields
        try:
            # If it's already a string, try to parse it first
            if isinstance(value, str):
                # Validate it's valid JSON
                json.loads(value)
                return value
            else:
                # Convert dict/list/other to JSON string
                return json.dumps(value, default=str, ensure_ascii=False)
        except (ValueError, TypeError):
            # If parsing fails, convert to JSON string
            return json.dumps(value, default=str, ensure_ascii=False)
