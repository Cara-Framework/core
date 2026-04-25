"""
Primitive Cast Types for Cara ORM

Handles basic data types like bool, int, float, decimal.
"""

import json
from decimal import Decimal, InvalidOperation

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
    """Cast to integer.

    Preserves ``None`` as ``None`` — SQL NULL must not silently collapse to
    0, because nullable integer columns that happen to be foreign keys
    (e.g. ``product_container.brand_id``) would then point at a
    non-existent row and trip FK violations downstream. Previously this
    cast returned 0 for any non-numeric input including ``None``, which
    caused ``ConsolidateProductRecordJob`` to insert ``brand_id=0`` and
    hit ``fk_product_brand_id`` when the scraped brand failed to resolve.
    """

    def get(self, value):
        """Get as integer, preserving ``None`` for SQL NULL."""
        if value is None:
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return 0

    def set(self, value):
        """Set as integer, preserving ``None`` for SQL NULL."""
        if value is None:
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return 0


class FloatCast(BaseCast):
    """Cast to float. ``None`` passes through — SQL NULL stays NULL."""

    def get(self, value):
        """Get as float, preserving ``None``."""
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0

    def set(self, value):
        """Set as float, preserving ``None``."""
        if value is None:
            return None
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
        """Get as Decimal for high precision arithmetic."""
        if value is None:
            return None

        if isinstance(value, Decimal):
            return value

        try:
            return Decimal(str(value))
        except (ValueError, TypeError, InvalidOperation):
            return None

    def set(self, value):
        """Set as Decimal."""
        # Handle None and empty values
        if value is None or str(value).strip() == "":
            return None

        try:
            return Decimal(str(value))
        except (ValueError, TypeError, InvalidOperation):
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
