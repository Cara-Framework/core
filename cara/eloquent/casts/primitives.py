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
    """Cast to Decimal for high-precision arithmetic.

    The ``precision`` argument now actually does something: values are
    quantised to that many fractional digits on both ``get`` and
    ``set``. Previously the precision was stored but never applied —
    arithmetic ran at full input precision (``Decimal("12.345678901")``)
    and Postgres truncated on insert to the column's NUMERIC scale,
    so ``saved_value != original_value`` for any value with more
    fractional digits than the column allowed. Quantising at the cast
    boundary makes the round-trip exact.
    """

    def __init__(self, precision: int = 2):
        self.precision = int(precision)
        # Pre-build the quantum once; ``Decimal(10) ** -2`` is
        # ``Decimal("0.01")``. Used by both get / set.
        self._quantum: Decimal = Decimal(10) ** -self.precision

    def _quantize(self, dec: Decimal) -> Decimal:
        try:
            return dec.quantize(self._quantum)
        except InvalidOperation:
            # Value can't be represented at the requested precision —
            # leave it unquantised rather than corrupt it; the DB
            # NUMERIC scale will still truncate, but at least we don't
            # silently round to ``None``.
            return dec

    def get(self, value):
        if value is None:
            return None
        if isinstance(value, Decimal):
            return self._quantize(value)
        try:
            return self._quantize(Decimal(str(value)))
        except (ValueError, TypeError, InvalidOperation):
            return None

    def set(self, value):
        if value is None or str(value).strip() == "":
            return None
        try:
            return self._quantize(Decimal(str(value)))
        except (ValueError, TypeError, InvalidOperation):
            return None


class JsonCast(BaseCast):
    """Cast to/from JSON."""

    def get(self, value):
        """Get as parsed JSON."""
        if value is None:
            return None
        if isinstance(value, str):
            # Empty/whitespace strings are null-equivalent — they
            # come back as NULL from many DB schemas via empty-string
            # default. Treat them as None instead of failing JSON
            # parse and silently producing None anyway.
            if not value.strip():
                return None
            try:
                return json.loads(value)
            except (ValueError, TypeError):
                return None
        return value

    def set(self, value):
        """Set as JSON string.

        Empty string is null-equivalent — previously ``set("")`` fell
        through to ``json.dumps("")`` and produced the literal JSON
        string ``'""'``. The next ``get()`` then returned the empty
        string instead of ``None``, breaking ``if obj.field is None``
        checks all over the call site. Now empty becomes NULL.
        """
        if value is None:
            return None
        if isinstance(value, str) and not value.strip():
            return None

        try:
            # If it's already a string, validate it's valid JSON;
            # invalid → treat as a literal value to encode.
            if isinstance(value, str):
                json.loads(value)
                return value
            return json.dumps(value, default=str, ensure_ascii=False)
        except (ValueError, TypeError):
            return json.dumps(value, default=str, ensure_ascii=False)
