"""Canonical definition of ``FloatCast``."""

from __future__ import annotations

import math

from .BaseCast import BaseCast
from .primitives import _is_blank


class FloatCast(BaseCast):
    """Cast to float. ``None`` passes through — SQL NULL stays NULL.

    Unparseable input is ``None`` too, for the same reason ``IntCast``
    above stopped returning 0: a fake zero is indistinguishable from a
    measured zero once it lands in a column, and it averages into every
    report built on that column. ``float`` is for measurements — money
    belongs in ``DecimalCast``.
    """

    def get(self, value):
        """Get as float, preserving ``None`` for SQL NULL / unparseable."""
        if value is None or _is_blank(value):
            return None
        if isinstance(value, bool):
            return None
        try:
            parsed = float(value)
        except ValueError, TypeError:
            return None
        return parsed if math.isfinite(parsed) else None

    def set(self, value):
        """Set as float, preserving ``None`` for SQL NULL / unparseable."""
        if value is None or _is_blank(value):
            return None
        if isinstance(value, bool):
            return None
        try:
            parsed = float(value)
        except ValueError, TypeError:
            return None
        return parsed if math.isfinite(parsed) else None
