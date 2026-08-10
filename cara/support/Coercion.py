"""Safe-coercion helpers — best-effort numeric conversion that never raises.

Project-agnostic framework utility: turn arbitrary input into an ``int`` /
``float`` (or ``None`` when it isn't numeric) without exception handling at
every call site.
"""

from __future__ import annotations

import math
from typing import Any


def safe_float(value: Any) -> float | None:
    """Return one finite real number, or ``None`` for invalid input.

    Python's ``float`` accepts booleans and the IEEE non-finite spellings
    (``NaN`` / ``Infinity``).  Those are not measurements: they poison
    ordering and arithmetic in every consumer, while ``NaN`` in particular
    makes both ``value < floor`` and ``value > ceiling`` false.  Keep the
    rejection here so callers cannot accidentally implement different
    financial and analytical boundaries.
    """
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except OverflowError, TypeError, ValueError:
        return None
    return parsed if math.isfinite(parsed) else None


def safe_int(value: Any) -> int | None:
    """Best-effort int coercion — returns *None* on non-numeric input."""
    if value is None:
        return None
    try:
        return int(value)
    except TypeError, ValueError:
        return None


__all__ = [
    "safe_float",
    "safe_int",
]
