"""Safe-coercion helpers — best-effort conversion that never raises.

Project-agnostic framework utility: turn arbitrary input into an ``int`` /
``float`` / ``bool`` (or ``None`` when it isn't one) without exception
handling at every call site.
"""

from __future__ import annotations

import math
from typing import Any

# The ONE boolean vocabulary: exactly what the ``boolean`` validation rule
# accepts (case-insensitive strings, the integers 0/1, real booleans). The
# rule validates with it and ``Request.boolean()`` converts with it, so a
# value the rule let through can never be read the other way round.
BOOLEAN_TRUE_TOKENS = frozenset({"true", "1", "yes"})
BOOLEAN_FALSE_TOKENS = frozenset({"false", "0", "no"})


def safe_bool(value: Any) -> bool | None:
    """The boolean a payload value spells, or ``None`` when it spells none.

    ``bool("false")`` is ``True``: a validated ``"false"`` / ``"0"`` handed to
    Python truthiness flips the caller's meaning. This is the conversion the
    ``boolean`` rule's vocabulary implies — ``None`` (not ``False``) for
    anything outside it, so a caller can tell "absent/garbage" from "false".
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        if value in (0, 1):
            return value == 1
        return None
    if isinstance(value, str):
        token = value.lower()
        if token in BOOLEAN_TRUE_TOKENS:
            return True
        if token in BOOLEAN_FALSE_TOKENS:
            return False
    return None


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
    "BOOLEAN_FALSE_TOKENS",
    "BOOLEAN_TRUE_TOKENS",
    "safe_bool",
    "safe_float",
    "safe_int",
]
