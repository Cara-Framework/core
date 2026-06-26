"""Safe-coercion helpers — best-effort numeric conversion that never raises.

Project-agnostic framework utility: turn arbitrary input into an ``int`` /
``float`` (or ``None`` when it isn't numeric) without exception handling at
every call site.
"""

from __future__ import annotations

from typing import Any


def safe_float(value: Any) -> float | None:
    """Best-effort float coercion — returns *None* on non-numeric input."""
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def safe_int(value: Any) -> int | None:
    """Best-effort int coercion — returns *None* on non-numeric input."""
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


__all__ = [
    "safe_float",
    "safe_int",
]
