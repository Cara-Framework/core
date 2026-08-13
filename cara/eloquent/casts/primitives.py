"""
Primitive Cast Types for Cara ORM

Handles basic data types like bool, int, float, decimal.
"""

from __future__ import annotations


def _is_blank(value) -> bool:
    """A string carrying no non-whitespace characters is unknown, not zero.

    Shared by the numeric casts so ``""`` and ``"   "`` — what a CSV
    import and a half-populated API payload actually deliver for a
    missing number — resolve to ``NULL`` and not to a measured 0.
    """
    return isinstance(value, str) and not value.strip()
