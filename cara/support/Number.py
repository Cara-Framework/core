"""Decimal-safe number utilities — coercion and safe division.

Generic math helpers for any application dealing with precise numeric
values (money, measurements, percentages). Floats are routed through
``str()`` before ``Decimal()`` to avoid binary-float drift.
"""

from __future__ import annotations

from decimal import Decimal, DivisionByZero, InvalidOperation
from typing import Any


def to_decimal(value: Any) -> Decimal:
    """Coerce ``value`` to ``Decimal``, returning ``Decimal('0')`` for
    None / invalid input.

    Accepts None, str, int, float, Decimal. Floats are routed through
    ``str()`` first so we never store the binary-float drift that
    ``Decimal(float)`` would introduce.
    """
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    try:
        if isinstance(value, float):
            return Decimal(str(value))
        return Decimal(value)
    except InvalidOperation, ValueError, TypeError:
        return Decimal("0")


def safe_divide_decimal(num: Any, den: Any) -> Decimal:
    """Decimal division that returns ``Decimal('0')`` when the divisor
    is zero (or coerces to zero) instead of raising ``ZeroDivisionError``.
    """
    n = to_decimal(num)
    d = to_decimal(den)
    if d == 0:
        return Decimal("0")
    try:
        return n / d
    except DivisionByZero, InvalidOperation:
        return Decimal("0")


__all__ = ["safe_divide_decimal", "to_decimal"]
