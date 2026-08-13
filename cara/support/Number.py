"""Decimal-safe number utilities — coercion and safe division.

Generic math helpers for applications dealing with precise numeric values.
Floats are routed through ``str()`` before ``Decimal()`` to avoid importing
binary-float drift. Invalid, boolean and non-finite values are never minted
into a plausible zero.
"""

from __future__ import annotations

import re
from decimal import Decimal, DivisionByZero, InvalidOperation
from typing import Any

_DECIMAL_TEXT_RE = re.compile(r"^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$")


def parse_decimal_text(value: Any) -> Decimal | None:
    """Parse an exact JSON decimal string; numbers/bools/exponents are refused."""
    if not isinstance(value, str) or not _DECIMAL_TEXT_RE.fullmatch(value):
        return None
    try:
        out = Decimal(value)
    except InvalidOperation:
        return None
    return out if out.is_finite() else None


def decimal_text(value: Decimal) -> str:
    """Render a validated Decimal without exponent notation or precision loss."""
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ValueError("a finite Decimal is required")
    return format(value, "f")


def to_decimal(value: Any) -> Decimal:
    """Coerce a finite numeric value to ``Decimal`` or raise ``ValueError``."""
    if value is None or isinstance(value, bool):
        raise ValueError("a finite numeric value is required")
    try:
        out = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError("a finite numeric value is required") from exc
    if not out.is_finite():
        raise ValueError("a finite numeric value is required")
    return out


def to_decimal_or_none(value: Any) -> Decimal | None:
    """Coerce ``value`` to ``Decimal``, returning ``None`` when it is
    absent or unparseable.

    The partial sibling of :func:`to_decimal`. Empty and whitespace-only
    strings — what a CSV column and a half-populated API payload
    actually deliver for a missing number — are unknown, not zero, and
    resolve to ``None`` rather than falling through ``Decimal("")``.
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, str) and not value.strip():
        return None
    try:
        out = value if isinstance(value, Decimal) else Decimal(str(value))
    except InvalidOperation, ValueError, TypeError:
        return None
    return out if out.is_finite() else None


def safe_divide_decimal(num: Any, den: Any) -> Decimal | None:
    """Decimal division that returns ``None`` when the divisor is zero
    (or coerces to zero) instead of raising ``ZeroDivisionError``.

    "Safe" means *does not raise*, not *always answers a number*. A
    ratio with a zero denominator is undefined, and the previous
    ``Decimal('0')`` claimed the opposite: a margin over zero revenue, a
    conversion rate over zero sessions and a fill rate over zero
    demanded units all reported a confident 0% that is arithmetically
    indistinguishable from a real, terrible 0% — and then averaged into
    the roll-up above it. Undefined is ``NULL``.

    Callers that genuinely want a numeric floor say so at the call site:
    ``safe_divide_decimal(a, b) or Decimal("0")``.
    """
    n = to_decimal_or_none(num)
    d = to_decimal_or_none(den)
    if n is None or d is None or d == 0:
        return None
    try:
        return n / d
    except DivisionByZero, InvalidOperation:
        return None


__all__ = [
    "decimal_text",
    "parse_decimal_text",
    "safe_divide_decimal",
    "to_decimal",
    "to_decimal_or_none",
]
