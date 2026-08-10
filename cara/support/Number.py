"""Decimal-safe number utilities — coercion and safe division.

Generic math helpers for any application dealing with precise numeric
values (money, measurements, percentages). Floats are routed through
``str()`` before ``Decimal()`` to avoid binary-float drift.

Two coercions live here on purpose, and picking the wrong one is a
money bug:

* :func:`to_decimal` is **total** — it always answers a ``Decimal``,
  substituting ``0`` for anything it cannot parse. That is correct only
  where zero is the genuine additive identity for the caller (a running
  total seeded from optional parts), and it is what ``Money`` relies on
  to stay non-optional.
* :func:`to_decimal_or_none` is **partial** — unparseable input answers
  ``None``. Use it anywhere the value is a *measurement*: unknown is
  ``NULL``, never 0, because a fake zero is indistinguishable from a
  measured zero once it lands in a column and it averages into every
  report built on that column.
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

    This function is deliberately total. ``Money.__post_init__`` calls
    it unconditionally on a non-optional ``amount`` field, so handing
    back ``None`` here would mint a ``Money(None)`` that blows up at
    every downstream comparison instead of at construction. When you
    need to tell "unknown" apart from "zero", reach for
    :func:`to_decimal_or_none`.
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


def to_decimal_or_none(value: Any) -> Decimal | None:
    """Coerce ``value`` to ``Decimal``, returning ``None`` when it is
    absent or unparseable.

    The partial sibling of :func:`to_decimal`. Empty and whitespace-only
    strings — what a CSV column and a half-populated API payload
    actually deliver for a missing number — are unknown, not zero, and
    resolve to ``None`` rather than falling through ``Decimal("")``.
    """
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, str) and not value.strip():
        return None
    try:
        if isinstance(value, float):
            return Decimal(str(value))
        return Decimal(value)
    except InvalidOperation, ValueError, TypeError:
        return None


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
    n = to_decimal(num)
    d = to_decimal(den)
    if d == 0:
        return None
    try:
        return n / d
    except DivisionByZero, InvalidOperation:
        return None


__all__ = ["safe_divide_decimal", "to_decimal", "to_decimal_or_none"]
