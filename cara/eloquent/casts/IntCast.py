"""Canonical definition of ``IntCast``."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation

from .BaseCast import BaseCast
from .primitives import _is_blank


class IntCast(BaseCast):
    """Cast to integer.

    Preserves ``None`` as ``None`` — SQL NULL must not silently collapse to
    0, because nullable integer columns that happen to be foreign keys
    (e.g. ``child.parent_id``) would then point at a
    non-existent row and trip FK violations downstream. Previously this
    cast returned 0 for any non-numeric input including ``None``, which
    caused a job to insert ``parent_id=0`` and
    hit ``fk_child_parent_id`` when the referenced row failed to resolve.

    That fix stopped one input short: it special-cased ``None`` but left
    the ``except`` arms returning 0, so ``""``, ``"   "`` and ``"N/A"``
    — the shapes a CSV import and a half-populated API payload actually
    deliver — kept minting the same ``parent_id=0``. Unparseable is
    unknown, and unknown is ``NULL``. ``DecimalCast.set`` below got the
    empty/whitespace guard right first; it is now the shared
    ``_is_blank`` helper so the three numeric casts cannot drift apart
    again.
    """

    def get(self, value):
        """Get as integer, preserving ``None`` for SQL NULL / unparseable."""
        if value is None or _is_blank(value):
            return None
        if isinstance(value, bool):
            return None
        try:
            parsed = Decimal(str(value))
        except ValueError, TypeError, InvalidOperation:
            return None
        return (
            int(parsed) if parsed.is_finite() and parsed == parsed.to_integral() else None
        )

    def set(self, value):
        """Set as integer, preserving ``None`` for SQL NULL / unparseable."""
        if value is None or _is_blank(value):
            return None
        if isinstance(value, bool):
            return None
        try:
            parsed = Decimal(str(value))
        except ValueError, TypeError, InvalidOperation:
            return None
        return (
            int(parsed) if parsed.is_finite() and parsed == parsed.to_integral() else None
        )
