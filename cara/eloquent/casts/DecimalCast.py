"""Canonical definition of ``DecimalCast``."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation

from .BaseCast import BaseCast


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

    def _quantize(self, dec: Decimal) -> Decimal | None:
        if not dec.is_finite():
            return None
        try:
            return dec.quantize(self._quantum)
        except InvalidOperation:
            return None

    def get(self, value):
        if value is None:
            return None
        if isinstance(value, bool):
            return None
        if isinstance(value, Decimal):
            return self._quantize(value)
        try:
            return self._quantize(Decimal(str(value)))
        except ValueError, TypeError, InvalidOperation:
            return None

    def set(self, value):
        if value is None or str(value).strip() == "":
            return None
        if isinstance(value, bool):
            return None
        # A float converts through its shortest repr — exact for every
        # literal a caller writes (1.0, 0.85). Money boundaries refuse
        # floats loudly (require_currency/round_money); the generic cast
        # must never turn a set value into a silent NULL.
        try:
            return self._quantize(Decimal(str(value)))
        except ValueError, TypeError, InvalidOperation:
            return None
