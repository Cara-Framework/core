"""Money on the realtime wire — an exact ``Decimal`` and an ISO currency.

An event that carries an amount states it exactly or refuses to leave: a
float, or a figure with no unit, on a frame is a rounding or a guess the
client cannot undo (a yen price toasted as "$1,200.00", a 0.1 + 0.2 drift
in a live price card). Cara's broadcasters already serialize a ``Decimal``
exactly; this is the composer-side gate in front of them.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any

from .Currency import normalize_currency_code, require_currency_code


def event_money(
    value: Any,
    currency: Any,
    *,
    context: str,
) -> tuple[Decimal | None, str | None]:
    """``(amount, currency)`` fit for an event payload, else ``ValueError``.

    An absent amount stays ``None`` beside whatever currency was named; a
    present one must be a finite exact number (``Decimal``, ``int`` or a
    numeric string — never a ``float`` or ``bool``) with a 3-letter code.
    """
    currency_code = normalize_currency_code(currency)
    if currency is not None and currency_code is None:
        raise ValueError(f"{context} requires a 3-letter currency code.")
    if value is None:
        return None, currency_code
    if isinstance(value, (bool, float)):
        raise ValueError(f"{context} requires exact Decimal money.")
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{context} requires a numeric amount.") from exc
    if not amount.is_finite():
        raise ValueError(f"{context} requires a finite amount.")
    return amount, require_currency_code(currency_code, context=context)


__all__ = ["event_money"]
