"""CurrencyMismatch."""

from __future__ import annotations

from cara.exceptions import CaraException


class CurrencyMismatch(CaraException, ValueError):
    """Raised when an operation would mix or silently replace currencies.

    A subclass of ``ValueError`` so callers that don't care about the
    distinction still catch it with the usual numeric-coercion guards,
    while currency-aware callers can branch on the precise type.
    """
