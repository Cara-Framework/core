"""Currency helpers — symbol-aware amount rendering + default fallback.

Formatting prices as ``"$19.99"`` shows up across notifications, email
templates, SEO copy, comparison narratives, and any other surface that
echoes a numeric price back to a user. Hard-coding the ``$`` literal
broke any time the product was sourced in a non-USD market: a notice
saying "was $50, now $45" for an EUR product is wrong on its face.

This module wraps the symbol → format pipeline once so:

* ``default_currency()`` resolves the storefront fallback from
  ``config("app.default_currency", "USD")`` and caches the result
  per-process (currency choice is set at boot, doesn't change at
  runtime).
* ``currency_symbol(code)`` maps known ISO codes to their canonical
  symbols, falling back to a "<CODE> " prefix for unknowns instead
  of silently picking ``$`` (better visible-unfamiliar than
  silently-wrong).
* ``format_money(amount, currency, *, decimals)`` is the typical
  call site — pass a float, get back a formatted string.

Generic, no app coupling: the only outside read is the
``app.default_currency`` config key, a convention every cara app
follows. Callers can pass ``currency`` explicitly to bypass it.
"""

from __future__ import annotations

from typing import Optional

# NOTE: ``cara.configuration`` is imported lazily inside ``default_currency``
# to avoid a circular import — ``cara.support`` is imported during
# ``cara.foundation.Application`` boot (via ``PathManager``), which itself is
# pulled in by ``cara.configuration.Configuration``. Importing ``config`` at
# module top-level here re-enters a partially-initialised
# ``cara.configuration`` package.


# Cache the default per-process so we don't pay a config lookup on
# every product card render.
_DEFAULT_CACHE: Optional[str] = None

# Symbol map for the currencies cheapa apps actually serve. Adding a
# new market = one entry here. Codes not listed render with the ISO
# code as a prefix (``"PLN 50.00"``) — explicit and unambiguous.
_CURRENCY_SYMBOLS = {
    "USD": "$",
    "CAD": "$",      # Canadian dollar shares the symbol — recipients
                     # already see it on their localised storefront.
    "AUD": "$",
    "MXN": "$",
    "EUR": "€",
    "GBP": "£",
    "JPY": "¥",
    "CNY": "¥",
    "TRY": "₺",
    "INR": "₹",
    "BRL": "R$",
    "CHF": "CHF ",   # No common single-char symbol — prefix.
}


def default_currency() -> str:
    """Return the storefront fallback currency (``"USD"`` unless overridden).

    Reads ``config("app.default_currency")`` once and caches the result
    for the rest of the process lifetime. Currency choice is set via
    env at boot — changing it requires a redeploy anyway, so the cache
    is safe.
    """
    global _DEFAULT_CACHE
    if _DEFAULT_CACHE is None:
        from cara.configuration import config  # lazy: see module docstring note
        _DEFAULT_CACHE = str(config("app.default_currency", "USD") or "USD").upper()
    return _DEFAULT_CACHE


def currency_symbol(currency: Optional[str] = None) -> str:
    """Return the symbol prefix for ``currency``.

    Unknown / missing codes render as ``"<CODE> "`` (e.g. ``"PLN "``)
    instead of ``"$"`` — better visible-unfamiliar than
    silently-wrong. Empty / ``None`` falls back to
    :func:`default_currency`.
    """
    code = (currency or default_currency() or "").strip().upper()
    if not code:
        return ""
    if code in _CURRENCY_SYMBOLS:
        return _CURRENCY_SYMBOLS[code]
    return f"{code} "


def format_money(
    amount: float,
    currency: Optional[str] = None,
    *,
    decimals: int = 2,
) -> str:
    """Render ``amount`` with the right currency symbol.

    Args:
        amount: Numeric value. Coerced via ``float()``; non-numeric /
            ``None`` collapses to ``0.0`` so callers can chain without
            guards.
        currency: ISO 4217 code (``"USD"``, ``"EUR"``, …). Empty /
            ``None`` falls back to :func:`default_currency`.
        decimals: Trailing-precision digits. Default ``2`` covers most
            consumer prices; pass ``0`` for whole-unit currencies
            (``"¥1500"`` not ``"¥1500.00"``).

    Examples::

        >>> format_money(19.99)
        '$19.99'
        >>> format_money(19.99, "EUR")
        '€19.99'
        >>> format_money(1500, "JPY", decimals=0)
        '¥1500'
        >>> format_money(50, "PLN")
        'PLN 50.00'
    """
    try:
        value = float(amount)
    except (TypeError, ValueError):
        value = 0.0
    return f"{currency_symbol(currency)}{value:.{decimals}f}"


__all__ = ["default_currency", "currency_symbol", "format_money"]
