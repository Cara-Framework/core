"""Currency helpers — strict codes and symbol-aware amount rendering.

Formatting prices as ``"$19.99"`` shows up across notifications, email
templates, SEO copy, and any other surface that echoes a numeric price
back to a user. Hard-coding the ``$`` literal broke any time the amount
was denominated in a non-USD currency: a notice saying "was $50, now
$45" for an EUR amount is wrong on its face.

This module wraps the symbol → format pipeline once so:

* ``currency_symbol(code)`` maps known ISO codes to their canonical
  symbols, falling back to a "<CODE> " prefix for unknowns instead
  of silently picking ``$`` (better visible-unfamiliar than
  silently-wrong).
* ``format_money(amount, currency, *, decimals)`` is the typical
  call site — pass a float, get back a formatted string.

Generic, no app coupling: every amount carries its unit explicitly.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation

# Symbol map for the currencies the app actually serves. Adding a new
# currency = one entry here. Codes not listed render with the ISO
# code as a prefix (``"PLN 50.00"``) — explicit and unambiguous.
_CURRENCY_SYMBOLS = {
    "USD": "$",
    "CAD": "CA$",
    "AUD": "A$",
    "MXN": "MX$",
    "EUR": "€",
    "GBP": "£",
    "JPY": "¥",
    "CNY": "¥",
    "TRY": "₺",
    "INR": "₹",
    "BRL": "R$",
    "CHF": "CHF ",
    "KRW": "₩",
    "HKD": "HK$",
    "SGD": "S$",
    "NZD": "NZ$",
    "ZAR": "R",
    "RUB": "₽",
    "PLN": "zł ",
}

# ISO 4217 zero-decimal currencies — these currencies have no minor unit
# (no "cents"), so displaying "¥1500.00" or "₩25000.00" is wrong.
# ``format_money`` auto-detects these when ``decimals`` is not explicitly
# overridden so callers don't have to remember per-currency precision.
_ZERO_DECIMAL_CURRENCIES = frozenset(
    {
        "BIF",
        "CLP",
        "DJF",
        "GNF",
        "ISK",
        "JPY",
        "KMF",
        "KRW",
        "PYG",
        "RWF",
        "UGX",
        "VND",
        "VUV",
        "XAF",
        "XOF",
        "XPF",
    }
)


def normalize_currency_code(value: object) -> str | None:
    """Return one canonical three-letter ASCII code, else ``None``.

    This validates shape, not membership in a stale local ISO catalog. Unknown
    real codes remain usable; absent or malformed values remain unknown.
    """

    if value is None:
        return None
    code = str(value).strip().upper()
    if len(code) != 3 or not code.isascii() or not code.isalpha():
        return None
    return code


def require_currency_code(value: object, *, context: str = "Money") -> str:
    """Return a canonical code or fail before an amount loses its unit."""

    code = normalize_currency_code(value)
    if code is None:
        raise ValueError(f"{context} requires a 3-letter currency code.")
    return code


def currency_symbol(currency: str) -> str:
    """Return the symbol prefix for ``currency``.

    Unknown valid codes render as ``"<CODE> "`` (e.g. ``"PLN "``).
    Missing/malformed codes fail before the amount can lose its unit.
    """
    code = require_currency_code(currency)
    if code in _CURRENCY_SYMBOLS:
        return _CURRENCY_SYMBOLS[code]
    return f"{code} "


def format_money(
    amount: object,
    currency: str,
    *,
    decimals: int | None = None,
) -> str:
    """Render ``amount`` with the right currency symbol.

    Args:
        amount: Numeric value (``Decimal`` / ``float`` / ``int`` /
            numeric ``str``). Coerced via ``Decimal(str(...))`` so the
            EXACT decimal value is formatted — never the binary-float
            approximation ``float(amount)`` would introduce (a stored
            ``Decimal('1234.50')`` must not display as ``1234.49``).
            Non-numeric, missing and non-finite values are rejected.
        currency: Required ISO 4217 code (``"USD"``, ``"EUR"``, …).
        decimals: Trailing-precision digits. ``None`` (default) auto-
            detects from the currency: 0 for zero-decimal currencies
            (JPY, KRW, …), 2 for everything else. Pass an explicit
            int to override.

    The integer part is grouped with thousands separators so a
    ``$1234.50`` price renders as ``$1,234.50`` (and ``¥1500000`` as
    ``¥1,500,000``) instead of an unreadable run of digits.

    Examples::

        >>> format_money(19.99, "USD")
        '$19.99'
        >>> format_money(1234.5, "EUR")
        '€1,234.50'
        >>> format_money(1500, "JPY")
        '¥1,500'
        >>> format_money(50, "PLN")
        'zł 50.00'
    """
    try:
        if isinstance(amount, Decimal):
            value = amount
        else:
            value = Decimal(str(amount))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("Money formatting requires a numeric amount.") from exc
    if not value.is_finite():
        raise ValueError("Money formatting requires a finite amount.")
    code = require_currency_code(currency)
    if decimals is None:
        decimals = 0 if code in _ZERO_DECIMAL_CURRENCIES else 2
    if isinstance(decimals, bool) or not isinstance(decimals, int) or decimals < 0:
        raise ValueError("Money formatting decimals must be a non-negative integer.")
    # ``,`` groups thousands; ``.{decimals}f`` fixes the precision and
    # rounds at the display boundary using Decimal's context (ROUND_HALF_EVEN
    # by default), so the formatted string matches the stored value.
    return f"{currency_symbol(code)}{value:,.{decimals}f}"


__all__ = [
    "currency_symbol",
    "format_money",
    "normalize_currency_code",
    "require_currency_code",
]
