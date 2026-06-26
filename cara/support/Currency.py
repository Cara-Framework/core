"""Currency helpers — symbol-aware amount rendering + default fallback.

Formatting prices as ``"$19.99"`` shows up across notifications, email
templates, SEO copy, and any other surface that echoes a numeric price
back to a user. Hard-coding the ``$`` literal broke any time the amount
was denominated in a non-USD currency: a notice saying "was $50, now
$45" for an EUR amount is wrong on its face.

This module wraps the symbol → format pipeline once so:

* ``default_currency()`` resolves the app-wide fallback from
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

from decimal import Decimal, InvalidOperation

# NOTE: ``cara.configuration`` is imported lazily inside ``default_currency``
# to avoid a circular import — ``cara.support`` is imported during
# ``cara.foundation.Application`` boot (via ``PathManager``), which itself is
# pulled in by ``cara.configuration.Configuration``. Importing ``config`` at
# module top-level here re-enters a partially-initialised
# ``cara.configuration`` package.


# Cache the default per-process so we don't pay a config lookup on
# every render that formats a price.
_DEFAULT_CACHE: str | None = None

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
_ZERO_DECIMAL_CURRENCIES = frozenset({
    "BIF", "CLP", "DJF", "GNF", "ISK", "JPY", "KMF", "KRW",
    "PYG", "RWF", "UGX", "VND", "VUV", "XAF", "XOF", "XPF",
})


def default_currency() -> str:
    """Return the app-wide fallback currency (``"USD"`` unless overridden).

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


def currency_symbol(currency: str | None = None) -> str:
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
    amount: object,
    currency: str | None = None,
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
            Non-numeric / ``None`` collapses to ``0`` so callers can
            chain without guards.
        currency: ISO 4217 code (``"USD"``, ``"EUR"``, …). Empty /
            ``None`` falls back to :func:`default_currency`.
        decimals: Trailing-precision digits. ``None`` (default) auto-
            detects from the currency: 0 for zero-decimal currencies
            (JPY, KRW, …), 2 for everything else. Pass an explicit
            int to override.

    The integer part is grouped with thousands separators so a
    ``$1234.50`` price renders as ``$1,234.50`` (and ``¥1500000`` as
    ``¥1,500,000``) instead of an unreadable run of digits.

    Examples::

        >>> format_money(19.99)
        '$19.99'
        >>> format_money(1234.5, "EUR")
        '€1,234.50'
        >>> format_money(1500, "JPY")
        '¥1,500'
        >>> format_money(50, "PLN")
        'zł 50.00'
    """
    if isinstance(amount, Decimal):
        value = amount
    else:
        try:
            value = Decimal(str(amount))
        except (InvalidOperation, TypeError, ValueError):
            value = Decimal("0")
    if decimals is None:
        code = (currency or default_currency() or "").strip().upper()
        decimals = 0 if code in _ZERO_DECIMAL_CURRENCIES else 2
    # ``,`` groups thousands; ``.{decimals}f`` fixes the precision and
    # rounds at the display boundary using Decimal's context (ROUND_HALF_EVEN
    # by default), so the formatted string matches the stored value.
    return f"{currency_symbol(currency)}{value:,.{decimals}f}"


__all__ = ["default_currency", "currency_symbol", "format_money"]
