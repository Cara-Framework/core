"""``Money`` — an immutable (amount, currency) value object.

Amounts and their currency usually travel as two loose primitives: a numeric
column and a currency string carried beside it. Nothing structurally couples
them, and that is the root of a whole bug CLASS — code that updates the
amount can silently re-stamp the currency from a DIFFERENT source, turning a
£45 target into a $45 one, with no error anywhere and a wrong number at the
end of the line.

``Money`` closes that at the type level:

* **Immutable.** ``amount`` and ``currency`` are set once at construction and
  cannot be reassigned (frozen dataclass). You don't mutate a ``Money`` — you
  build a new one, which makes the currency-of-record an explicit decision.
* **Rejects silent currency changes.** :meth:`with_amount` keeps the same
  currency by design; any operation that would *replace* the currency
  (:meth:`replace_currency` to a different code, or a cross-currency
  comparison / arithmetic) raises :class:`CurrencyMismatch`. There is no API
  that quietly swaps the currency out from under an amount.
* **Comparable + formattable.** Same-currency values order and equate by
  amount; formatting reuses :func:`cara.support.format_money` so the symbol
  rendering matches everywhere.

There is deliberately NO exchange-rate logic here: converting between
currencies is a policy decision (which rate, from when, rounded how) that
belongs to the application, not to the identity of an amount.

:func:`margin_ratio` and :func:`markup_ratio` close the SECOND silent-money
bug class this module exists for — see their docstrings.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from .Currency import format_money
from .Number import to_decimal


class CurrencyMismatch(ValueError):
    """Raised when an operation would mix or silently replace currencies.

    A subclass of ``ValueError`` so callers that don't care about the
    distinction still catch it with the usual numeric-coercion guards,
    while currency-aware callers can branch on the precise type.
    """


def _normalize_currency(currency: Any) -> str:
    """Canonical ISO-4217-ish currency code: stripped, upper-cased, non-empty.

    Currency codes are compared and stamped upper-case, so the value object
    enforces the same normalisation at its boundary — ``"gbp"`` and ``"GBP"``
    are the same currency and must never be treated as a swap.
    """
    code = str(currency or "").strip().upper()
    if not code:
        raise CurrencyMismatch("Money requires a non-empty currency code")
    return code


@dataclass(frozen=True)
class Money:
    """An amount of money in a specific currency. Immutable.

    Construct from any numeric-ish amount (``Decimal`` / ``int`` / ``float`` /
    numeric ``str``) plus a currency code::

        >>> Money(Decimal("45.00"), "GBP")
        Money(amount=Decimal('45.00'), currency='GBP')
        >>> Money("45", "gbp").currency
        'GBP'

    Cross-currency operations raise :class:`CurrencyMismatch` rather than
    coercing — there is no exchange-rate logic here, and a silent currency
    swap is exactly the bug this type exists to prevent.
    """

    amount: Decimal
    currency: str

    def __post_init__(self) -> None:
        # Normalise both fields at the boundary. ``object.__setattr__`` is
        # the documented escape hatch for a frozen dataclass to assign in
        # ``__post_init__`` — after this the instance is immutable.
        object.__setattr__(self, "amount", to_decimal(self.amount))
        object.__setattr__(self, "currency", _normalize_currency(self.currency))

    # ── construction ────────────────────────────────────────────────

    def with_amount(self, amount: Any) -> Money:
        """A new ``Money`` with a different amount, SAME currency.

        This is the sanctioned way to "change" a stored amount: the currency
        rides along untouched, so an amount edit can never become a currency
        edit by accident.
        """
        return Money(amount, self.currency)

    def replace_currency(self, currency: Any) -> Money:
        """A new ``Money`` re-denominated to ``currency``, SAME amount.

        Explicit, loud currency replacement — the caller is asserting "this
        amount is now meant in ``currency``" (e.g. a user-supplied override).
        Re-stating the *same* currency is a harmless no-op and returns an
        equal value; switching to a *different* currency raises
        :class:`CurrencyMismatch` so that a re-denomination is always a
        deliberate, visible act and never a silent rewrite of an existing
        amount.
        """
        new_code = _normalize_currency(currency)
        if new_code != self.currency:
            raise CurrencyMismatch(
                f"Refusing to silently re-denominate {self.currency} as "
                f"{new_code}; build a new Money explicitly if this is intended"
            )
        return Money(self.amount, new_code)

    # ── invariants ──────────────────────────────────────────────────

    def assert_same_currency(self, other: Money) -> None:
        """Raise :class:`CurrencyMismatch` unless ``other`` shares this currency."""
        if not isinstance(other, Money):
            raise CurrencyMismatch(f"Cannot compare Money against {type(other).__name__}")
        if other.currency != self.currency:
            raise CurrencyMismatch(
                f"Currency mismatch: {self.currency} vs {other.currency}"
            )

    def is_same_currency(self, other: Money) -> bool:
        """``True`` when ``other`` is a ``Money`` in the same currency."""
        return isinstance(other, Money) and other.currency == self.currency

    @property
    def is_positive(self) -> bool:
        """``True`` when the amount is strictly greater than zero."""
        return self.amount > 0

    # ── arithmetic (same-currency only) ─────────────────────────────

    def add(self, other: Money) -> Money:
        """Sum two same-currency amounts; raises on a currency mismatch."""
        self.assert_same_currency(other)
        return Money(self.amount + other.amount, self.currency)

    def subtract(self, other: Money) -> Money:
        """Difference of two same-currency amounts; raises on a mismatch."""
        self.assert_same_currency(other)
        return Money(self.amount - other.amount, self.currency)

    # ── comparison ──────────────────────────────────────────────────

    def __eq__(self, other: object) -> bool:
        # Equality is currency-aware but total: a different currency (or a
        # non-Money) is simply NOT equal — equality must never raise, so it
        # can be used safely in sets / dict keys / ``==`` chains. Ordering
        # (below) is the operation that enforces the same-currency invariant.
        if not isinstance(other, Money):
            return NotImplemented
        return self.currency == other.currency and self.amount == other.amount

    def __hash__(self) -> int:
        return hash((self.amount, self.currency))

    def __lt__(self, other: Money) -> bool:
        self.assert_same_currency(other)
        return self.amount < other.amount

    def __le__(self, other: Money) -> bool:
        self.assert_same_currency(other)
        return self.amount <= other.amount

    def __gt__(self, other: Money) -> bool:
        self.assert_same_currency(other)
        return self.amount > other.amount

    def __ge__(self, other: Money) -> bool:
        self.assert_same_currency(other)
        return self.amount >= other.amount

    # ── formatting ──────────────────────────────────────────────────

    def format(self, *, decimals: int | None = None) -> str:
        """Symbol-aware display string (``£45.00``), via ``format_money``."""
        return format_money(self.amount, self.currency, decimals=decimals)

    def __str__(self) -> str:
        return self.format()


# ── profitability ratios ────────────────────────────────────────────────
#
# A profit figure divided by the WRONG base is the other silent-money bug:
# both spellings type-check, both return a plausible percentage, and only a
# reconciliation months later shows two surfaces disagreeing about what
# "margin" meant. The base is therefore never positional here — it is a
# keyword whose NAME is the accounting basis, so choosing the wrong one
# requires writing ``revenue=cost`` and reading it back.


def _amount_and_currency(value: Any) -> tuple[Decimal | None, str | None]:
    """Split a ``Money`` / numeric-ish / absent input into (amount, currency).

    Unlike :func:`~cara.support.to_decimal`, an absent or unparseable input
    yields ``None`` rather than ``Decimal("0")``: a ratio must be able to
    tell "no profit" from "profit unknown", and coercing the unknown to zero
    is how a missing input becomes a fabricated 0% verdict.
    """
    if value is None:
        return None, None
    if isinstance(value, Money):
        return value.amount, value.currency
    if isinstance(value, Decimal):
        return (value if value.is_finite() else None), None
    try:
        amount = Decimal(str(value))
    except InvalidOperation, TypeError, ValueError:
        return None, None
    return (amount if amount.is_finite() else None), None


def _share_of(profit: Any, base: Any, base_label: str) -> Decimal | None:
    p_amount, p_currency = _amount_and_currency(profit)
    b_amount, b_currency = _amount_and_currency(base)
    if p_amount is None or b_amount is None:
        return None
    if p_currency and b_currency and p_currency != b_currency:
        raise CurrencyMismatch(
            f"Cannot express {p_currency} profit as a share of {b_currency} {base_label}"
        )
    if b_amount == 0:
        # Genuine 0/0 is a truthful zero. A non-zero profit (or loss) against
        # a zero base has NO ratio — printing "0%" beside a real loss reads as
        # break-even, and infinity is not a percentage.
        return Decimal("0") if p_amount == 0 else None
    return p_amount / b_amount


def margin_ratio(*, profit: Any, revenue: Any) -> Decimal | None:
    """Profit as a fraction of REVENUE — the margin basis.

    ``profit / revenue``: of every unit taken in, this much was kept. This
    is the basis financial reporting means by "margin", and it is bounded
    above by 1.

    Returns ``None`` when either input is absent/unparseable, or when
    revenue is zero against a non-zero profit — an honest "unknown" instead
    of a fabricated percentage. Raises :class:`CurrencyMismatch` when both
    inputs are :class:`Money` in different currencies, because a ratio
    across currencies is meaningless without a rate.

    Multiply by 100 at the DISPLAY edge; the ratio itself stays unrounded so
    that two surfaces cannot round the same pair to different numbers.
    """
    return _share_of(profit, revenue, "revenue")


def markup_ratio(*, profit: Any, cost: Any) -> Decimal | None:
    """Profit as a fraction of COST — the markup basis.

    ``profit / cost``: on top of every unit spent, this much was added. It
    is NOT margin and is unbounded above — a 50% markup is a 33.3% margin —
    so the two are never interchangeable even though both are "a profit
    percentage".

    Same absent-input, zero-base and currency rules as :func:`margin_ratio`.
    """
    return _share_of(profit, cost, "cost")


__all__ = ["CurrencyMismatch", "Money", "margin_ratio", "markup_ratio"]
