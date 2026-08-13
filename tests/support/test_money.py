"""``cara.support.Money`` — value-object invariants.

``Money`` exists to kill a currency-corruption bug class: an amount and its
currency are no longer two loose primitives that code can desync, but one
immutable value that REFUSES to silently swap the currency out from under an
amount.

These tests pin the contract callers rely on:

* immutability (amount + currency are set once);
* ``with_amount`` preserves currency by construction;
* ``replace_currency`` is a no-op for the same currency and RAISES for a
  different one (the silent-swap guard);
* comparison / arithmetic are same-currency only (cross-currency raises),
  while equality stays total (never raises);
* formatting reuses the framework's symbol-aware renderer;
* ``margin_ratio`` / ``markup_ratio`` keep the accounting BASE explicit and
  refuse to invent a percentage from an unknown or zero base.
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from decimal import Decimal

import pytest

from cara.support import (
    CurrencyMismatch,
    Money,
    format_money,
    margin_ratio,
    markup_ratio,
    normalize_currency_code,
    parse_decimal_text,
    require_currency_code,
)


def test_currency_codes_are_ascii_canonical_and_fail_closed() -> None:
    assert normalize_currency_code(" usd ") == "USD"
    assert normalize_currency_code(None) is None
    assert normalize_currency_code("ÜSD") is None
    with pytest.raises(ValueError, match="currency code"):
        require_currency_code("dollars", context="Invoice")


@pytest.mark.parametrize("value", [19.99, True, "01.00", "+1", "1e2", " NaN "])
def test_exact_decimal_text_parser_rejects_non_contract_values(value) -> None:
    assert parse_decimal_text(value) is None


def test_exact_decimal_text_parser_preserves_scale() -> None:
    assert parse_decimal_text("19.9900") == Decimal("19.9900")


class TestConstruction:
    def test_normalizes_amount_to_decimal(self) -> None:
        assert Money("45.00", "GBP").amount == Decimal("45.00")
        assert Money(45, "GBP").amount == Decimal("45")
        # Float routed through str() — no binary-float drift.
        assert Money(45.10, "GBP").amount == Decimal("45.10")

    def test_normalizes_currency_uppercase_and_stripped(self) -> None:
        assert Money("45", " gbp ").currency == "GBP"
        assert Money("45", "usd").currency == "USD"

    def test_empty_currency_rejected(self) -> None:
        with pytest.raises(CurrencyMismatch):
            Money("45", "")
        with pytest.raises(CurrencyMismatch):
            Money("45", None)  # type: ignore[arg-type]

    @pytest.mark.parametrize("amount", [None, True, "bad", "NaN", "Infinity"])
    def test_invalid_or_non_finite_amount_rejected(self, amount) -> None:
        with pytest.raises(ValueError, match="finite numeric"):
            Money(amount, "USD")

    @pytest.mark.parametrize("currency", ["US", "ÜSD", "US1"])
    def test_malformed_currency_rejected(self, currency) -> None:
        with pytest.raises(CurrencyMismatch):
            Money("45", currency)

    def test_is_frozen_immutable(self) -> None:
        m = Money("45", "GBP")
        with pytest.raises(FrozenInstanceError):
            m.amount = Decimal("50")  # type: ignore[misc]
        with pytest.raises(FrozenInstanceError):
            m.currency = "USD"  # type: ignore[misc]


class TestWithAmountPreservesCurrency:
    def test_with_amount_keeps_currency(self) -> None:
        m = Money("45", "GBP")
        out = m.with_amount("50")
        assert out.amount == Decimal("50")
        assert out.currency == "GBP"
        # Original untouched (immutability).
        assert m.amount == Decimal("45")

    def test_with_amount_returns_new_instance(self) -> None:
        m = Money("45", "GBP")
        assert m.with_amount("45") is not m


class TestReplaceCurrency:
    def test_same_currency_is_noop(self) -> None:
        m = Money("45", "GBP")
        # Case-insensitive / whitespace tolerant — still the SAME currency.
        assert m.replace_currency("gbp") == m
        assert m.replace_currency(" GBP ").amount == Decimal("45")

    def test_different_currency_raises(self) -> None:
        m = Money("45", "GBP")
        with pytest.raises(CurrencyMismatch, match="re-denominate"):
            m.replace_currency("USD")

    def test_empty_currency_raises(self) -> None:
        with pytest.raises(CurrencyMismatch):
            Money("45", "GBP").replace_currency("")


class TestComparison:
    def test_same_currency_orders_by_amount(self) -> None:
        assert Money("10", "GBP") < Money("20", "GBP")
        assert Money("20", "GBP") > Money("10", "GBP")
        assert Money("10", "GBP") <= Money("10", "GBP")
        assert Money("10", "GBP") >= Money("10", "GBP")

    def test_cross_currency_ordering_raises(self) -> None:
        for op in ("__lt__", "__le__", "__gt__", "__ge__"):
            with pytest.raises(CurrencyMismatch):
                getattr(Money("10", "GBP"), op)(Money("10", "USD"))

    def test_equality_is_total_never_raises(self) -> None:
        # Same value.
        assert Money("45", "GBP") == Money("45", "GBP")
        # Different currency → not equal, but does NOT raise (safe in sets).
        assert Money("45", "GBP") != Money("45", "USD")
        # Non-Money comparand → not equal, no raise.
        assert (Money("45", "GBP") == 45) is False

    def test_hashable_same_value_same_hash(self) -> None:
        assert hash(Money("45", "GBP")) == hash(Money("45.00", "GBP"))
        # Usable as a set member.
        assert len({Money("45", "GBP"), Money("45.00", "GBP")}) == 1


class TestArithmetic:
    def test_add_same_currency(self) -> None:
        assert Money("10", "GBP").add(Money("5", "GBP")) == Money("15", "GBP")

    def test_subtract_same_currency(self) -> None:
        assert Money("10", "GBP").subtract(Money("4", "GBP")) == Money("6", "GBP")

    def test_cross_currency_arithmetic_raises(self) -> None:
        with pytest.raises(CurrencyMismatch):
            Money("10", "GBP").add(Money("5", "USD"))
        with pytest.raises(CurrencyMismatch):
            Money("10", "GBP").subtract(Money("5", "USD"))


class TestInvariantHelpers:
    def test_is_positive(self) -> None:
        assert Money("0.01", "GBP").is_positive is True
        assert Money("0", "GBP").is_positive is False
        assert Money("-5", "GBP").is_positive is False

    def test_assert_same_currency(self) -> None:
        Money("1", "GBP").assert_same_currency(Money("2", "GBP"))  # no raise
        with pytest.raises(CurrencyMismatch):
            Money("1", "GBP").assert_same_currency(Money("2", "USD"))
        with pytest.raises(CurrencyMismatch):
            Money("1", "GBP").assert_same_currency(45)  # type: ignore[arg-type]

    def test_is_same_currency(self) -> None:
        assert Money("1", "GBP").is_same_currency(Money("2", "GBP")) is True
        assert Money("1", "GBP").is_same_currency(Money("2", "USD")) is False
        assert Money("1", "GBP").is_same_currency(45) is False  # type: ignore[arg-type]


class TestMarginVersusMarkup:
    """The two ratios must never be the same number for the same trade.

    Sell at 150 what cost 100: profit 50. Margin (over REVENUE) is 33.3%;
    markup (over COST) is 50%. A codebase that computes one and labels it
    the other reports a wrong number that still looks plausible — the exact
    silent-money bug these two functions separate by name.
    """

    def test_same_trade_different_bases(self) -> None:
        revenue, cost = Money("150", "GBP"), Money("100", "GBP")
        profit = revenue.subtract(cost)

        assert margin_ratio(profit=profit, revenue=revenue) == (
            Decimal("50") / Decimal("150")
        )
        assert markup_ratio(profit=profit, cost=cost) == Decimal("0.5")
        # And they genuinely differ — the whole point.
        assert margin_ratio(profit=profit, revenue=revenue) != markup_ratio(
            profit=profit, cost=cost
        )

    def test_base_is_keyword_only(self) -> None:
        # Positional use is a TypeError, so a caller can never silently swap
        # the base by argument order.
        with pytest.raises(TypeError):
            margin_ratio(Money("50", "GBP"), Money("150", "GBP"))  # type: ignore[misc]
        with pytest.raises(TypeError):
            markup_ratio(Money("50", "GBP"), Money("100", "GBP"))  # type: ignore[misc]

    def test_margin_is_bounded_markup_is_not(self) -> None:
        # Doubling the money: margin 50%, markup 100%.
        assert margin_ratio(profit=Money("100", "GBP"), revenue=Money("200", "GBP")) == (
            Decimal("0.5")
        )
        assert markup_ratio(profit=Money("100", "GBP"), cost=Money("100", "GBP")) == (
            Decimal("1")
        )


class TestRatioHonesty:
    def test_absent_input_is_none_not_zero(self) -> None:
        assert margin_ratio(profit=None, revenue=Money("150", "GBP")) is None
        assert margin_ratio(profit=Money("50", "GBP"), revenue=None) is None
        assert markup_ratio(profit=None, cost=Money("100", "GBP")) is None
        assert markup_ratio(profit=Money("50", "GBP"), cost=None) is None

    def test_unparseable_input_is_none(self) -> None:
        assert margin_ratio(profit="not-a-number", revenue="150") is None
        assert margin_ratio(profit="50", revenue="unknown") is None

    def test_zero_base_with_profit_is_none_not_zero(self) -> None:
        # A fully refunded window nets a real LOSS against zero revenue.
        # Reporting 0% there would read as break-even.
        assert margin_ratio(profit=Money("-5", "GBP"), revenue=Money("0", "GBP")) is None
        assert margin_ratio(profit=Money("5", "GBP"), revenue=Money("0", "GBP")) is None
        assert markup_ratio(profit=Money("5", "GBP"), cost=Money("0", "GBP")) is None

    def test_genuine_zero_over_zero_is_zero(self) -> None:
        assert margin_ratio(
            profit=Money("0", "GBP"), revenue=Money("0", "GBP")
        ) == Decimal("0")
        assert markup_ratio(profit=Money("0", "GBP"), cost=Money("0", "GBP")) == (
            Decimal("0")
        )

    def test_loss_is_a_negative_ratio(self) -> None:
        assert margin_ratio(
            profit=Money("-30", "GBP"), revenue=Money("150", "GBP")
        ) == Decimal("-0.2")

    def test_cross_currency_ratio_raises(self) -> None:
        with pytest.raises(CurrencyMismatch):
            margin_ratio(profit=Money("50", "GBP"), revenue=Money("150", "USD"))
        with pytest.raises(CurrencyMismatch):
            markup_ratio(profit=Money("50", "USD"), cost=Money("100", "GBP"))

    def test_plain_numbers_are_accepted_without_currency(self) -> None:
        # Not every caller has (amount, currency) pairs yet; bare numerics
        # keep the same honest-null rules, they just carry no currency to
        # cross-check.
        assert margin_ratio(profit="50", revenue="200") == Decimal("0.25")
        assert markup_ratio(profit=Decimal("50"), cost=200) == Decimal("0.25")
        assert margin_ratio(profit=Money("50", "GBP"), revenue=200) == Decimal("0.25")

    def test_float_input_avoids_binary_drift(self) -> None:
        assert margin_ratio(profit=0.1, revenue=1.0) == Decimal("0.1")

    def test_ratio_is_unrounded(self) -> None:
        # Rounding belongs to the display edge; the ratio keeps full Decimal
        # precision so two surfaces cannot round the same pair differently.
        value = margin_ratio(profit=Money("1", "GBP"), revenue=Money("3", "GBP"))
        assert value is not None
        assert value != Decimal("0.33")
        assert round(value, 4) == Decimal("0.3333")


class TestFormatting:
    def test_format_uses_currency_symbol(self) -> None:
        assert Money("45", "GBP").format() == "£45.00"
        assert Money("1234.5", "EUR").format() == "€1,234.50"
        # str() renders the same way.
        assert str(Money("19.99", "USD")) == "$19.99"

    def test_zero_decimal_currency(self) -> None:
        # JPY is zero-decimal — no trailing cents.
        assert Money("1500", "JPY").format() == "¥1,500"

    @pytest.mark.parametrize("amount", [None, "bad", "NaN", "Infinity"])
    def test_format_rejects_unknown_or_non_finite_amount(self, amount: object) -> None:
        with pytest.raises(ValueError, match="numeric|finite"):
            format_money(amount, "USD")

    @pytest.mark.parametrize("currency", [None, "", "US", "US1"])
    def test_format_rejects_missing_or_malformed_currency(self, currency: object) -> None:
        with pytest.raises(ValueError, match="currency code"):
            format_money("1.00", currency)  # type: ignore[arg-type]
