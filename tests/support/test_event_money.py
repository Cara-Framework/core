"""Money on the realtime wire is an exact Decimal beside an ISO currency."""

from __future__ import annotations

from decimal import Decimal

import pytest

from cara.support import event_money


def test_exact_amount_and_canonical_currency() -> None:
    assert event_money("19.99", " usd ", context="t") == (Decimal("19.99"), "USD")
    assert event_money(Decimal("1200"), "JPY", context="t") == (Decimal("1200"), "JPY")
    assert event_money(5, "EUR", context="t") == (Decimal("5"), "EUR")


def test_absent_amount_keeps_the_named_currency() -> None:
    assert event_money(None, "usd", context="t") == (None, "USD")
    assert event_money(None, None, context="t") == (None, None)


@pytest.mark.parametrize("value", [19.99, True, "NaN", "Infinity", "abc"])
def test_inexact_or_non_finite_amounts_are_refused(value) -> None:
    with pytest.raises(ValueError):
        event_money(value, "USD", context="t")


def test_amount_without_a_currency_is_refused() -> None:
    with pytest.raises(ValueError, match="currency"):
        event_money("1.00", None, context="PriceAlert")
    with pytest.raises(ValueError, match="currency"):
        event_money("1.00", "dollars", context="PriceAlert")
