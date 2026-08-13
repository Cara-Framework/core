"""DecimalCast: a set value never becomes a silent NULL."""

from decimal import Decimal

from cara.eloquent.casts.DecimalCast import DecimalCast


def test_floats_convert_exactly_instead_of_nulling():
    # confidence=1.0 through a gate once became NULL on INSERT and a
    # NOT NULL violation at the database — the generic cast must carry
    # the value; money boundaries refuse floats loudly elsewhere.
    cast = DecimalCast(2)
    assert cast.set(1.0) == Decimal("1.00")
    assert cast.set(0.85) == Decimal("0.85")
    assert cast.get(0.85) == Decimal("0.85")


def test_bool_and_blank_stay_refused():
    cast = DecimalCast(2)
    assert cast.set(True) is None
    assert cast.set("") is None
    assert cast.set(None) is None
