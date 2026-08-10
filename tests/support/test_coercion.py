from __future__ import annotations

from decimal import Decimal

import pytest

from cara.support import safe_float


@pytest.mark.parametrize(
    "value",
    [
        True,
        False,
        float("nan"),
        float("inf"),
        float("-inf"),
        "NaN",
        "Infinity",
        "-Infinity",
        Decimal("NaN"),
        Decimal("Infinity"),
        pytest.param(10**10_000, id="overflowing-int"),
    ],
)
def test_safe_float_rejects_non_real_or_non_finite_values(value) -> None:
    assert safe_float(value) is None


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (0, 0.0),
        (-2, -2.0),
        (" 12.5 ", 12.5),
        (Decimal("1.25"), 1.25),
    ],
)
def test_safe_float_returns_finite_numeric_values(value, expected: float) -> None:
    assert safe_float(value) == expected
