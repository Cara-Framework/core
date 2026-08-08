from __future__ import annotations

import pytest

from cara.validation.Validation import Validation

_A = "CHN01J00000000000000000000000"
_B = "CHN01J00000000000000000000001"


def _passes(value) -> bool:
    return Validation.make({"channel": value}, {"channel": "public_id_csv:CHN"}).passes()


def test_public_id_csv_accepts_one_or_more_canonical_ids() -> None:
    assert _passes(_A) is True
    assert _passes(f"{_A},{_B}") is True


@pytest.mark.parametrize(
    "value",
    [
        "",
        ",",
        f"{_A},",
        f"{_A},,{_B}",
        f"{_A}, {_B}",
        "CHN_A",
        "ORD01J00000000000000000000000",
        [_A],
    ],
)
def test_public_id_csv_rejects_empty_noncanonical_or_composite_values(value) -> None:
    assert _passes(value) is False
