"""CSV formula-injection defense and its round-trip inverse."""

from __future__ import annotations

from decimal import Decimal

import pytest

from cara.support import defuse_csv_cell, restore_csv_cell


@pytest.mark.parametrize(
    "value",
    ["=SUM(A1)", "+1", "-RED-01", "@user", "\tindent", "\rreturn", "- bullet"],
)
def test_every_trigger_is_defused_and_restored(value: str) -> None:
    defused = defuse_csv_cell(value)
    assert defused == f"'{value}"
    assert restore_csv_cell(defused) == value


@pytest.mark.parametrize(
    "value",
    ["SKU-1", "", "'", "'quoted", "it's fine", 4, Decimal("3.25"), None, True],
)
def test_values_the_defuser_never_touches_pass_through(value) -> None:
    assert defuse_csv_cell(value) == value
    assert restore_csv_cell(value) == value


def test_only_one_apostrophe_is_stripped() -> None:
    assert restore_csv_cell("''-x") == "''-x"
    assert restore_csv_cell(defuse_csv_cell("'-x")) == "-x"  # documented ambiguity
