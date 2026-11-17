"""``cara.eloquent.RawSql`` — the raw-SQL fragments both products rebuilt."""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

from cara.eloquent import (
    dedupe_last,
    in_list,
    json_column,
    json_param,
    row_value,
    row_values_in,
    values_rows,
)


def test_row_value_reads_mappings_objects_and_none():
    assert row_value({"a": 1}, "a") == 1
    assert row_value({"a": 1}, "b", "x") == "x"
    assert row_value(SimpleNamespace(a=2), "a") == 2
    assert row_value(SimpleNamespace(), "a") is None
    assert row_value(None, "a", 0) == 0


def test_in_list_dedupes_keeps_order_and_refuses_empty():
    assert in_list([3, 1, 3, 2]) == ("%s, %s, %s", [3, 1, 2])
    with pytest.raises(ValueError):
        in_list([])


def test_row_values_in_builds_one_predicate():
    sql, bindings = row_values_in(
        ("identifier_type", "identifier_value"),
        [("gtin", "1"), ("upc", "2"), ("gtin", "1")],
    )
    assert sql == "(identifier_type, identifier_value) IN ((%s, %s), (%s, %s))"
    assert bindings == ["gtin", "1", "upc", "2"]


def test_row_values_in_negate_and_expression_columns():
    sql, bindings = row_values_in(("channel_id", "data->>'k'"), [(1, "a")], negate=True)
    assert sql == "(channel_id, data->>'k') NOT IN ((%s, %s))"
    assert bindings == [1, "a"]


@pytest.mark.parametrize("rows", [[], [(1,)]])
def test_row_values_in_refuses_empty_and_ragged(rows):
    with pytest.raises(ValueError):
        row_values_in(("a", "b"), rows)


def test_values_rows_with_casts_and_tail():
    sql, bindings = values_rows(
        [(1, "{}"), (2, None)], casts=(None, "jsonb"), tail=("NOW()",)
    )
    assert sql == "(%s, %s::jsonb, NOW()), (%s, %s::jsonb, NOW())"
    assert bindings == [1, "{}", 2, None]


def test_values_rows_refuses_empty_ragged_and_bad_casts():
    with pytest.raises(ValueError):
        values_rows([])
    with pytest.raises(ValueError):
        values_rows([(1, 2), (3,)])
    with pytest.raises(ValueError):
        values_rows([(1, 2)], casts=("int",))


def test_dedupe_last_keeps_last_value_at_first_position():
    rows = [{"k": 1, "v": "a"}, {"k": 2, "v": "b"}, {"k": 1, "v": "c"}]
    assert dedupe_last(rows, key=lambda row: row["k"]) == [
        {"k": 1, "v": "c"},
        {"k": 2, "v": "b"},
    ]


def test_json_param_keeps_decimal_digits_and_null():
    assert json_param(None) is None
    assert json_param({"p": Decimal("19.99")}) == '{"p": "19.99"}'
    assert json_param({}) == "{}"
    assert json_param({}, empty_as_null=True) is None
    assert json_param([], empty_as_null=True) is None


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (None, None),
        ({"a": 1}, {"a": 1}),
        ('{"a": 1}', {"a": 1}),
        (b"[1, 2]", [1, 2]),
        ("", None),
        ("not json", None),
    ],
)
def test_json_column_decodes_text_and_tolerates_garbage(raw, expected):
    assert json_column(raw) == expected


def test_json_column_default():
    assert json_column("oops", default={}) == {}
