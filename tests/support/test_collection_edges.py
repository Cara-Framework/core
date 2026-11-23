"""Collection results that were wrong at the edges, and one that was O(n²).

* aggregates over a callable dropped falsy values (0, 0.0, False);
* ``unique`` / ``duplicates`` returned hash order and refused dict rows;
* ``where_in`` / ``where_not_in`` / ``diff`` scanned the value list per item;
* ``flatten(1)`` was a no-op, ``implode(glue, key)`` raised on non-strings,
  ``first``/``last`` raised on keyed collections, ``first_where(k, "!=",
  None)`` compared against the operator, ``pad`` never padded on the left.
"""

from __future__ import annotations

import pytest

from cara.support import collect


class _Counting:
    """A value that counts how many times it is compared for equality."""

    comparisons = 0

    def __init__(self, value):
        self.value = value

    def __eq__(self, other):
        type(self).comparisons += 1
        return isinstance(other, _Counting) and other.value == self.value

    def __hash__(self):
        return hash(self.value)


def test_aggregates_over_a_callable_keep_zero_and_false():
    rows = collect([{"v": 0}, {"v": 10}, {"v": None}])

    assert rows.avg(lambda row: row["v"]) == rows.avg("v") == 5.0
    assert rows.min(lambda row: row["v"]) == 0
    assert collect([{"v": -3}, {"v": 0}]).max(lambda row: row["v"]) == 0
    assert rows.sum(lambda row: row["v"]) == 10


def test_unique_keeps_first_occurrences_in_order():
    assert collect(["b", "a", "c", "a", "b"]).unique().all() == ["b", "a", "c"]
    assert collect([3, 1, 3, 2, 1]).unique().all() == [3, 1, 2]


def test_unique_accepts_unhashable_rows():
    rows = [{"id": 1}, {"id": 2}, {"id": 1}]

    assert collect(rows).unique().all() == [{"id": 1}, {"id": 2}]


def test_duplicates_come_back_in_first_repeat_order():
    assert collect(["c", "a", "b", "a", "c", "a"]).duplicates().all() == ["a", "c"]


@pytest.mark.parametrize("method", ["where_in", "where_not_in"])
def test_membership_filters_do_not_scan_the_value_list_per_item(method):
    size = 400
    rows = collect([{"id": _Counting(i)} for i in range(size)])
    values = [_Counting(i) for i in range(size)]
    _Counting.comparisons = 0

    result = getattr(rows, method)("id", values)

    assert len(result) == (size if method == "where_in" else 0)
    # One hash lookup per item; the list scan made ~size²/2 = 80,000.
    assert _Counting.comparisons <= size * 3


def test_diff_does_not_scan_the_other_list_per_item():
    size = 400
    left = collect([_Counting(i) for i in range(size)])
    right = [_Counting(i) for i in range(size // 2, size + size // 2)]
    _Counting.comparisons = 0

    remaining = left.diff(right)

    assert [item.value for item in remaining] == list(range(size // 2))
    assert _Counting.comparisons <= size * 3


def test_membership_filters_still_work_with_unhashable_values():
    rows = collect([{"tags": ["a"]}, {"tags": ["b"]}, {"tags": None}])

    assert rows.where_in("tags", [["a"], None]).all() == [{"tags": ["a"]}, {"tags": None}]
    assert rows.where_not_in("tags", [["a"]]).all() == [{"tags": ["b"]}, {"tags": None}]
    assert collect([[1], [2], 3]).diff([[2], 3]).all() == [[1]]
    assert collect([1, 2, 3]).diff(collect([2])).all() == [1, 3]


def test_flatten_depth_counts_levels_of_nesting():
    nested = collect([[1, [2, [3]]], [4]])

    assert nested.flatten(1).all() == [1, [2, [3]], 4]
    assert nested.flatten(2).all() == [1, 2, [3], 4]
    assert nested.flatten().all() == [1, 2, 3, 4]


def test_implode_with_a_key_joins_non_string_values():
    assert collect([{"id": 1}, {"id": 2}]).implode(",", "id") == "1,2"
    assert collect(["a", "b"]).implode("-") == "a-b"


def test_first_and_last_on_a_keyed_collection():
    grouped = collect(
        [{"k": "a", "v": 1}, {"k": "b", "v": 2}, {"k": "a", "v": 3}]
    ).group_by("k")

    assert [row["v"] for row in grouped.first()] == [1, 3]
    assert [row["v"] for row in grouped.last()] == [2]
    assert collect([]).first(default="none") == "none"
    assert collect([1, 2, 3]).first() == 1 and collect([1, 2, 3]).last() == 3


def test_first_where_with_an_explicit_none_value():
    rows = collect([{"d": None}, {"d": 1}])

    assert rows.first_where("d", "!=", None) == {"d": 1}
    assert rows.first_where("d", "==", None) == {"d": None}
    assert rows.first_where("d") == {"d": None}
    # The two-argument form still reads its second argument as the value.
    assert rows.first_where("d", 1) == {"d": 1}
    assert collect([{"d": 1}, {"d": 5}]).first_where("d", ">=", 2) == {"d": 5}


def test_pad_left_with_a_negative_size():
    assert collect([1, 2, 3]).pad(5, 0).all() == [1, 2, 3, 0, 0]
    assert collect([1, 2, 3]).pad(-5, 0).all() == [0, 0, 1, 2, 3]
    assert collect([1, 2, 3]).pad(2, 0).all() == [1, 2, 3]
    assert collect([1, 2, 3]).pad(-2, 0).all() == [1, 2, 3]
