"""Tests for the Laravel-style methods added to ``cara.support.Collection``.

Every method covers a happy path plus at least one edge case (empty input,
key collisions, dotted-path resolution, or a callable predicate). These pin
the ADDITIVE enrichment so the new behavior cannot silently regress.
"""

from decimal import Decimal

import pytest

from cara.support.Collection import Collection, collect

# --- key_by ---


def test_key_by_dot_path():
    items = collect([{"id": 1, "name": "a"}, {"id": 2, "name": "b"}])
    keyed = items.key_by("id")
    assert keyed.all() == {1: {"id": 1, "name": "a"}, 2: {"id": 2, "name": "b"}}


def test_key_by_callable():
    keyed = collect(["foo", "barbar"]).key_by(len)
    assert keyed.all() == {3: "foo", 6: "barbar"}


def test_key_by_collision_later_wins():
    items = collect([{"k": "x", "v": 1}, {"k": "x", "v": 2}])
    assert items.key_by("k").all() == {"x": {"k": "x", "v": 2}}


# --- flat_map ---


def test_flat_map_collapses_one_level():
    result = collect([1, 2, 3]).flat_map(lambda x: [x, x * 10])
    assert result.all() == [1, 10, 2, 20, 3, 30]


def test_flat_map_empty():
    assert collect([]).flat_map(lambda x: [x]).all() == []


# --- concat ---


def test_concat_list():
    assert collect([1, 2]).concat([3, 4]).all() == [1, 2, 3, 4]


def test_concat_collection():
    assert collect([1]).concat(collect([2, 3])).all() == [1, 2, 3]


def test_concat_dict_appends_values():
    assert collect([1]).concat({"a": 2, "b": 3}).all() == [1, 2, 3]


def test_concat_does_not_mutate_original():
    original = collect([1, 2])
    original.concat([3])
    assert original.all() == [1, 2]


# --- when_empty / when_not_empty ---


def test_when_empty_applies_on_empty():
    result = collect([]).when_empty(lambda c, _: c.push("x"))
    assert result.all() == ["x"]


def test_when_empty_skips_when_not_empty():
    result = collect([1]).when_empty(lambda c, _: c.push("x"))
    assert result.all() == [1]


def test_when_not_empty_applies():
    result = collect([1]).when_not_empty(lambda c, _: c.push(2))
    assert result.all() == [1, 2]


def test_when_not_empty_default_on_empty():
    result = collect([]).when_not_empty(
        lambda c, _: c.push(1), lambda c, _: c.push("default")
    )
    assert result.all() == ["default"]


# --- has ---


def test_has_list_indices():
    assert collect([10, 20, 30]).has(0, 2) is True
    assert collect([10]).has(0, 5) is False


def test_has_dict_keys():
    items = collect({"a": 1, "b": 2})
    assert items.has("a", "b") is True
    assert items.has("a", "z") is False


# --- value ---


def test_value_first_item_dot_path():
    items = collect([{"price": 5}, {"price": 9}])
    assert items.value("price") == 5


def test_value_empty_returns_default():
    assert collect([]).value("price", default="none") == "none"


# --- make / wrap / unwrap ---


def test_make_classmethod():
    c = Collection.make([1, 2])
    assert isinstance(c, Collection) and c.all() == [1, 2]


def test_make_default_empty():
    assert Collection.make().all() == []


def test_wrap_scalar():
    assert Collection.wrap(5).all() == [5]


def test_wrap_list():
    assert Collection.wrap([1, 2]).all() == [1, 2]


def test_wrap_existing_collection_is_identity():
    existing = collect([1])
    assert Collection.wrap(existing) is existing


def test_unwrap_collection():
    assert Collection.unwrap(collect([1, 2])) == [1, 2]


def test_unwrap_plain_value():
    assert Collection.unwrap([1, 2]) == [1, 2]


# --- skip_while / skip_until / take_while / take_until ---


def test_skip_while_callable():
    assert collect([1, 2, 3, 1]).skip_while(lambda x: x < 3).all() == [3, 1]


def test_skip_while_value():
    assert collect([1, 1, 2, 3]).skip_while(1).all() == [2, 3]


def test_skip_until_callable():
    assert collect([1, 2, 3, 4]).skip_until(lambda x: x >= 3).all() == [3, 4]


def test_take_while_callable():
    assert collect([1, 2, 3, 1]).take_while(lambda x: x < 3).all() == [1, 2]


def test_take_while_empty():
    assert collect([]).take_while(lambda x: True).all() == []


def test_take_until_callable():
    assert collect([1, 2, 3, 4]).take_until(lambda x: x >= 3).all() == [1, 2]


# --- chunk_while ---


def test_chunk_while_consecutive_runs():
    result = collect([1, 2, 4, 5, 7]).chunk_while(
        lambda value, chunk: value == chunk.last() + 1
    )
    assert [c.all() for c in result.all()] == [[1, 2], [4, 5], [7]]


def test_chunk_while_empty():
    assert collect([]).chunk_while(lambda v, c: True).all() == []


# --- split ---


def test_split_even():
    result = collect([1, 2, 3, 4]).split(2)
    assert [c.all() for c in result.all()] == [[1, 2], [3, 4]]


def test_split_uneven_front_loaded():
    result = collect([1, 2, 3, 4, 5]).split(2)
    assert [c.all() for c in result.all()] == [[1, 2, 3], [4, 5]]


# --- sort_keys / sort_keys_desc ---


def test_sort_keys_dict():
    items = collect({"b": 2, "a": 1, "c": 3})
    assert list(items.sort_keys().all().keys()) == ["a", "b", "c"]


def test_sort_keys_desc():
    items = collect({"a": 1, "c": 3, "b": 2})
    assert list(items.sort_keys_desc().all().keys()) == ["c", "b", "a"]


def test_sort_keys_list_is_copy():
    assert collect([3, 1, 2]).sort_keys().all() == [3, 1, 2]


# --- after / before ---


def test_after_returns_next():
    assert collect([1, 2, 3]).after(2) == 3


def test_after_last_returns_none():
    assert collect([1, 2, 3]).after(3) is None


def test_after_missing_returns_none():
    assert collect([1, 2]).after(99) is None


def test_after_callable():
    assert collect([1, 2, 3]).after(lambda x: x == 1) == 2


def test_before_returns_previous():
    assert collect([1, 2, 3]).before(2) == 1


def test_before_first_returns_none():
    assert collect([1, 2, 3]).before(1) is None


# --- contains_one_item ---


def test_contains_one_item_true():
    assert collect([1]).contains_one_item() is True


def test_contains_one_item_false_when_empty_or_many():
    assert collect([]).contains_one_item() is False
    assert collect([1, 2]).contains_one_item() is False


# --- replace ---


def test_replace_dict_overlay():
    items = collect({"a": 1, "b": 2})
    assert items.replace({"b": 99, "c": 3}).all() == {"a": 1, "b": 99, "c": 3}


def test_replace_list_by_index():
    assert collect([1, 2, 3]).replace([10, 20]).all() == [10, 20, 3]


def test_replace_list_extends():
    assert collect([1]).replace([10, 20, 30]).all() == [10, 20, 30]


# --- map_spread ---


def test_map_spread_tuples():
    pairs = collect([(1, 2), (3, 4)])
    assert pairs.map_spread(lambda a, b: a + b).all() == [3, 7]


def test_map_spread_empty():
    assert collect([]).map_spread(lambda a, b: a).all() == []


# --- where_instance_of ---


def test_where_instance_of_single_type():
    items = collect([1, "two", 3, "four"])
    assert items.where_instance_of(str).all() == ["two", "four"]


def test_where_instance_of_tuple_of_types():
    items = collect([1, "two", 3.0, None])
    assert items.where_instance_of((int, float)).all() == [1, 3.0]


# --- cross_join ---


def test_cross_join_two_lists():
    result = collect([1, 2]).cross_join(["a", "b"])
    assert result.all() == [[1, "a"], [1, "b"], [2, "a"], [2, "b"]]


def test_cross_join_with_collection():
    result = collect([1]).cross_join(collect(["x", "y"]))
    assert result.all() == [[1, "x"], [1, "y"]]


# --- range ---


def test_range_ascending_inclusive():
    assert Collection.range(1, 5).all() == [1, 2, 3, 4, 5]


def test_range_descending():
    assert Collection.range(5, 1).all() == [5, 4, 3, 2, 1]


# --- times ---


def test_times_without_callback():
    assert Collection.times(3).all() == [1, 2, 3]


def test_times_with_callback():
    assert Collection.times(3, lambda i: i * i).all() == [1, 4, 9]


def test_times_zero_is_empty():
    assert Collection.times(0).all() == []


# --- aggregates: empty semantics and the no-swallow rule ---
#
# Pre-fix every aggregate here wrapped its arithmetic in
# ``contextlib.suppress(TypeError)`` (or an ``except TypeError, ValueError``
# arm) and answered the literal ``0``. Two distinct lies came out of that:
# "no data" was reported as a measured zero, and a collection that genuinely
# held money but could not be summed — one ``float`` among ``Decimal``s —
# was reported as a revenue total of zero, with no log line and no exception.


def test_sum_of_empty_is_zero():
    """Empty sum stays the additive identity — see Collection.sum's docstring."""
    assert collect([]).sum() == 0


def test_sum_of_decimals_stays_decimal():
    total = collect([Decimal("1.50"), Decimal("2.25")]).sum()
    assert total == Decimal("3.75")
    assert isinstance(total, Decimal)


def test_sum_of_mixed_decimal_and_float_raises():
    """Pinned wrong behaviour: this used to return ``0`` for £4.00 of money."""
    with pytest.raises(TypeError):
        collect([Decimal("1.50"), 2.5]).sum()


def test_sum_by_key_over_missing_key_is_zero():
    assert collect([{"a": 1}, {"a": 2}]).sum("b") == 0


def test_avg_of_empty_is_none():
    """Pinned wrong behaviour: an empty cohort reported an average of ``0``."""
    assert collect([]).avg() is None


def test_avg_of_mixed_decimal_and_float_raises():
    with pytest.raises(TypeError):
        collect([Decimal("1.50"), 2.5]).avg()


def test_avg_of_values():
    assert collect([2, 4, 6]).avg() == 4


def test_median_of_empty_is_none():
    """Pinned wrong behaviour: an empty collection reported a median of ``0``."""
    assert collect([]).median() is None


def test_median_of_unorderable_items_raises():
    with pytest.raises(TypeError):
        collect([1, "two"]).median()


def test_median_odd_and_even():
    assert collect([3, 1, 2]).median() == 2
    assert collect([1, 2, 3, 4]).median() == 2.5


def test_max_of_empty_is_none():
    """Pinned wrong behaviour: an empty collection reported a ceiling of ``0``."""
    assert collect([]).max() is None


def test_min_of_empty_is_none():
    """Pinned wrong behaviour: an empty collection reported a floor of ``0``."""
    assert collect([]).min() is None


def test_max_and_min_of_negative_values():
    # The pre-fix ``0`` default was not merely imprecise here — it sat OUTSIDE
    # the data's own range, so a max below zero read as a max of zero.
    assert collect([-5, -2, -9]).max() == -2
    assert collect([-5, -2, -9]).min() == -9


def test_max_of_unorderable_items_raises():
    with pytest.raises(TypeError):
        collect([1, "two"]).max()


def test_min_of_unorderable_items_raises():
    with pytest.raises(TypeError):
        collect([1, "two"]).min()
