"""``distinct`` on a wildcard field reads its sibling collection once.

The wildcard form (``item_ids.*: ...|distinct``) runs the rule per element, and
each run used to rebuild the sibling list and ``count`` through it — quadratic
in an array the CLIENT sizes. 8,000 ids (a 120 KB body) blocked the event loop
for over a second, and the parent's ``max:100`` did not stop it because every
field's rules still run. Pinned by counting how often the collection is walked,
plus the verdicts the tally must keep identical to ``list.count``.
"""

from __future__ import annotations

from cara.validation import Validation
from cara.validation.rules.DistinctRule import DistinctRule


class _CountingList(list):
    """A list that counts how many times it is iterated."""

    walks = 0

    def __iter__(self):
        type(self).walks += 1
        return super().__iter__()


def _walks_for(size: int) -> int:
    _CountingList.walks = 0
    ids = _CountingList(f"ITM{index:06d}" for index in range(size))
    validator = Validation.make(
        {"item_ids": ids},
        {"item_ids": "required|array", "item_ids.*": "required|string|distinct"},
    )
    assert validator.passes()
    return _CountingList.walks


def test_collection_walks_do_not_grow_with_the_array():
    # Before: one walk per element from the rule alone (400 for 400 ids).
    assert _walks_for(400) == _walks_for(20)
    assert _walks_for(400) < 20


def test_every_copy_of_a_duplicate_is_reported():
    validator = Validation.make(
        {"ids": ["a", "b", "a", "c"]}, {"ids.*": "required|string|distinct"}
    )

    assert validator.fails()
    assert set(validator.errors().all()) == {"ids.0", "ids.2"}


def test_equal_numbers_of_different_types_are_duplicates():
    # ``list.count`` compares with ``==``: 1, 1.0 and True are one value.
    validator = Validation.make({"ids": [1, 1.0, True, 2]}, {"ids.*": "distinct"})

    assert set(validator.errors().all()) == {"ids.0", "ids.1", "ids.2"}


def test_object_leaf_form_compares_the_leaf_across_siblings():
    data = {"items": [{"id": 7}, {"id": 8}, {"id": 7}]}

    validator = Validation.make(data, {"items.*.id": "required|integer|distinct"})

    assert set(validator.errors().all()) == {"items.0.id", "items.2.id"}


def test_unhashable_siblings_are_still_compared():
    data = {"rows": [{"a": 1}, {"a": 2}, {"a": 1}, "x", "x"]}

    validator = Validation.make(data, {"rows.*": "distinct"})

    assert set(validator.errors().all()) == {"rows.0", "rows.2", "rows.3", "rows.4"}


def test_rule_called_without_a_memo_keeps_working():
    # A direct caller (no ``Validation.make``) passes no ``_memo``.
    params = {"_data": {"ids": ["a", "a", "b"]}}

    assert DistinctRule().validate("ids.0", "a", params) is False
    assert DistinctRule().validate("ids.2", "b", params) is True


def test_two_collections_in_one_validation_do_not_share_a_tally():
    data = {"left": ["a", "b"], "right": ["a", "a"]}

    validator = Validation.make(data, {"left.*": "distinct", "right.*": "distinct"})

    assert set(validator.errors().all()) == {"right.0", "right.1"}
