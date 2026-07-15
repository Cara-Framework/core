"""Regression tests for eager-loaded ``BelongsTo`` relationships."""

from __future__ import annotations

from types import SimpleNamespace

from cara.eloquent.relationships.BelongsTo import BelongsTo
from cara.support import Collection


class _Parent:
    def __init__(self, foreign_id):
        self.foreign_id = foreign_id
        self.relations = {}

    def add_relation(self, relation):
        self.relations.update(relation)


def test_nullable_foreign_key_registers_none_when_eager_result_is_empty():
    """A NULL FK must not be used as a list index during eager loading."""
    relation = BelongsTo("foreign_id", "id")
    parent = _Parent(None)

    relation.register_related("foreign", parent, Collection())

    assert parent.relations == {"foreign": None}


def test_non_null_foreign_key_still_registers_matching_model():
    relation = BelongsTo("foreign_id", "id")
    parent = _Parent(7)
    related = SimpleNamespace(id=7)

    relation.register_related("foreign", parent, Collection({7: [related]}))

    assert parent.relations == {"foreign": related}
