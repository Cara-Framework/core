"""ModelIndexAudit + ModelSchema: index rules over model declarations."""

from __future__ import annotations

from cara.testing import ModelSchema
from cara.testing.audits import ModelIndexAudit


def _model(table: str, **parts) -> dict:
    return {"name": table.title(), "table": table, "fields": {}, **parts}


def test_a_bare_foreign_key_is_reported_and_an_index_satisfies_it():
    bare = _model("child", fields={"parent_id": {"foreign_key": True, "params": {}}})
    indexed = _model(
        "child",
        fields={"parent_id": {"foreign_key": True, "params": {"index": True}}},
    )
    assert [str(f) for f in ModelIndexAudit([bare]).missing_foreign_key_indexes()] == [
        "child.parent_id: foreign key has no lookup index"
    ]
    assert ModelIndexAudit([indexed]).missing_foreign_key_indexes() == []


def test_a_partial_index_counts_only_when_it_binds_the_key():
    def with_index(where: str) -> dict:
        return _model(
            "child",
            fields={"parent_id": {"foreign_key": True, "params": {}}},
            indexes=[{"up": f"CREATE INDEX ix ON child (parent_id, id) {where}"}],
        )

    assert (
        ModelIndexAudit(
            [with_index("WHERE parent_id IS NOT NULL")]
        ).missing_foreign_key_indexes()
        == []
    )
    assert ModelIndexAudit([with_index("")]).missing_foreign_key_indexes() == []
    assert ModelIndexAudit(
        [with_index("WHERE settled_at IS NULL")]
    ).missing_foreign_key_indexes()


def test_a_composite_key_needs_its_columns_leading():
    model = _model(
        "line",
        composite_foreign_keys=[
            {
                "name": "fk_line_order",
                "columns": ["tenant_id", "order_id"],
                "references": ["tenant_id", "id"],
            }
        ],
    )
    assert ModelIndexAudit([model]).missing_foreign_key_indexes()
    model["composite_indexes"] = [{"columns": ["order_id", "created_at"]}]
    assert ModelIndexAudit([model]).missing_foreign_key_indexes() == []


def test_a_narrow_twin_is_a_redundant_prefix_but_a_unique_one_is_not():
    model = _model(
        "item",
        composite_indexes=[
            {"name": "item_a_index", "columns": ["a"]},
            {"name": "item_a_b_index", "columns": ["a", "b"]},
        ],
    )
    found = ModelIndexAudit([model]).redundant_prefix_indexes()
    assert [str(f) for f in found] == [
        "item.item_a_index: is a redundant prefix of item_a_b_index"
    ]
    model["composite_indexes"] = [{"name": "item_a_b_index", "columns": ["a", "b"]}]
    model["indexes"] = [{"up": "CREATE UNIQUE INDEX item_a_unique ON item (a)"}]
    assert ModelIndexAudit([model]).redundant_prefix_indexes() == []


def test_split_index_body_reads_columns_and_predicate():
    columns, predicate = ModelSchema.split_index_body(
        "CREATE INDEX ix ON t (a, lower(b)) INCLUDE (c) WHERE a IS NOT NULL"
    )
    assert columns == ("a", "lower(b)")
    assert predicate == "a IS NOT NULL"
