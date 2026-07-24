"""Runtime contract for model-first ``Schema.build`` declarations."""

from __future__ import annotations

import pytest

from cara.eloquent.schema.Schema import FieldBuilder, Schema


def test_schema_build_executes_composite_unique_and_index_declarations() -> None:
    definitions = Schema.build(
        lambda field: (
            field.unsigned_big_integer("tenant_id"),
            field.string("external_id", 120),
            field.unique(
                ["tenant_id", "external_id"],
                name="records_tenant_external_unique",
            ),
            field.index(["tenant_id", "external_id"]),
        )
    )

    assert [definition.to_dict() for definition in definitions] == [
        {"type": "unsigned_big_integer", "params": {}},
        {"type": "string", "params": {"length": 120}},
        {
            "type": "unique",
            "params": {
                "columns": ["tenant_id", "external_id"],
                "name": "records_tenant_external_unique",
            },
        },
        {
            "type": "index",
            "params": {
                "columns": ["tenant_id", "external_id"],
                "name": None,
            },
        },
    ]


def test_schema_build_executes_column_index_and_timestamp_modifiers() -> None:
    indexed, timestamp = Schema.build(
        lambda field: (
            field.string("status", 30).index(),
            field.datetime("recorded_at").use_current(),
        )
    )

    assert indexed.to_dict()["params"]["index"] is True
    assert timestamp.to_dict()["params"]["use_current"] is True


@pytest.mark.parametrize(
    ("method", "columns", "name"),
    [
        ("unique", [], None),
        ("index", ["tenant_id", ""], None),
        ("index", ["tenant_id"], ""),
    ],
)
def test_schema_build_rejects_invalid_constraint_declarations(
    method: str,
    columns: list[str],
    name: str | None,
) -> None:
    with pytest.raises(ValueError, match=method):
        getattr(FieldBuilder(), method)(columns, name=name)
