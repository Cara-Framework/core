"""Runtime contract for model-first ``Schema.build`` declarations."""

from __future__ import annotations

import pytest

from cara.eloquent.schema.FieldBuilder import FieldBuilder
from cara.eloquent.schema.Schema import Schema


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


def test_schema_build_preserves_named_composite_foreign_key() -> None:
    (definition,) = Schema.build(
        lambda field: (
            field.foreign(
                ["product_id", "tenant_id"],
                name="child_product_tenant_fk",
            )
            .references(["id", "tenant_id"])
            .on("product")
            .on_delete("cascade"),
        )
    )

    assert definition.to_dict()["foreign_key"] == {
        "field": ["product_id", "tenant_id"],
        "name": "child_product_tenant_fk",
        "references": ["id", "tenant_id"],
        "on": "product",
        "on_delete": "cascade",
        "on_update": None,
    }


def test_schema_build_executes_column_index_and_timestamp_modifiers() -> None:
    indexed, timestamp = Schema.build(
        lambda field: (
            field.string("status", 30).index(),
            field.datetime("recorded_at").use_current(),
        )
    )

    assert indexed.to_dict()["params"]["index"] is True
    assert timestamp.to_dict()["params"]["use_current"] is True


def test_schema_build_executes_evolve_backfill_declaration() -> None:
    (definition,) = Schema.build(
        lambda field: (field.string("value_key", 32).backfill_from("md5(source_value)"),)
    )

    assert definition.to_dict()["params"]["backfill_from"] == "md5(source_value)"


@pytest.mark.parametrize("expression", ["", "   ", None])
def test_schema_build_rejects_empty_evolve_backfill(expression) -> None:
    with pytest.raises(ValueError, match="backfill_from"):
        FieldBuilder().string("value_key", 32).backfill_from(expression)


def test_schema_build_rejects_duplicate_explicit_and_expanded_columns() -> None:
    with pytest.raises(ValueError, match="duplicate column.*tenant_id"):
        Schema.build(
            lambda field: (
                field.unsigned_big_integer("tenant_id"),
                field.string("tenant_id"),
            )
        )

    with pytest.raises(ValueError, match="duplicate column.*created_at"):
        Schema.build(
            lambda field: (
                field.datetime("created_at"),
                field.timestamps(),
            )
        )


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


def test_schema_build_executes_named_check_declaration() -> None:
    (definition,) = Schema.build(
        lambda field: (
            field.check("amount >= 0", name="invoice_amount_check"),
        )
    )

    assert definition.to_dict() == {
        "type": "check",
        "params": {
            "expression": "amount >= 0",
            "name": "invoice_amount_check",
        },
    }


@pytest.mark.parametrize("expression", ["", "   ", None])
def test_schema_build_rejects_empty_check_expression(expression) -> None:
    with pytest.raises(ValueError, match="check expression"):
        FieldBuilder().check(expression)
