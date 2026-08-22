"""A declared FOREIGN KEY must be both SEEN and LANDABLE.

``schema:check`` compared columns, CHECKs and indexes and never once looked at
foreign keys, and the planner could not create one either. On synkronus's
development database that meant 90 of the 104 composite
``(child_id, tenant_id) -> parent(id, tenant_id)`` keys — the ones that make
tenant isolation a storage guarantee rather than a query habit — were absent
from a database every gate called "in sync". Nothing surfaced it until a fresh
CI database, built from the same migrations, began refusing rows the
development database accepted, so the two disagreed about which tests could
pass and the disagreement read as flakiness.
"""

from __future__ import annotations

from cara.schema import LiveSchema, plan
from cara.schema.Objects import declared_foreign_keys


def _model(table="product_image", **extra):
    return {
        "table": table,
        "has_fields_method": True,
        "fields": {"id": {"type": "big_increments", "params": {}}},
        "indexes": [],
        **extra,
    }


def _live(*tables, constraints=()):
    columns = {
        "id": {"data_type": "bigint", "is_nullable": False, "max_length": None},
    }
    return LiveSchema(
        tables={table: dict(columns) for table in tables},
        checks={},
        indexes={},
        constraint_indexes={},
        constraints={table: set(constraints) for table in tables},
        relation_kinds={table: "BASE TABLE" for table in tables},
    )


_COMPOSITE = {
    "columns": ["product_id", "tenant_id"],
    "name": "product_image_product_tenant_fk",
    "references": ["id", "tenant_id"],
    "on": "product",
    "on_delete": "cascade",
    "on_update": None,
}


def test_an_unnamed_single_column_key_takes_the_blueprints_default_name():
    model = _model(
        fields={
            "tenant_id": {
                "type": "unsigned_big_integer",
                "params": {},
                "foreign_key": {
                    "composite": False,
                    "field": "tenant_id",
                    "name": None,
                    "references": "id",
                    "on": "tenant",
                    "on_delete": "cascade",
                    "on_update": None,
                },
            }
        }
    )

    assert [key["name"] for key in declared_foreign_keys(model, "product_image")] == [
        "product_image_tenant_id_foreign"
    ]


def test_a_composite_key_keeps_the_name_the_model_gave_it():
    model = _model(composite_foreign_keys=[_COMPOSITE])

    assert [key["name"] for key in declared_foreign_keys(model, "product_image")] == [
        "product_image_product_tenant_fk"
    ]


def test_a_declared_key_lands_as_not_valid_then_validate():
    model = _model(composite_foreign_keys=[_COMPOSITE])

    operations, refusals, _ = plan([model], _live("product_image", "product"))

    assert refusals == []
    kinds = [(op.kind, op.key) for op in operations]
    assert ("add_foreign_key", "product_image:product_image_product_tenant_fk") in kinds
    assert (
        "validate_foreign_key",
        "product_image:product_image_product_tenant_fk:validate",
    ) in kinds
    add = next(op for op in operations if op.kind == "add_foreign_key")
    validate = next(op for op in operations if op.kind == "validate_foreign_key")
    assert 'FOREIGN KEY ("product_id", "tenant_id")' in add.forward_sql
    assert 'REFERENCES "product" ("id", "tenant_id")' in add.forward_sql
    assert "ON DELETE CASCADE" in add.forward_sql
    assert add.forward_sql.endswith("NOT VALID")
    assert add.reverse_sql is not None
    assert operations.index(add) < operations.index(validate)


def test_the_validation_preflight_exempts_rows_with_a_null_key_column():
    """MATCH SIMPLE: any null in the key columns exempts the row.

    A probe that ignored that would report violations Postgres will never
    raise, and the operator would go looking for corruption that is not there.
    """
    model = _model(composite_foreign_keys=[_COMPOSITE])

    operations, _, _ = plan([model], _live("product_image", "product"))
    validate = next(op for op in operations if op.kind == "validate_foreign_key")

    assert 'c."product_id" IS NOT NULL' in validate.preflight_sql
    assert 'c."tenant_id" IS NOT NULL' in validate.preflight_sql
    assert 'p."id" = c."product_id"' in validate.preflight_sql
    assert "NOT EXISTS" in validate.preflight_sql


def test_a_key_the_database_already_has_is_not_planned_again():
    model = _model(composite_foreign_keys=[_COMPOSITE])
    live = _live(
        "product_image", "product", constraints=("product_image_product_tenant_fk",)
    )

    operations, _, _ = plan([model], live)

    assert [op for op in operations if "foreign_key" in op.kind] == []


def test_a_key_pointing_at_a_table_this_plan_still_has_to_create_waits():
    """The parent is absent, so there is nothing to reference yet.

    Its own ``create_table`` runs first; the key lands on the next plan rather
    than being emitted as a statement that cannot execute.
    """
    model = _model(composite_foreign_keys=[_COMPOSITE])

    operations, refusals, _ = plan([model], _live("product_image"))

    assert refusals == []
    assert [op for op in operations if "foreign_key" in op.kind] == []
