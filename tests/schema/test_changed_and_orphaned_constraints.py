"""A CHECK that changed under its name, and a UNIQUE no model declares.

Both stayed invisible to ``schema:check`` and ``schema:plan`` on a real
database (Synkronus dev, 2026-10-28):

* ``catalog_proposal_identity_check`` kept allowing ``'quantity_review'``
  after the model dropped it — the planner compared CHECK NAMES only, and
  ``dev:seed-check`` reading its vocabulary from that stale body kept asking
  for the value the model had removed.
* ``external_attribute_value_map_source_source_key_unique`` outlived the
  column rename that replaced it; the orphaned-index rule exempts every index
  behind a constraint, so the old unique rule failed 1,817 of 1,819 imports
  while both gates read clean.
"""

from __future__ import annotations

from cara.schema import (
    LiveSchema,
    changed_checks,
    constraint_body,
    orphaned_unique_constraints,
    plan,
)

_OLD_KIND = (
    "CHECK (((kind)::text = ANY ((ARRAY['pull_conflict'::character varying, "
    "'quantity_review'::character varying])::text[])))"
)
_NEW_KIND = (
    "CHECK (((kind)::text = ANY ((ARRAY['pull_conflict'::character varying])::text[])))"
)


def _model(table="catalog_proposal", **extra):
    return {
        "table": table,
        "has_fields_method": True,
        "fields": {"id": {"type": "big_increments", "params": {}}},
        "indexes": [],
        **extra,
    }


def _live(
    table="catalog_proposal",
    *,
    checks=(),
    uniques=(),
    definitions=None,
    declared=None,
):
    return LiveSchema(
        tables={
            table: {
                "id": {"data_type": "bigint", "is_nullable": False, "max_length": None},
            }
        },
        checks={table: set(checks)},
        indexes={table: set(uniques)},
        constraint_indexes={table: set(uniques)},
        constraints={table: set(checks) | set(uniques)},
        relation_kinds={table: "BASE TABLE"},
        constraint_definitions={table: dict(definitions or {})},
        unique_constraints={table: set(uniques)},
        declared_check_definitions={table: dict(declared or {})},
    )


_KIND_CHECK = {"expression": "kind IN ('pull_conflict')", "name": "kind_check"}


def test_a_check_whose_body_changed_is_replaced_then_validated():
    model = _model(checks=[_KIND_CHECK])
    live = _live(
        checks=("kind_check",),
        definitions={"kind_check": _OLD_KIND},
        declared={"kind_check": _NEW_KIND + " NOT VALID"},
    )

    operations, refusals, _ = plan([model], live)

    assert refusals == []
    replace = next(op for op in operations if op.kind == "replace_check")
    validate = next(op for op in operations if op.kind == "validate_check")
    # One statement: the table is never without the constraint.
    assert "DROP CONSTRAINT kind_check, ADD CONSTRAINT kind_check" in replace.forward_sql
    assert "NOT VALID" in replace.forward_sql
    # The reverse puts the database's old body back.
    assert "'quantity_review'" in replace.reverse_sql
    assert validate.preflight_sql.endswith(
        "WHERE NOT (kind IN ('pull_conflict')) LIMIT 1"
    )
    assert operations.index(replace) < operations.index(validate)


def test_the_same_body_spelled_by_the_server_is_not_a_change():
    """NOT VALID is how a constraint was attached, not what it asserts."""
    model = _model(checks=[_KIND_CHECK])
    live = _live(
        checks=("kind_check",),
        definitions={"kind_check": _NEW_KIND},
        declared={"kind_check": _NEW_KIND + " NOT VALID"},
    )

    assert changed_checks(model, "catalog_proposal", live) == []
    assert constraint_body(_NEW_KIND + " NOT VALID") == _NEW_KIND


def test_a_check_the_probe_could_not_spell_is_never_called_changed():
    model = _model(checks=[_KIND_CHECK])
    live = _live(checks=("kind_check",), definitions={"kind_check": _OLD_KIND})

    assert changed_checks(model, "catalog_proposal", live) == []


def test_an_undeclared_unique_constraint_is_dropped_with_its_definition_as_reverse():
    table = "external_attribute_value_map"
    old = "external_attribute_value_map_source_source_key_unique"
    model = _model(table=table)
    live = _live(
        table=table,
        uniques=(old,),
        definitions={old: "UNIQUE (source, source_value)"},
    )

    operations = orphaned_unique_constraints(model, table, live)

    assert [op.kind for op in operations] == ["drop_unique_constraint"]
    drop = operations[0]
    assert drop.safety == "destructive"
    assert drop.forward_sql == f'ALTER TABLE "{table}" DROP CONSTRAINT {old}'
    assert drop.reverse_sql == (
        f'ALTER TABLE "{table}" ADD CONSTRAINT {old} UNIQUE (source, source_value)'
    )


def test_a_unique_an_indexes_entry_creates_is_owned():
    table = "category"
    name = "category_tenant_root_name_unique"
    model = _model(
        table=table,
        indexes=[
            {
                "name": name,
                "up": f"CREATE UNIQUE INDEX IF NOT EXISTS {name} ON category (name)",
            }
        ],
    )
    live = _live(table=table, uniques=(name,), definitions={name: "UNIQUE (name)"})

    assert orphaned_unique_constraints(model, table, live) == []
