"""The planner must be able to LAND every shape ``schema:check`` verifies.

Two blind spots kept a deployed database from ever converging on its models:

* ``field.check(...)`` declarations were reported by ``schema:check`` and
  silently omitted by the planner — 115 missing CHECK constraints on one real
  database, with no derivable path to close them.
* A blueprint ``field.unique(...)`` is a CONSTRAINT from zero, but evolve
  built a bare ``CREATE UNIQUE INDEX CONCURRENTLY`` and stopped — and
  ``schema:check`` exempts only constraint-BACKED indexes, so every unique
  the planner itself created came back as an orphan on the next check,
  forever.
"""

from __future__ import annotations

from cara.schema import LiveSchema, plan


def _model(table="product", **extra):
    return {
        "table": table,
        "has_fields_method": True,
        "fields": {"id": {"type": "big_increments", "params": {}}},
        "indexes": [],
        **extra,
    }


def _live(table="product", *, checks=(), indexes=(), constraints=()):
    return LiveSchema(
        tables={
            table: {
                "id": {"data_type": "bigint", "is_nullable": False, "max_length": None},
            }
        },
        checks={table: set(checks)},
        indexes={table: set(indexes)},
        constraint_indexes={},
        constraints={table: set(constraints)},
        relation_kinds={table: "BASE TABLE"},
    )


def test_a_declared_check_lands_as_not_valid_then_validate():
    model = _model(checks=[{"expression": "price >= 0", "name": "product_price_check"}])

    operations, refusals, _ = plan([model], _live())

    assert refusals == []
    kinds = [(op.kind, op.key) for op in operations]
    assert ("add_check", "product:product_price_check") in kinds
    assert ("validate_check", "product:product_price_check:validate") in kinds
    add = next(op for op in operations if op.kind == "add_check")
    validate = next(op for op in operations if op.kind == "validate_check")
    assert "NOT VALID" in add.forward_sql
    assert add.reverse_sql is not None
    assert validate.preflight_sql is not None
    assert "NOT (price >= 0)" in validate.preflight_sql
    assert operations.index(add) < operations.index(validate)


def test_an_unnamed_check_derives_the_generator_name():
    """The auto-name must equal what the Blueprint writes into a from-zero
    migration — two names for one declaration would mean permanent drift."""
    model = _model(checks=[{"expression": "current_price >= 0", "name": None}])

    operations, _, _ = plan([model], _live())

    assert any(op.key == "product:product_current_price_0_check" for op in operations)


def test_a_present_check_is_not_replanned():
    model = _model(checks=[{"expression": "price >= 0", "name": "product_price_check"}])

    operations, _, _ = plan([model], _live(checks=("product_price_check",)))

    assert operations == []


def test_an_undeclared_check_is_a_destructive_drop():
    model = _model()

    operations, _, _ = plan([model], _live(checks=("product_legacy_check",)))

    assert [op.kind for op in operations] == ["drop_check"]
    assert operations[0].safety == "destructive"


def test_a_check_created_by_named_ddl_is_not_an_orphan():
    model = _model(
        indexes=[
            {
                "name": "product_audit_check",
                "up": (
                    "ALTER TABLE product ADD CONSTRAINT product_audit_check "
                    "CHECK (price >= 0)"
                ),
                "down": "ALTER TABLE product DROP CONSTRAINT product_audit_check",
            }
        ]
    )

    operations, _, _ = plan([model], _live(checks=("product_audit_check",)))

    assert operations == []


def test_a_blueprint_unique_builds_the_index_and_adopts_it():
    model = _model(
        composite_uniques=[{"columns": ["id", "tenant_id"], "name": None}],
    )

    operations, _, _ = plan([model], _live())

    kinds = [op.kind for op in operations]
    assert "create_index" in kinds and "add_unique_constraint" in kinds
    create = next(op for op in operations if op.kind == "create_index")
    adopt = next(op for op in operations if op.kind == "add_unique_constraint")
    assert "UNIQUE INDEX CONCURRENTLY" in create.forward_sql
    assert "UNIQUE USING INDEX product_id_tenant_id_unique" in adopt.forward_sql
    # The CONCURRENTLY build must have finished before the adopt runs.
    assert operations.index(create) < operations.index(adopt)


def test_a_bare_unique_index_is_adopted_not_recreated_and_not_orphaned():
    """The exact state my first evolve apply left behind: the declared unique
    exists only as a bare index. One adopt, no rebuild, no orphan drop."""
    model = _model(
        composite_uniques=[{"columns": ["id", "tenant_id"], "name": None}],
    )

    operations, _, _ = plan(
        [model], _live(indexes=("product_id_tenant_id_unique",))
    )

    assert [op.kind for op in operations] == ["add_unique_constraint"]


def _live_with_column(table, column, data_type, max_length):
    live = _live(table)
    live.tables[table][column] = {
        "data_type": data_type,
        "is_nullable": False,
        "max_length": max_length,
    }
    return live


def _text_model(column, field_type, **params):
    return _model(
        fields={
            "id": {"type": "big_increments", "params": {}},
            column: {"type": field_type, "params": {"nullable": False, **params}},
        }
    )


def test_a_text_declaration_widens_a_bounded_live_column():
    """The rename gap in the field: ``source_key`` VARCHAR(255) renamed to
    ``source_value`` TEXT arrived with its old bound still on, and the check
    reported it while the planner shrugged."""
    model = _text_model("source_value", "text")

    operations, refusals, _ = plan(
        [model], _live_with_column("product", "source_value", "character varying", 255)
    )

    assert refusals == []
    assert [op.kind for op in operations] == ["widen_column"]
    assert "TYPE TEXT" in operations[0].forward_sql


def test_a_longer_varchar_declaration_widens_the_bound():
    model = _text_model("source_key", "string", length=190)

    operations, refusals, _ = plan(
        [model], _live_with_column("product", "source_key", "character varying", 160)
    )

    assert refusals == []
    assert [op.kind for op in operations] == ["widen_column"]
    assert "VARCHAR(190)" in operations[0].forward_sql


def test_a_narrowing_declaration_stays_the_operators_decision():
    """Mirrors the integer rule: the planner derives nothing lossy —
    ``schema:check`` reports the narrowing, a human resolves it."""
    model = _text_model("sku", "string", length=40)

    operations, refusals, _ = plan(
        [model], _live_with_column("product", "sku", "character varying", 120)
    )

    assert operations == [] and refusals == []


def test_matching_bounds_are_not_replanned():
    model = _text_model("sku", "string", length=120)

    operations, refusals, _ = plan(
        [model], _live_with_column("product", "sku", "character varying", 120)
    )

    assert operations == [] and refusals == []


def test_a_constraint_backed_unique_is_finished_work():
    model = _model(
        composite_uniques=[{"columns": ["id", "tenant_id"], "name": None}],
    )

    operations, _, _ = plan(
        [model],
        _live(
            indexes=("product_id_tenant_id_unique",),
            constraints=("product_id_tenant_id_unique",),
        ),
    )

    assert operations == []
