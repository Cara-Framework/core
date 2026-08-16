"""The planner: classification, reverse statements, and what it refuses.

These are the decisions that make a derived plan safe to run on production,
so each one is pinned by behaviour rather than by the SQL string it happens
to emit today.
"""

from __future__ import annotations

from cara.schema import (
    ADDITIVE,
    DESTRUCTIVE,
    LOCKING,
    LiveSchema,
    migration_to_run,
    plan,
)


def _col(data_type="character varying", nullable=True, max_length=255):
    return {"data_type": data_type, "is_nullable": nullable, "max_length": max_length}


def _live(
    columns=None, indexes=None, checks=None, table="product", constraint_indexes=None
):
    """A deployed table. ``id`` is always present — every model declares it,
    so leaving it out of a fixture would put a spurious drop in every plan and
    hide the delta each test is actually about."""
    if columns is None:
        return LiveSchema(tables={}, checks={}, indexes={}, constraint_indexes={})
    return LiveSchema(
        tables={table: {"id": _col("bigint", False, None), **columns}},
        checks={table: set(checks or ())},
        indexes={table: set(indexes or ())},
        constraint_indexes={table: set(constraint_indexes or ())},
    )


def _field(field_type, **params):
    return {"type": field_type, "params": params}


def _model(fields, table="product", indexes=None, renamed=None):
    """A model declaring ``id`` plus the fields under test — see ``_live``."""
    return {
        "name": "Product",
        "table": table,
        "has_fields_method": True,
        "fields": {"id": _field("big_increments"), **fields},
        "indexes": indexes or [],
        "renamed_from": renamed or {},
    }


# ── additive ────────────────────────────────────────────────────────────────


def test_new_nullable_column_is_additive_and_reversible():
    model = _model({"sku": _field("string", length=64, nullable=True)})
    operations, refusals, _ = plan([model], _live({}))

    assert refusals == []
    assert len(operations) == 1
    operation = operations[0]
    assert operation.kind == "add_column"
    assert operation.safety == ADDITIVE
    assert 'ADD COLUMN "sku" VARCHAR(64) NULL' in operation.forward_sql
    assert 'DROP COLUMN "sku"' in operation.reverse_sql
    # The column comes back; what was written into it does not.
    assert operation.restores_data is False


def test_relaxing_to_nullable_is_additive():
    model = _model({"sku": _field("string", length=64, nullable=True)})
    operations, _, _ = plan([model], _live({"sku": _col(nullable=False)}))
    assert [op.safety for op in operations] == [ADDITIVE]
    assert operations[0].kind == "drop_not_null"


def test_missing_table_runs_its_generated_creator():
    """The generated migration already renders this table's DDL; re-rendering
    it here would be a second renderer of one model.

    ``forward_sql`` therefore NAMES the file rather than carrying SQL — it
    used to carry a comment, which made apply die on "can't execute an empty
    query" and meant a new table could never reach a deployed database."""
    model = _model({"sku": _field("string", length=64)})
    operations, _, _ = plan([model], _live(None))
    assert [op.key for op in operations] == ["product"]
    operation = operations[0]
    assert operation.kind == "create_table"
    assert migration_to_run(operation.forward_sql) == "create_product_table"
    assert operation.reverse_sql == 'DROP TABLE IF EXISTS "product"'


# ── locking ─────────────────────────────────────────────────────────────────


def test_not_null_with_default_becomes_the_three_step_recipe():
    model = _model({"tier": _field("string", length=20, default="basic")})
    operations, refusals, _ = plan([model], _live({}))

    assert refusals == []
    assert [op.kind for op in operations] == [
        "add_column",
        "backfill_column",
        "set_not_null",
    ]
    add, backfill, tighten = operations
    assert add.safety == ADDITIVE and "NULL" in add.forward_sql
    assert backfill.safety == LOCKING
    assert "'basic'" in backfill.forward_sql
    # A backfill has no honest undo — the previous values are not recorded.
    assert backfill.reverse_sql is None
    assert tighten.safety == LOCKING and "SET NOT NULL" in tighten.forward_sql


def test_tightening_an_existing_column_carries_a_null_preflight():
    """Classifying this as locking says what it COSTS, not whether it works.
    On a column with one NULL row it fails outright, halfway through a deploy,
    after the operations before it already applied — so the operation carries
    the read-only query that answers the question in advance."""
    model = _model({"sku": _field("string", length=64)})
    operations, _, _ = plan([model], _live({"sku": _col(nullable=True)}))
    operation = operations[0]
    assert operation.kind == "set_not_null"
    assert operation.safety == LOCKING
    assert operation.preflight_sql == (
        'SELECT 1 FROM "product" WHERE "sku" IS NULL LIMIT 1'
    )
    assert "NULL rows" in operation.preflight_failure


def test_integer_widening_is_locking_and_its_reverse_is_not_promised():
    model = _model({"views": _field("big_integer")})
    operations, _, _ = plan([model], _live({"views": _col("integer", True, None)}))
    widen = next(op for op in operations if op.kind == "widen_column")
    assert widen.safety == LOCKING
    assert "BIGINT" in widen.forward_sql
    assert widen.restores_data is False
    assert any("out-of-range" in note for note in widen.notes)


def test_non_concurrent_model_index_is_locking_concurrent_is_additive():
    entry = {
        "name": "product_sku_idx",
        "up": "CREATE INDEX product_sku_idx ON product (sku)",
        "down": "DROP INDEX IF EXISTS product_sku_idx",
    }
    model = _model({"sku": _field("string", length=64, nullable=True)}, indexes=[entry])
    operations, _, _ = plan([model], _live({"sku": _col()}))
    index_op = next(op for op in operations if op.kind == "create_index")
    assert index_op.safety == LOCKING
    assert index_op.transactional is True

    concurrent = dict(
        entry, up="CREATE INDEX CONCURRENTLY product_sku_idx ON product (sku)"
    )
    operations, _, _ = plan(
        [
            _model(
                {"sku": _field("string", length=64, nullable=True)},
                indexes=[concurrent],
            )
        ],
        _live({"sku": _col()}),
    )
    index_op = next(op for op in operations if op.kind == "create_index")
    assert index_op.safety == ADDITIVE
    assert index_op.transactional is False


def test_raw_partial_unique_index_gets_a_duplicate_preflight():
    entry = {
        "name": "product_tenant_sku_unique",
        "up": (
            "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS "
            "product_tenant_sku_unique ON product (tenant_id, sku) "
            "WHERE sku IS NOT NULL"
        ),
        "down": "DROP INDEX CONCURRENTLY IF EXISTS product_tenant_sku_unique",
    }
    model = _model(
        {
            "tenant_id": _field("big_integer", nullable=False),
            "sku": _field("string", length=64, nullable=True),
        },
        indexes=[entry],
    )

    operations, _, _ = plan(
        [model],
        _live(
            {
                "tenant_id": _col("bigint", False),
                "sku": _col(),
            }
        ),
    )

    index_op = next(op for op in operations if op.kind == "create_index")
    assert index_op.preflight_sql == (
        'SELECT 1 FROM "product" WHERE sku IS NOT NULL '
        "GROUP BY tenant_id, sku HAVING COUNT(*) > 1 LIMIT 1"
    )
    assert "product_tenant_sku_unique" in index_op.preflight_failure


def test_an_index_the_database_already_has_is_not_planned():
    entry = {
        "name": "product_sku_idx",
        "up": "CREATE INDEX product_sku_idx ON product (sku)",
        "down": "DROP INDEX IF EXISTS product_sku_idx",
    }
    model = _model({"sku": _field("string", length=64, nullable=True)}, indexes=[entry])
    operations, _, _ = plan([model], _live({"sku": _col()}, indexes=["product_sku_idx"]))
    assert [op for op in operations if op.kind == "create_index"] == []


def test_presence_is_judged_by_what_the_sql_creates_not_the_entry_label():
    """An entry's ``name`` is a LABEL: this one creates two differently-named
    constraints. Keying presence on the label alone makes it replan forever,
    and a plan with permanent phantom entries trains its reader to skim."""
    entry = {
        "name": "product_counter_checks",
        "up": (
            "ALTER TABLE product ADD CONSTRAINT product_attempts_check "
            "CHECK (attempts >= 0); ALTER TABLE product ADD CONSTRAINT "
            "product_version_check CHECK (version > 0)"
        ),
        "down": "ALTER TABLE product DROP CONSTRAINT IF EXISTS product_attempts_check",
    }
    model = _model({"sku": _field("string", length=64, nullable=True)}, indexes=[entry])
    live = _live(
        {"sku": _col()}, checks=["product_attempts_check", "product_version_check"]
    )
    operations, _, _ = plan([model], live)
    assert [op for op in operations if op.kind == "create_index"] == []


# ── destructive ─────────────────────────────────────────────────────────────


def test_column_only_in_the_database_is_destructive_and_shape_only():
    model = _model({"sku": _field("string", length=64, nullable=True)})
    operations, _, _ = plan([model], _live({"sku": _col(), "legacy": _col()}))
    drop = next(op for op in operations if op.kind == "drop_column")
    assert drop.safety == DESTRUCTIVE
    assert drop.restores_data is False
    assert 'ADD COLUMN "legacy"' in drop.reverse_sql
    assert any("__renamed_from__" in note for note in drop.notes)


# ── renames are DECLARED, never inferred ────────────────────────────────────


def test_declared_rename_keeps_the_data():
    model = _model(
        {"sku": _field("string", length=64, nullable=True)}, renamed={"sku": "code"}
    )
    operations, _, _ = plan([model], _live({"code": _col()}))

    assert [op.kind for op in operations] == ["rename_column"]
    operation = operations[0]
    assert 'RENAME COLUMN "code" TO "sku"' in operation.forward_sql
    assert 'RENAME COLUMN "sku" TO "code"' in operation.reverse_sql
    assert operation.safety == ADDITIVE
    assert operation.restores_data is True


def test_undeclared_rename_is_reported_as_a_drop_and_an_add_not_guessed():
    """The diff cannot distinguish a rename from a drop plus an add. Guessing
    is what loses the column's data, so the planner states both facts and
    leaves the judgement to a human."""
    model = _model({"sku": _field("string", length=64, nullable=True)})
    operations, _, _ = plan([model], _live({"code": _col()}))
    assert {op.kind for op in operations} == {"add_column", "drop_column"}


# ── refusals ────────────────────────────────────────────────────────────────


def test_not_null_without_a_default_is_refused():
    model = _model({"tier": _field("string", length=20)})
    operations, refusals, _ = plan([model], _live({}))
    assert operations == []
    assert len(refusals) == 1
    assert "expand/contract" in refusals[0]


def test_incompatible_type_change_is_refused_not_guessed():
    model = _model({"count": _field("integer")})
    operations, refusals, _ = plan([model], _live({"count": _col("character varying")}))
    assert operations == []
    assert "USING clause" in refusals[0]


def test_unmappable_type_is_refused():
    model = _model({"shape": _field("geometry")})
    operations, refusals, _ = plan([model], _live({}))
    assert operations == []
    assert "no PostgreSQL mapping" in refusals[0]


def test_raw_indexes_column_is_refused_rather_than_re_rendered():
    model = _model(
        {},
        indexes=[
            {
                "name": "product_search_vector",
                "up": "ALTER TABLE product ADD COLUMN search_vector tsvector",
                "down": "ALTER TABLE product DROP COLUMN search_vector",
            }
        ],
    )
    operations, refusals, _ = plan([model], _live({}))
    assert any("raw __indexes__" in refusal for refusal in refusals)
    assert not any(op.kind == "add_column" for op in operations)


# ── ordering ────────────────────────────────────────────────────────────────


def test_plan_is_ordered_safest_first():
    model = _model({"sku": _field("string", length=64, nullable=True)})
    operations, _, _ = plan([model], _live({"legacy": _col(), "old": _col()}))
    ranks = {ADDITIVE: 0, LOCKING: 1, DESTRUCTIVE: 2}
    safeties = [ranks[op.safety] for op in operations]
    assert safeties == sorted(safeties)


# ── field-level indexes reach a deployed database too ───────────────────────


def test_field_level_index_is_planned_when_the_database_lacks_it():
    """``field.string(...).index()`` is the common spelling. Before this, only
    ``__indexes__`` entries were compared, so an index added this way never
    reached a deployed database and nothing reported it."""
    model = _model({"sku": _field("string", length=64, nullable=True, index=True)})
    operations, _, _ = plan([model], _live({"sku": _col()}))
    index_op = next(op for op in operations if op.kind == "create_index")
    assert index_op.key == "product:product_sku_index"
    assert "CREATE INDEX CONCURRENTLY" in index_op.forward_sql
    # CONCURRENTLY by default: a plain build holds a write lock, and the model
    # only asked for an index.
    assert index_op.safety == ADDITIVE
    assert index_op.transactional is False
    assert "DROP INDEX CONCURRENTLY" in index_op.reverse_sql


def test_field_level_unique_uses_the_unique_naming_convention():
    model = _model({"slug": _field("string", length=64, nullable=True, unique=True)})
    operations, _, _ = plan([model], _live({"slug": _col()}))
    index_op = next(op for op in operations if op.kind == "create_index")
    assert index_op.key == "product:product_slug_unique"
    assert "CREATE UNIQUE INDEX CONCURRENTLY" in index_op.forward_sql


def test_field_level_index_the_database_already_has_is_not_planned():
    model = _model({"sku": _field("string", length=64, nullable=True, index=True)})
    operations, _, _ = plan(
        [model], _live({"sku": _col()}, indexes=["product_sku_index"])
    )
    assert [op for op in operations if op.kind == "create_index"] == []


def test_composite_index_declaration_is_planned_by_its_declared_name():
    model = _model({"sku": _field("string", length=64, nullable=True)})
    model["composite_indexes"] = [
        {"columns": ["sku", "brand"], "name": "product_sku_brand_idx"}
    ]
    operations, _, _ = plan([model], _live({"sku": _col(), "brand": _col()}))
    index_op = next(op for op in operations if op.kind == "create_index")
    assert index_op.key == "product:product_sku_brand_idx"
    assert '"sku", "brand"' in index_op.forward_sql


def test_long_index_names_are_truncated_the_way_postgres_truncates_them():
    """Postgres stores identifiers truncated to 63 bytes. Comparing the full
    convention name against the catalogue finds nothing, so every long-named
    index reads as missing and is replanned on every single run."""
    table = "advertising_intent_entry"
    model = _model(
        {
            "advertising_intent_id": _field("big_integer"),
            "external_listing_id": _field("string", length=64),
        },
        table=table,
    )
    model["composite_uniques"] = [
        {"columns": ["advertising_intent_id", "external_listing_id"]}
    ]
    stored = "advertising_intent_entry_advertising_intent_id_external_listing"
    assert len(stored) == 63
    live = _live(
        {
            "advertising_intent_id": _col("bigint", False, None),
            "external_listing_id": _col(),
        },
        indexes=[stored],
        table=table,
    )
    operations, _, _ = plan([model], live)
    assert [op for op in operations if op.kind == "create_index"] == []


def test_one_index_declared_two_ways_is_planned_once():
    """The discoverer records ``field.string(...).index()`` under both the
    param flag and a standalone declaration. A plan that lists the same
    CREATE INDEX twice is a plan nobody trusts."""
    model = _model({"sku": _field("string", length=64, nullable=True, index=True)})
    model["composite_indexes"] = [{"columns": ["sku"]}]
    operations, _, _ = plan([model], _live({"sku": _col()}))
    index_ops = [op for op in operations if op.kind == "create_index"]
    assert len(index_ops) == 1
    assert index_ops[0].key == "product:product_sku_index"


# ── objects the database has and the model does not ─────────────────────────


def test_orphaned_index_is_planned_as_a_destructive_drop():
    """Removing ``.index()`` from a model is a real instruction. Without this
    the index survives in production forever and nothing reports it."""
    model = _model({"sku": _field("string", length=64, nullable=True)})
    operations, _, _ = plan(
        [model], _live({"sku": _col()}, indexes=["product_stale_idx"])
    )
    drop = next(op for op in operations if op.kind == "drop_index")
    assert drop.safety == DESTRUCTIVE
    assert "DROP INDEX CONCURRENTLY" in drop.forward_sql
    assert drop.transactional is False
    # No declaration exists to rebuild it from, and saying so is the point.
    assert drop.reverse_sql is None


def test_constraint_backed_index_is_never_dropped():
    """The index behind a PRIMARY KEY or UNIQUE constraint belongs to the
    constraint — dropping it means dropping the constraint, which no model
    asked for."""
    model = _model({"sku": _field("string", length=64, nullable=True)})
    live = _live(
        {"sku": _col()},
        indexes=["product_pkey", "product_public_id_unique"],
        constraint_indexes=["product_public_id_unique"],
    )
    operations, _, _ = plan([model], live)
    assert [op for op in operations if op.kind == "drop_index"] == []


def test_an_index_a_model_declares_is_not_dropped():
    model = _model({"sku": _field("string", length=64, nullable=True, index=True)})
    operations, _, _ = plan(
        [model], _live({"sku": _col()}, indexes=["product_sku_index"])
    )
    assert [op for op in operations if op.kind == "drop_index"] == []


def test_an_index_created_by_a_model_ddl_entry_is_not_dropped():
    entry = {
        "name": "product_partial_idx",
        "up": "CREATE INDEX product_partial_idx ON product (sku) WHERE sku IS NOT NULL",
        "down": "DROP INDEX IF EXISTS product_partial_idx",
    }
    model = _model({"sku": _field("string", length=64, nullable=True)}, indexes=[entry])
    operations, _, _ = plan(
        [model], _live({"sku": _col()}, indexes=["product_partial_idx"])
    )
    assert [op for op in operations if op.kind == "drop_index"] == []


def test_orphaned_table_is_reported_and_never_dropped():
    """A DROP TABLE derived from a diff is where an autogenerator does its
    worst damage: the diff cannot tell an abandoned table from a partition
    child, an extension's table, or the migration tracker."""
    model = _model({"sku": _field("string", length=64, nullable=True)})
    live = LiveSchema(
        tables={
            "product": {"id": _col("bigint", False, None), "sku": _col()},
            "zombie": {"id": _col("bigint", False, None)},
            "migrations": {"id": _col("bigint", False, None)},
        },
        checks={},
        indexes={},
        constraint_indexes={},
    )
    operations, _, notices = plan([model], live)
    assert not any(op.kind == "drop_table" for op in operations)
    assert len(notices) == 1
    assert "zombie" in notices[0]
    # The framework's own tracker has no model by design and must never be
    # reported as an orphan.
    assert "migrations" not in notices[0]


# ── preflight: will this actually succeed against the rows in there? ────────


def test_unique_index_carries_a_duplicate_preflight():
    model = _model({"slug": _field("string", length=64, nullable=True, unique=True)})
    operations, _, _ = plan([model], _live({"slug": _col()}))
    operation = next(op for op in operations if op.kind == "create_index")
    assert operation.preflight_sql == (
        'SELECT 1 FROM "product" GROUP BY "slug" HAVING COUNT(*) > 1 LIMIT 1'
    )
    assert "duplicate" in operation.preflight_failure


def test_a_plain_index_needs_no_preflight():
    """A non-unique index cannot fail on data — claiming a check where there
    is none would train the reader to skip the ones that matter."""
    model = _model({"sku": _field("string", length=64, nullable=True, index=True)})
    operations, _, _ = plan([model], _live({"sku": _col()}))
    operation = next(op for op in operations if op.kind == "create_index")
    assert operation.preflight_sql is None


def test_the_not_null_recipes_third_step_checks_its_own_backfill():
    model = _model({"tier": _field("string", length=20, default="basic")})
    operations, _, _ = plan([model], _live({}))
    tighten = next(op for op in operations if op.kind == "set_not_null")
    assert tighten.preflight_sql is not None
    assert "the backfill above did not cover them" in tighten.preflight_failure


def test_an_added_nullable_column_needs_no_preflight():
    model = _model({"sku": _field("string", length=64, nullable=True)})
    operations, _, _ = plan([model], _live({}))
    assert operations[0].preflight_sql is None


# ── declared backfill: what the EXISTING rows get ───────────────────────────


def test_backfill_from_makes_a_not_null_column_derivable():
    """A schema default says what a NEW row gets. ``session_version`` had to
    start as each row's existing ``auth_version``, not as 1, or the rotation
    those rows had been through would be undone — which is why a constant was
    the wrong answer and the operation was refused outright."""
    model = _model({"session_version": _field("integer", backfill_from='"auth_version"')})
    operations, refusals, _ = plan(
        [model], _live({"auth_version": _col("integer", False, None)})
    )

    assert refusals == []
    backfill = next(op for op in operations if op.kind == "backfill_column")
    assert '"session_version" = "auth_version"' in backfill.forward_sql
    assert "declared backfill" in backfill.reason
    # No constant default was declared, so nothing invents one.
    tighten = next(op for op in operations if op.kind == "set_not_null")
    assert "SET DEFAULT" not in tighten.forward_sql


def test_backfill_from_wins_over_a_constant_default():
    """When a model declares both, the backfill is the more specific
    statement — it is there precisely because the two answers differ."""
    model = _model(
        {"tier": _field("string", length=20, default="basic", backfill_from="'legacy'")}
    )
    operations, _, _ = plan([model], _live({}))
    backfill = next(op for op in operations if op.kind == "backfill_column")
    assert "'legacy'" in backfill.forward_sql
    # The default still becomes the column default for NEW rows.
    tighten = next(op for op in operations if op.kind == "set_not_null")
    assert "SET DEFAULT 'basic'" in tighten.forward_sql


def test_neither_default_nor_backfill_is_still_refused():
    model = _model({"tier": _field("string", length=20)})
    operations, refusals, _ = plan([model], _live({}))
    assert operations == []
    assert "backfill_from" in refusals[0]
