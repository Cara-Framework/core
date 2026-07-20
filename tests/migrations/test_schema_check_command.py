"""Tests for ``SchemaCheckCommand`` — the model-vs-live-DB drift gate.

These exercise the command's pure comparison logic (no live database): column
flattening from model declarations, the declared-vs-live diff, and the
conservative type/nullable mismatch rules. The introspection + skip-on-no-DB
paths are mock-driven.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from cara.commands.core.SchemaCheckCommand import SchemaCheckCommand


def _make_command(options=None) -> SchemaCheckCommand:
    cmd = SchemaCheckCommand(application=None)
    cmd.set_parsed_options(options or {})
    cmd.console = MagicMock()
    return cmd


# --- _declared_columns: model fields -> concrete columns ----------------------


def test_declared_columns_expands_timestamps_and_soft_deletes():
    cmd = _make_command()
    model = {
        "name": "Widget",
        "table": "widget",
        "uses_soft_deletes": True,
        "fields": {
            "id": {"type": "big_increments", "params": {}},
            "name": {"type": "string", "params": {"length": 255}},
            "note": {"type": "text", "params": {"nullable": True}},
            "timestamps": {"type": "timestamps", "params": {}},
        },
    }

    cols = cmd._declared_columns(model)

    assert set(cols) == {"id", "name", "note", "created_at", "updated_at", "deleted_at"}
    # timestamps expand to nullable datetime columns
    assert cols["created_at"] == {"type": "datetime", "nullable": True}
    assert cols["updated_at"] == {"type": "datetime", "nullable": True}
    # soft-delete column injected from uses_soft_deletes
    assert cols["deleted_at"] == {"type": "datetime", "nullable": True}
    # declared nullability honoured
    assert cols["note"]["nullable"] is True
    assert cols["name"]["nullable"] is False


def test_declared_columns_harvests_raw_sql_added_columns():
    """GENERATED columns declared via the ``__indexes__`` raw-SQL escape hatch
    (e.g. a tsvector ``search_vector``) ARE declared by the model — they must
    not be flagged as 'present in DB but not in model'."""
    cmd = _make_command()
    model = {
        "name": "Product",
        "table": "product",
        "fields": {
            "id": {"type": "big_increments", "params": {}},
            "title": {"type": "string", "params": {}},
        },
        "indexes": [
            {
                "name": "product_search_vector_col",
                "up": "ALTER TABLE product ADD COLUMN IF NOT EXISTS search_vector "
                "tsvector GENERATED ALWAYS AS (...) STORED",
                "down": "ALTER TABLE product DROP COLUMN IF EXISTS search_vector",
            },
            {
                "name": "product_search_vector_gin",
                "up": "CREATE INDEX IF NOT EXISTS product_search_vector_gin "
                "ON product USING GIN (search_vector)",
                "down": "DROP INDEX IF EXISTS product_search_vector_gin",
            },
        ],
    }

    cols = cmd._declared_columns(model)
    assert "search_vector" in cols
    # Raw-SQL columns: type unknown (skips type check), nullable unknown.
    assert cols["search_vector"]["nullable"] is None

    # And a live table that HAS search_vector now shows no drift for it.
    live = {
        "id": {"data_type": "bigint", "is_nullable": False},
        "title": {"data_type": "character varying", "is_nullable": False},
        "search_vector": {"data_type": "tsvector", "is_nullable": True},
    }
    assert cmd._diff_table("listing", cols, live) == []


# --- _diff_table: missing / extra columns ------------------------------------


def test_diff_table_reports_missing_and_extra_columns():
    cmd = _make_command()
    declared = {
        "id": {"type": "big_increments", "nullable": False},
        "name": {"type": "string", "nullable": False},
        "new_col": {"type": "string", "nullable": True},  # in model, not DB
    }
    live = {
        "id": {"data_type": "bigint", "is_nullable": False},
        "name": {"data_type": "character varying", "is_nullable": False},
        "legacy_col": {"data_type": "text", "is_nullable": True},  # in DB, not model
    }

    issues = cmd._diff_table("listing", declared, live)

    assert any("new_col" in i and "MISSING in database" in i for i in issues)
    assert any("legacy_col" in i and "NOT declared in model" in i for i in issues)


# --- _diff_column: nullable + conservative type ------------------------------


def test_diff_column_flags_nullability_mismatch():
    cmd = _make_command()
    issues = cmd._diff_column(
        "listing",
        "email",
        {"type": "string", "nullable": False},
        {"data_type": "character varying", "is_nullable": True},
    )
    assert any("nullability differs" in i for i in issues)


def test_diff_column_flags_clear_type_mismatch():
    cmd = _make_command()
    # model says boolean, DB has an integer column -> clearly different category.
    issues = cmd._diff_column(
        "listing",
        "is_active",
        {"type": "boolean", "nullable": False},
        {"data_type": "integer", "is_nullable": False},
    )
    assert any("type differs" in i for i in issues)


def test_diff_column_does_not_flag_aliased_types():
    cmd = _make_command()
    # string <-> character varying are the SAME category; no false positive.
    issues = cmd._diff_column(
        "listing",
        "name",
        {"type": "string", "nullable": False},
        {"data_type": "character varying", "is_nullable": False},
    )
    assert issues == []


def test_diff_column_skips_unknown_types_to_avoid_false_positives():
    cmd = _make_command()
    # An un-catalogued DB type must NOT produce a spurious mismatch.
    issues = cmd._diff_column(
        "listing",
        "geo",
        {"type": "string", "nullable": False},
        {"data_type": "some_exotic_pg_type", "is_nullable": False},
    )
    assert issues == []


# --- in-sync table yields no drift -------------------------------------------


def test_diff_table_clean_when_in_sync():
    cmd = _make_command()
    declared = {
        "id": {"type": "big_increments", "nullable": False},
        "name": {"type": "string", "nullable": False},
        "active": {"type": "boolean", "nullable": False},
    }
    live = {
        "id": {"data_type": "bigint", "is_nullable": False},
        "name": {"data_type": "character varying", "is_nullable": False},
        "active": {"data_type": "boolean", "is_nullable": False},
    }
    assert cmd._diff_table("listing", declared, live) == []
