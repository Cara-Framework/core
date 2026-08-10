"""Structured schema diff — column parse + typed add/remove/alter/rename diff.

Pins the keystone capability the old name-only string differ lacked: detecting
column TYPE/length/nullable/unique CHANGES, RENAMES (vs data-losing drop+add),
and carrying a removed column's real definition for a lossless down().
"""

from __future__ import annotations

from cara.eloquent.migrations.ModelMigrationComparator import (
    Column,
    FieldDiff,
    ModelMigrationComparator,
    summarize_change_name,
)


def _diff(model_cols, migration_cols):
    return ModelMigrationComparator.__new__(ModelMigrationComparator)._diff(
        model_cols, migration_cols
    )


def test_parse_column_line_captures_type_length_modifiers():
    col = ModelMigrationComparator._parse_column_line(
        'table.string("slug", 1000).nullable().unique()'
    )
    assert col.name == "slug"
    assert col.type == "string"
    assert col.length == 1000
    assert col.nullable is True
    assert col.unique is True
    dec = ModelMigrationComparator._parse_column_line('table.decimal("rating", 5, 2)')
    assert (dec.type, dec.precision, dec.scale) == ("decimal", 5, 2)


def test_added_and_removed():
    model = {"new_col": Column("new_col", "integer")}
    migration = {"old_col": Column("old_col", "text", raw_line='table.text("old_col")')}
    diffs = _diff(model, migration)
    kinds = {d.kind for d in diffs}
    assert kinds == {"added", "removed"}
    removed = next(d for d in diffs if d.kind == "removed")
    # lossless down(): the removed column keeps its REAL definition, not varchar
    assert removed.column.raw_line == 'table.text("old_col")'


def test_altered_detects_length_and_nullable_change():
    model = {"title": Column("title", "string", length=2000, nullable=True)}
    migration = {"title": Column("title", "string", length=255, nullable=False)}
    diffs = _diff(model, migration)
    assert len(diffs) == 1 and diffs[0].kind == "altered"
    assert set(diffs[0].changed_attrs) == {"length", "nullable"}


def test_unchanged_column_produces_no_diff():
    same = lambda: {"x": Column("x", "string", length=50, nullable=True)}  # noqa: E731
    assert _diff(same(), same()) == []


def test_rename_detected_instead_of_drop_add():
    # one removed + one added with the SAME parsed signature => a rename
    model = {"full_name": Column("full_name", "string", length=255)}
    migration = {
        "name": Column("name", "string", length=255, raw_line='table.string("name", 255)')
    }
    diffs = _diff(model, migration)
    assert len(diffs) == 1 and diffs[0].kind == "renamed"
    assert diffs[0].old_name == "name" and diffs[0].name == "full_name"


def test_intent_revealing_names():
    assert summarize_change_name("product", [FieldDiff("added", "sku")]) == (
        "add_sku_to_product_table",
        "AddSkuToProduct",
    )
    assert summarize_change_name("product", [FieldDiff("removed", "sku")])[0] == (
        "drop_sku_from_product_table"
    )
    assert (
        summarize_change_name(
            "product", [FieldDiff("renamed", "title", old_name="name")]
        )[0]
        == "rename_name_to_title_on_product_table"
    )
    # mixed change set falls back to the generic name
    assert summarize_change_name(
        "product", [FieldDiff("added", "a"), FieldDiff("removed", "b")]
    ) == ("update_product_table", "UpdateProductTable")


def test_default_and_scalar_index_changes_are_detected():
    model = {
        "state": Column("state", "string", default="active", has_default=True, index=True)
    }
    migration = {
        "state": Column(
            "state", "string", default="'pending'", has_default=True, index=False
        )
    }
    diffs = _diff(model, migration)
    assert len(diffs) == 1
    assert set(diffs[0].changed_attrs) == {"default", "index"}


def test_intent_named_migrations_are_applied_to_snapshot(tmp_path):
    create = tmp_path / "0001_create_widget_table.py"
    create.write_text(
        'class X:\n    def up(self):\n        with self.schema.create("widget") as table:\n'
        '            table.string("name", 255)\n    def down(self):\n        pass\n'
    )
    add = tmp_path / "0002_add_status_to_widget_table.py"
    add.write_text(
        'class X:\n    def up(self):\n        with self.schema.table("widget") as table:\n'
        '            table.string("status", 20).default("active")\n'
        "    def down(self):\n        pass\n"
    )
    comparator = ModelMigrationComparator.__new__(ModelMigrationComparator)
    comparator.migrations_dir = tmp_path

    columns, exists = comparator._migration_columns("widget")

    assert exists is True
    assert set(columns) == {"name", "status"}


def test_scalar_standalone_constraints_round_trip_without_diff(tmp_path):
    migration = tmp_path / "0001_create_widget_table.py"
    migration.write_text(
        'class X:\n    def up(self):\n        with self.schema.create("widget") as table:\n'
        '            table.string("email", 255)\n'
        '            table.string("sid", 64)\n'
        '            table.index(["email"])\n'
        '            table.unique(["sid"])\n'
        "    def down(self):\n        pass\n"
    )
    comparator = ModelMigrationComparator.__new__(ModelMigrationComparator)
    comparator.migrations_dir = tmp_path
    model_info = {
        "table": "widget",
        "fields": {
            "email": {"type": "string", "params": {"length": 255}},
            "sid": {"type": "string", "params": {"length": 64}},
        },
        "composite_indexes": [{"columns": ["email"], "name": None}],
        "composite_uniques": [{"columns": ["sid"], "name": None}],
    }

    assert comparator.compare_model_with_migrations(model_info) == []


# ── standalone declarations that are NOT columns ────────────────────────────


def test_named_single_column_index_is_applied_to_the_column():
    """``table.index(["expires_at"], name="...")`` marks the column indexed.

    The name kwarg's value used to be swept into the column-name scrape, so a
    NAMED single-column index looked multi-column, was skipped, and the model
    side reported a phantom ``index`` diff on every regenerate."""
    cols = {"expires_at": Column("expires_at", "datetime")}
    ModelMigrationComparator._apply_standalone_indexes(
        'table.index(["expires_at"], name="product_event_expires_at_idx")', cols
    )
    assert cols["expires_at"].index is True


def test_named_single_column_unique_is_applied_to_the_column():
    cols = {"selector": Column("selector", "string", length=64)}
    ModelMigrationComparator._apply_standalone_indexes(
        'table.unique(["selector"], name="session_selector_unique")', cols
    )
    assert cols["selector"].unique is True


def test_composite_index_marks_no_single_column():
    cols = {
        "name": Column("name", "string"),
        "occurred_at": Column("occurred_at", "datetime"),
    }
    ModelMigrationComparator._apply_standalone_indexes(
        'table.index(["name", "occurred_at"], name="product_event_name_time_idx")',
        cols,
    )
    assert cols["name"].index is False
    assert cols["occurred_at"].index is False


def test_standalone_primary_is_not_a_column_line():
    """``table.primary("job_id")`` declares the KEY of an existing column.

    Parsed as a column line it replaced the real ``string(64)`` snapshot with
    a phantom ``primary``-typed column, and every compare reported a
    type/length diff the regenerate could never satisfy."""
    body = 'table.string("job_id", 64)\ntable.primary("job_id")'
    lines = ModelMigrationComparator._blueprint_column_lines(body)
    assert lines == ['table.string("job_id", 64)']
