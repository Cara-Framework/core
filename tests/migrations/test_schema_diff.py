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
    assert summarize_change_name(
        "product", [FieldDiff("renamed", "title", old_name="name")]
    )[0] == "rename_name_to_title_on_product_table"
    # mixed change set falls back to the generic name
    assert summarize_change_name(
        "product", [FieldDiff("added", "a"), FieldDiff("removed", "b")]
    ) == ("update_product_table", "UpdateProductTable")
