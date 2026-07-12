"""``.change()`` migrations must COMPILE to ALTER COLUMN — not crash.

The platform compilers have emitted ``ALTER COLUMN … TYPE`` from
``Table.changed_columns`` all along (Postgres even hasattr-guards the slot),
but ``Table`` never declared the slot or the ``change()`` method — so every
auto-generated update migration (``table.text("col").nullable().change()``)
died with ``'Table' object has no attribute 'change'`` and the whole
model-driven ALTER flow was decorative. Same latent gap ``drop_column`` had
(its fix is memorialized in Table.__init__'s docstring); these pins keep the
pair honest.
"""

from __future__ import annotations

from cara.eloquent.schema.Blueprint import Blueprint
from cara.eloquent.schema.platforms.PostgresPlatform import PostgresPlatform


def _alter_blueprint() -> Blueprint:
    # Mirrors ``with self.schema.table("t") as table`` in a migration body.
    return Blueprint("pipeline_product_trace", action="alter")


def test_change_reclassifies_last_added_column():
    bp = _alter_blueprint()
    bp.text("job_id").nullable().change()
    assert "job_id" in bp.table.changed_columns
    assert "job_id" not in bp.table.added_columns


def test_change_compiles_to_alter_column_type():
    bp = _alter_blueprint()
    bp.text("job_id").nullable().change()
    sql = PostgresPlatform().compile_alter_sql(bp.table)
    joined = " ".join(sql)
    assert "ALTER COLUMN" in joined
    assert "job_id" in joined
    assert "TEXT" in joined.upper()
    # TEXT carries no length — the changed-path used to emit ``TEXT(None)``.
    assert "(None)" not in joined


def test_change_on_varchar_keeps_length():
    bp = _alter_blueprint()
    bp.string("status", 30).change()
    sql = " ".join(PostgresPlatform().compile_alter_sql(bp.table))
    assert "VARCHAR(30)" in sql.upper().replace(" ", "") or "(30)" in sql


def test_mixed_add_and_change_split_correctly():
    bp = _alter_blueprint()
    bp.integer("new_col").nullable()          # plain ADD
    bp.text("old_col").nullable().change()    # MODIFY
    assert "new_col" in bp.table.added_columns
    assert "old_col" in bp.table.changed_columns
