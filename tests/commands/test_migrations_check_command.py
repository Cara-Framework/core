"""``migrations:check`` — one test per violation class, on temp-directory fixtures.

The audit is a pure function of (migrations directory, model table -> declared
index names), so every case here writes real files to ``tmp_path`` and asserts
on the returned violations. No database, and no migration is ever imported —
the command parses with ``ast`` precisely so a gate never needs a connection.
"""

from __future__ import annotations

from cara.commands.core.MigrationsCheckCommand import (
    MigrationsCheckCommand,
    audit_migrations,
    parse_migration_file,
)

# A generated file as the generator authors it: Blueprint DSL, raw CREATE INDEX
# for the model's __indexes__ declarations, no comments.
_GENERATED = '''"""Create the {table} table."""

from cara.eloquent.migrations import Migration
from cara.facades import DB


class Create{camel}Table(Migration):
    def up(self):
        with self.schema.create("{table}") as table:
            table.big_increments("id")
            table.timestamps()
{extra}
    def down(self):
        self.schema.drop_if_exists("{table}")
'''


def _generated(directory, table, order=1, extra=""):
    """Write a generated create-table migration and return its path."""
    camel = "".join(part.capitalize() for part in table.split("_"))
    name = f"{order:04d}_01_01_{order:06d}_create_{table}_table.py"
    path = directory / name
    path.write_text(
        _GENERATED.format(table=table, camel=camel, extra=extra), encoding="utf-8"
    )
    return path


def _transition(directory, old_table, new_table, order, statements=()):
    path = directory / (
        f"{order:04d}_01_01_{order:06d}_rename_{old_table}_to_{new_table}.py"
    )
    transition_sql = "".join(
        f'        DB.statement("{statement}")\n' for statement in statements
    )
    path.write_text(
        f'"""Preserve the applied {old_table} table while its model is renamed."""\n\n'
        "from cara.facades import DB\n\n"
        f'MODEL_TRANSITION = ("{old_table}", "{new_table}")\n\n'
        "class RenameAppliedTable:\n"
        "    def up(self):\n"
        f'        DB.statement("ALTER TABLE {old_table} RENAME TO {new_table}")\n'
        f"{transition_sql}",
        encoding="utf-8",
    )
    return path


def _rules(violations):
    return sorted(v.rule for v in violations)


# ── clean baseline ──────────────────────────────────────────────────────────


def test_clean_directory_has_no_violations(tmp_path):
    _generated(tmp_path, "product")
    _generated(tmp_path, "listing", order=2)
    (tmp_path / "__init__.py").write_text("", encoding="utf-8")

    assert audit_migrations(tmp_path, {"product": set(), "listing": set()}) == []


# ── applied model transitions ──────────────────────────────────────────────


def test_single_applied_model_transition_covers_current_model(tmp_path):
    _generated(tmp_path, "legacy_product")
    _transition(tmp_path, "legacy_product", "product", order=2)

    assert audit_migrations(tmp_path, {"product": set()}) == []


def test_two_edge_applied_model_transition_chain_is_supported(tmp_path):
    _generated(tmp_path, "legacy_product")
    _transition(tmp_path, "legacy_product", "catalog_product", order=2)
    _transition(tmp_path, "catalog_product", "product", order=3)

    assert audit_migrations(tmp_path, {"product": set()}) == []


def test_model_transition_chain_rejects_cycle(tmp_path):
    _generated(tmp_path, "legacy_product")
    _transition(tmp_path, "legacy_product", "catalog_product", order=2)
    _transition(tmp_path, "catalog_product", "legacy_product", order=3)

    violations = audit_migrations(tmp_path, {"product": set()})

    assert "cyclic-model-transition" in _rules(violations)


def test_model_transition_chain_rejects_branch_or_merge(tmp_path):
    _generated(tmp_path, "legacy_product")
    _generated(tmp_path, "other_product", order=2)
    _transition(tmp_path, "legacy_product", "product", order=3)
    _transition(tmp_path, "other_product", "product", order=4)

    violations = audit_migrations(tmp_path, {"product": set()})

    assert _rules(violations).count("duplicate-model-transition") == 2


def test_model_transition_requires_exact_sql_docstring_and_terminal_model(tmp_path):
    _generated(tmp_path, "legacy_product")
    path = _transition(tmp_path, "legacy_product", "product", order=2)
    path.write_text(
        'MODEL_TRANSITION = ("legacy_product", "product")\nclass Broken:\n    pass\n',
        encoding="utf-8",
    )

    violations = audit_migrations(tmp_path, {"listing": set()})
    messages = "\n".join(violation.message for violation in violations)

    assert "missing explanation" in messages
    assert "does not prove" in messages
    assert "has no model" in messages


# ── rule 1: one file per table ──────────────────────────────────────────────


def test_model_table_with_no_migration_is_reported(tmp_path):
    _generated(tmp_path, "product")

    violations = audit_migrations(tmp_path, {"product": set(), "listing": set()})

    assert _rules(violations) == ["missing-migration"]
    assert "listing" in violations[0].message
    assert not violations[0].human_only


def test_two_generated_files_for_one_table_are_a_duplicate(tmp_path):
    _generated(tmp_path, "product", order=1)
    _generated(tmp_path, "product", order=2)

    violations = audit_migrations(tmp_path, {"product": set()})

    assert _rules(violations) == ["duplicate-table"]
    # Regeneration would delete one side unasked, so a human decides.
    assert violations[0].blocks_fix


# ── rule 2: no incremental migrations ───────────────────────────────────────


def test_incremental_file_is_a_violation(tmp_path):
    _generated(tmp_path, "product")
    (tmp_path / "0002_01_01_000002_add_sku_to_product.py").write_text(
        '"""Add sku."""\n', encoding="utf-8"
    )

    violations = audit_migrations(tmp_path, {"product": set()})

    assert _rules(violations) == ["incremental-migration"]
    assert "put the change in the model and regenerate" in violations[0].remedy
    # Fixable: --overwrite deletes it and the model carries the change.
    assert not violations[0].human_only


# ── rule 3: MODEL_LESS must explain itself ──────────────────────────────────


def test_model_less_without_docstring_is_reported(tmp_path):
    _generated(tmp_path, "product")
    (tmp_path / "9000_01_01_000000_refresh_view.py").write_text(
        "MODEL_LESS = True\n\n"
        "from cara.facades import DB\n\n"
        "class RefreshView:\n"
        "    def up(self):\n"
        '        DB.statement("CREATE MATERIALIZED VIEW v AS SELECT 1")\n',
        encoding="utf-8",
    )

    violations = audit_migrations(tmp_path, {"product": set()})

    assert _rules(violations) == ["unexplained-model-less"]
    # Never auto-fixed: only a human knows why no model can own the object.
    assert violations[0].human_only


def test_documented_model_less_file_is_accepted(tmp_path):
    _generated(tmp_path, "product")
    (tmp_path / "9000_01_01_000000_create_failed_job_table.py").write_text(
        '"""Framework-owned dead-letter table; no app model owns it."""\n\n'
        "MODEL_LESS = True\n\n"
        "from cara.facades import DB\n\n"
        "class CreateFailedJobTable:\n"
        "    def up(self):\n"
        '        DB.statement("CREATE TABLE failed_job (id BIGSERIAL PRIMARY KEY, '
        'failed_at TIMESTAMPTZ)")\n',
        encoding="utf-8",
    )

    # The file NAME matches the generated shape, but the marker wins — otherwise
    # the escape hatch would be reported as an orphan on every run.
    assert audit_migrations(tmp_path, {"product": set()}) == []


# ── rule 4: UTC everywhere ──────────────────────────────────────────────────


def _model_less_with_sql(directory, sql):
    (directory / "9000_01_01_000000_create_audit_view.py").write_text(
        '"""A materialized view no model can express."""\n\n'
        "MODEL_LESS = True\n\n"
        "from cara.facades import DB\n\n"
        "class CreateAuditView:\n"
        "    def up(self):\n"
        f'        DB.statement("""{sql}""")\n',
        encoding="utf-8",
    )


def test_naive_timestamp_in_hand_written_sql_is_reported(tmp_path):
    _generated(tmp_path, "product")
    _model_less_with_sql(
        tmp_path, "CREATE TABLE audit_snapshot (taken_at TIMESTAMP NOT NULL)"
    )

    violations = audit_migrations(tmp_path, {"product": set()})

    assert _rules(violations) == ["naive-timestamp"]
    assert "TIMESTAMPTZ" in violations[0].remedy
    # A human picks the conversion; --fix must never rewrite hand-written SQL.
    assert violations[0].human_only


def test_timestamp_without_time_zone_is_naive(tmp_path):
    _generated(tmp_path, "product")
    _model_less_with_sql(
        tmp_path,
        "CREATE TABLE audit_snapshot (taken_at TIMESTAMP WITHOUT TIME ZONE)",
    )

    assert _rules(audit_migrations(tmp_path, {"product": set()})) == ["naive-timestamp"]


def test_timestamptz_and_current_timestamp_are_not_flagged(tmp_path):
    _generated(tmp_path, "product")
    _model_less_with_sql(
        tmp_path,
        "CREATE TABLE audit_snapshot ("
        "taken_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, "
        "seen_at TIMESTAMP WITH TIME ZONE DEFAULT to_timestamp(0))",
    )

    assert audit_migrations(tmp_path, {"product": set()}) == []


def test_timestamp_mentioned_only_in_a_docstring_is_not_flagged(tmp_path):
    _generated(tmp_path, "product")
    (tmp_path / "9000_01_01_000000_create_audit_view.py").write_text(
        '"""A view no model owns. Its columns are TIMESTAMP-free by design."""\n\n'
        "MODEL_LESS = True\n\n"
        "from cara.facades import DB\n\n"
        "class CreateAuditView:\n"
        "    def up(self):\n"
        '        DB.statement("CREATE MATERIALIZED VIEW v AS SELECT 1")\n',
        encoding="utf-8",
    )

    # Prose is not SQL: the scan reads string literals minus docstrings, so a
    # gate that cried wolf on documentation can't happen.
    assert audit_migrations(tmp_path, {"product": set()}) == []


# ── rule 5 / orphans ────────────────────────────────────────────────────────


def test_model_less_file_creating_a_model_table_is_a_duplicate(tmp_path):
    _generated(tmp_path, "product")
    _model_less_with_sql(tmp_path, "CREATE TABLE product (id BIGSERIAL PRIMARY KEY)")

    violations = audit_migrations(tmp_path, {"product": set()})

    assert _rules(violations) == ["duplicate-table"]


def test_generated_file_whose_model_vanished_is_an_orphan(tmp_path):
    _generated(tmp_path, "product")
    _generated(tmp_path, "legacy_thing", order=2)

    violations = audit_migrations(tmp_path, {"product": set()})

    assert _rules(violations) == ["orphan-migration"]
    assert "legacy_thing" in violations[0].message
    assert not violations[0].human_only


# ── rule 7: indexes belong to models ────────────────────────────────────────


_INDEX_SQL = (
    "        DB.statement(\n"
    '            """\n'
    "            CREATE INDEX IF NOT EXISTS product_sku_idx ON product (sku)\n"
    '            """\n'
    "        )\n\n"
)


def test_index_only_in_a_migration_is_reported(tmp_path):
    _generated(tmp_path, "product", extra=_INDEX_SQL)

    violations = audit_migrations(tmp_path, {"product": set()})

    assert _rules(violations) == ["undeclared-index"]
    assert "product_sku_idx" in violations[0].message
    # Regenerating would DROP the index rather than repair it.
    assert violations[0].blocks_fix


def test_index_declared_by_the_model_is_accepted(tmp_path):
    _generated(tmp_path, "product", extra=_INDEX_SQL)

    assert audit_migrations(tmp_path, {"product": {"product_sku_idx"}}) == []


def test_later_valid_transition_may_rename_historical_index(tmp_path):
    historical_index = _INDEX_SQL.replace(
        "product_sku_idx",
        "legacy_product_sku_idx",
    )
    _generated(tmp_path, "product", extra=historical_index)
    _generated(tmp_path, "legacy_state", order=2)
    _transition(
        tmp_path,
        "legacy_state",
        "state",
        order=3,
        statements=(
            "ALTER INDEX legacy_product_sku_idx RENAME TO product_sku_idx",
        ),
    )

    assert (
        audit_migrations(
            tmp_path,
            {"product": {"product_sku_idx"}, "state": set()},
        )
        == []
    )


def test_later_valid_transition_may_drop_historical_index(tmp_path):
    historical_index = _INDEX_SQL.replace(
        "product_sku_idx",
        "legacy_product_sku_idx",
    )
    _generated(tmp_path, "product", extra=historical_index)
    _generated(tmp_path, "legacy_state", order=2)
    _transition(
        tmp_path,
        "legacy_state",
        "state",
        order=3,
        statements=("DROP INDEX legacy_product_sku_idx",),
    )

    assert audit_migrations(
        tmp_path,
        {"product": set(), "state": set()},
    ) == []


def test_index_transition_rejects_rename_to_undeclared_target(tmp_path):
    historical_index = _INDEX_SQL.replace(
        "product_sku_idx",
        "legacy_product_sku_idx",
    )
    _generated(tmp_path, "product", extra=historical_index)
    _generated(tmp_path, "legacy_state", order=2)
    _transition(
        tmp_path,
        "legacy_state",
        "state",
        order=3,
        statements=(
            "ALTER INDEX legacy_product_sku_idx RENAME TO stale_product_sku_idx",
        ),
    )

    violations = audit_migrations(
        tmp_path,
        {"product": {"product_sku_idx"}, "state": set()},
    )

    assert _rules(violations) == [
        "invalid-index-transition",
        "undeclared-index",
    ]


def test_sql_outside_valid_transition_cannot_mask_historical_index(tmp_path):
    _generated(tmp_path, "product", extra=_INDEX_SQL)
    _model_less_with_sql(tmp_path, "DROP INDEX product_sku_idx")

    violations = audit_migrations(tmp_path, {"product": set()})

    assert _rules(violations) == ["undeclared-index"]


def test_index_transition_must_sort_after_historical_creator(tmp_path):
    _generated(tmp_path, "legacy_widget")
    _transition(
        tmp_path,
        "legacy_widget",
        "widget",
        order=2,
        statements=("DROP INDEX IF EXISTS future_product_sku_idx",),
    )
    future_index = _INDEX_SQL.replace(
        "product_sku_idx",
        "future_product_sku_idx",
    )
    _generated(tmp_path, "product", order=3, extra=future_index)

    violations = audit_migrations(
        tmp_path,
        {"widget": set(), "product": set()},
    )

    assert _rules(violations) == ["undeclared-index"]


def test_index_on_a_non_model_table_is_out_of_scope(tmp_path):
    _generated(tmp_path, "product")
    _model_less_with_sql(tmp_path, "CREATE INDEX audit_view_idx ON audit_view (taken_on)")

    # No model can own a materialized view's index, so rule 7 does not apply.
    assert audit_migrations(tmp_path, {"product": set()}) == []


# ── unparseable files ───────────────────────────────────────────────────────


def test_unparseable_migration_is_reported_and_blocks_fix(tmp_path):
    _generated(tmp_path, "product")
    (tmp_path / "0002_01_01_000002_broken.py").write_text("def up(:\n", encoding="utf-8")

    violations = audit_migrations(tmp_path, {"product": set()})

    assert _rules(violations) == ["unparseable"]
    assert violations[0].blocks_fix


def test_unparseable_file_is_never_treated_as_model_less(tmp_path):
    path = tmp_path / "0002_01_01_000002_broken.py"
    path.write_text("MODEL_LESS = True\ndef up(:\n", encoding="utf-8")

    entry = parse_migration_file(path)

    assert entry.syntax_error is not None
    assert entry.model_less is False


# ── exit codes ──────────────────────────────────────────────────────────────


def _command() -> MigrationsCheckCommand:
    command = MigrationsCheckCommand(application=None)
    command.set_parsed_options({})
    return command


def test_report_returns_zero_only_when_clean(tmp_path):
    assert _command()._report([], table_count=3) == 0


def test_report_returns_non_zero_on_any_violation(tmp_path):
    _generated(tmp_path, "product", extra=_INDEX_SQL)
    violations = audit_migrations(tmp_path, {"product": set()})

    # CI gates on this: human-only violations still fail the build.
    assert _command()._report(violations, table_count=1) == 1
