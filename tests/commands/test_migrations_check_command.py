"""``migrations:check`` — one test per violation class, on temp-directory fixtures.

The audit is a pure function of (migrations directory, model table -> declared
index names), so every case here writes real files to ``tmp_path`` and asserts
on the returned violations. No database, and no migration is ever imported —
the command parses with ``ast`` precisely so a gate never needs a connection.

The convention under test is the hard one: every file is a generated
``create_<table>_table.py``, one per model table, and the retired escape
markers (``MODEL_LESS``, ``MODEL_TRANSITION``, ``DROPPED_INDEXES``) are
violations in themselves. Framework tables are models now (``cara.models``),
named DDL lives in a model's ``__indexes__``, and data rewrites are not
migrations — so there is nothing left for a marker to legitimately shelter.
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


def _rules(violations):
    return sorted(v.rule for v in violations)


# ── clean baseline ──────────────────────────────────────────────────────────


def test_clean_directory_has_no_violations(tmp_path):
    _generated(tmp_path, "product")
    _generated(tmp_path, "listing", order=2)
    (tmp_path / "__init__.py").write_text("", encoding="utf-8")

    assert audit_migrations(tmp_path, {"product": set(), "listing": set()}) == []


def test_generated_migration_rejects_model_constant_default(tmp_path):
    _generated(
        tmp_path,
        "product",
        extra='            table.string("source").default(Product.SOURCE_MANUAL)',
    )

    violations = audit_migrations(tmp_path, {"product": set()})

    assert _rules(violations) == ["non-literal-default"]
    assert "Product.SOURCE_MANUAL" in violations[0].message
    assert violations[0].blocks_fix


def test_model_default_drift_is_reported(tmp_path):
    _generated(
        tmp_path,
        "listing",
        extra='            table.string("currency", 3).default("USD")',
    )
    model = {
        "name": "Listing",
        "table": "listing",
        "has_fields_method": True,
        "fields": {
            "currency": {"type": "string", "params": {"length": 3}},
        },
    }

    violations = audit_migrations(
        tmp_path,
        {"listing": set()},
        model_infos=[model],
    )

    drift = [item for item in violations if item.rule == "model-schema-drift"]
    assert len(drift) == 1
    assert "currency (default)" in drift[0].message
    assert not drift[0].blocks_fix


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


# ── rule 2: generated files only ────────────────────────────────────────────


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


def test_model_less_marker_is_banned(tmp_path):
    """The escape hatch is not merely closed for NEW files — a file still
    carrying the marker is a violation by itself, with a remedy naming where
    each kind of content now lives."""
    _generated(tmp_path, "product")
    (tmp_path / "9000_01_01_000000_refresh_view.py").write_text(
        '"""A view refresh that used to hide behind the marker."""\n\n'
        "MODEL_LESS = True\n\n"
        "from cara.facades import DB\n\n"
        "class RefreshView:\n"
        "    def up(self):\n"
        '        DB.statement("CREATE MATERIALIZED VIEW v AS SELECT 1")\n',
        encoding="utf-8",
    )

    violations = audit_migrations(tmp_path, {"product": set()})

    assert "banned-marker" in _rules(violations)
    marker = next(v for v in violations if v.rule == "banned-marker")
    assert "MODEL_LESS" in marker.message
    assert "__indexes__" in marker.remedy
    # Deleting the file IS the fix, so --fix may run through it.
    assert not marker.human_only


def test_marker_on_a_generated_filename_is_still_banned(tmp_path):
    """A marker used to WIN over the generated-name classification (the
    framework's own failed_job file relied on that acceptance). Framework
    tables are models now, so the marker never legitimises a file: the run is
    red until --fix deletes it and regenerates the real creator."""
    _generated(tmp_path, "product")
    (tmp_path / "9000_01_01_000000_create_failed_job_table.py").write_text(
        '"""Framework-owned dead-letter table (pre-cara.models copy)."""\n\n'
        "MODEL_LESS = True\n\n"
        "from cara.facades import DB\n\n"
        "class CreateFailedJobTable:\n"
        "    def up(self):\n"
        '        DB.statement("CREATE TABLE failed_job (id BIGSERIAL PRIMARY KEY, '
        'failed_at TIMESTAMPTZ)")\n',
        encoding="utf-8",
    )

    violations = audit_migrations(tmp_path, {"product": set(), "failed_job": set()})

    rules = _rules(violations)
    assert "banned-marker" in rules
    # Its raw CREATE TABLE still registers for duplicate detection, so the
    # marked file "covers" failed_job here — the banned-marker violation is
    # what keeps the run red until the file is deleted and regenerated.
    assert "missing-migration" not in rules
    marker = next(v for v in violations if v.rule == "banned-marker")
    assert not marker.human_only


def test_model_transition_marker_is_banned(tmp_path):
    _generated(tmp_path, "legacy_product")
    (tmp_path / "0002_01_01_000002_rename_legacy_product_to_product.py").write_text(
        '"""Rename bridge from the retired transition mechanism."""\n\n'
        'MODEL_TRANSITION = ("legacy_product", "product")\n\n'
        "from cara.facades import DB\n\n"
        "class RenameAppliedTable:\n"
        "    def up(self):\n"
        '        DB.statement("ALTER TABLE legacy_product RENAME TO product")\n',
        encoding="utf-8",
    )

    violations = audit_migrations(tmp_path, {"product": set()})

    assert "banned-marker" in _rules(violations)
    marker = next(v for v in violations if v.rule == "banned-marker")
    assert "MODEL_TRANSITION" in marker.message


def test_dropped_indexes_marker_is_banned(tmp_path):
    _generated(tmp_path, "product")
    (tmp_path / "9001_01_01_000001_retire_index.py").write_text(
        '"""Index retirement from the retired mechanism."""\n\n'
        'DROPPED_INDEXES = {"legacy_idx": "product"}\n\n'
        "from cara.facades import DB\n\n"
        "class RetireIndex:\n"
        "    def up(self):\n"
        '        DB.statement("DROP INDEX IF EXISTS legacy_idx")\n',
        encoding="utf-8",
    )

    violations = audit_migrations(tmp_path, {"product": set()})

    assert "banned-marker" in _rules(violations)
    marker = next(v for v in violations if v.rule == "banned-marker")
    assert "DROPPED_INDEXES" in marker.message


# ── rule 3: UTC everywhere ──────────────────────────────────────────────────


def test_naive_timestamp_in_migration_sql_is_reported(tmp_path):
    _generated(
        tmp_path,
        "product",
        extra=(
            "        DB.statement(\n"
            '            """\n'
            "            ALTER TABLE product ADD COLUMN taken_at TIMESTAMP\n"
            '            """\n'
            "        )\n\n"
        ),
    )

    violations = audit_migrations(tmp_path, {"product": set()})

    assert _rules(violations) == ["naive-timestamp"]
    assert "TIMESTAMPTZ" in violations[0].remedy
    # The SQL is model-authored (__indexes__); a human fixes the model.
    assert violations[0].human_only


def test_timestamp_without_time_zone_is_naive(tmp_path):
    _generated(
        tmp_path,
        "product",
        extra=(
            "        DB.statement(\n"
            '            """\n'
            "            ALTER TABLE product ADD COLUMN t TIMESTAMP WITHOUT TIME ZONE\n"
            '            """\n'
            "        )\n\n"
        ),
    )

    assert _rules(audit_migrations(tmp_path, {"product": set()})) == ["naive-timestamp"]


def test_timestamptz_and_current_timestamp_are_not_flagged(tmp_path):
    _generated(
        tmp_path,
        "product",
        extra=(
            "        DB.statement(\n"
            '            """\n'
            "            ALTER TABLE product ADD COLUMN taken_at TIMESTAMPTZ\n"
            "                NOT NULL DEFAULT CURRENT_TIMESTAMP\n"
            '            """\n'
            "        )\n"
            "        DB.statement(\n"
            '            """\n'
            "            ALTER TABLE product ADD COLUMN seen_at\n"
            "                TIMESTAMP WITH TIME ZONE DEFAULT to_timestamp(0)\n"
            '            """\n'
            "        )\n\n"
        ),
    )

    assert audit_migrations(tmp_path, {"product": set()}) == []


def test_timestamp_mentioned_only_in_a_docstring_is_not_flagged(tmp_path):
    path = _generated(tmp_path, "product")
    text = path.read_text(encoding="utf-8").replace(
        '"""Create the product table."""',
        '"""Create the product table. Its columns are TIMESTAMP-free by design."""',
    )
    path.write_text(text, encoding="utf-8")

    # Prose is not SQL: the scan reads string literals minus docstrings, so a
    # gate that cried wolf on documentation can't happen.
    assert audit_migrations(tmp_path, {"product": set()}) == []


# ── rule 4 / orphans ────────────────────────────────────────────────────────


def test_raw_sql_creating_a_model_table_twice_is_a_duplicate(tmp_path):
    _generated(tmp_path, "product")
    (tmp_path / "0002_01_01_000002_bootstrap_extra.py").write_text(
        '"""A stray file that also creates product."""\n\n'
        "from cara.facades import DB\n\n"
        "class BootstrapExtra:\n"
        "    def up(self):\n"
        '        DB.statement("CREATE TABLE product (id BIGSERIAL PRIMARY KEY)")\n',
        encoding="utf-8",
    )

    violations = audit_migrations(tmp_path, {"product": set()})

    rules = _rules(violations)
    assert "duplicate-table" in rules
    # The stray is independently an incremental-migration violation too.
    assert "incremental-migration" in rules


def test_generated_file_whose_model_vanished_is_an_orphan(tmp_path):
    _generated(tmp_path, "product")
    _generated(tmp_path, "legacy_thing", order=2)

    violations = audit_migrations(tmp_path, {"product": set()})

    assert _rules(violations) == ["orphan-migration"]
    assert "legacy_thing" in violations[0].message
    assert not violations[0].human_only


# ── rule 5: indexes belong to models ────────────────────────────────────────


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


def test_index_on_a_non_model_table_is_out_of_scope(tmp_path):
    """An index on a table no model declares is the orphan/duplicate rules'
    problem, not the index rule's — reporting it here too would double-count
    every stray file."""
    _generated(tmp_path, "product")
    (tmp_path / "0002_01_01_000002_stray.py").write_text(
        '"""Stray."""\n\n'
        "from cara.facades import DB\n\n"
        "class Stray:\n"
        "    def up(self):\n"
        '        DB.statement("CREATE INDEX x_idx ON not_a_model (y)")\n',
        encoding="utf-8",
    )

    violations = audit_migrations(tmp_path, {"product": set()})

    assert "undeclared-index" not in _rules(violations)


# ── unparseable files ───────────────────────────────────────────────────────


def test_unparseable_file_is_reported_and_blocks_fix(tmp_path):
    _generated(tmp_path, "product")
    (tmp_path / "0002_01_01_000002_broken.py").write_text(
        "def broken(:\n", encoding="utf-8"
    )

    violations = audit_migrations(tmp_path, {"product": set()})

    assert _rules(violations) == ["unparseable"]
    assert violations[0].blocks_fix


def test_parse_migration_file_reports_banned_markers(tmp_path):
    path = tmp_path / "0001_01_01_000001_create_product_table.py"
    path.write_text(
        '"""Doc."""\nMODEL_LESS = True\nDROPPED_INDEXES = {"a": "b"}\n',
        encoding="utf-8",
    )

    entry = parse_migration_file(path)

    assert entry.banned_markers == ("MODEL_LESS", "DROPPED_INDEXES")
    assert entry.generated_table == "product"


# ── exit codes ──────────────────────────────────────────────────────────────


def test_report_exit_codes(tmp_path):
    command = MigrationsCheckCommand.__new__(MigrationsCheckCommand)
    printed: list[str] = []
    for attr in ("info", "success", "warning", "error"):
        setattr(command, attr, lambda msg, _p=printed: _p.append(str(msg)))
    command.option = lambda name, default=None: False

    assert command._report([], table_count=3) == 0

    from cara.commands.core.Violation import Violation

    violation = Violation(
        rule="incremental-migration", path="x.py", message="m", remedy="r"
    )
    assert command._report([violation], table_count=3) == 1


def test_a_column_merely_named_timestamp_is_not_naive_sql(tmp_path):
    """``table.datetime("timestamp")`` is a COLUMN NAME in a generated file,
    not a type declaration. Naive TIMESTAMP as a type only appears inside a
    longer SQL statement, so bare identifiers are out of scope."""
    _generated(
        tmp_path,
        "product_lifecycle",
        extra='            table.datetime("timestamp")\n',
    )

    assert audit_migrations(tmp_path, {"product_lifecycle": set()}) == []
