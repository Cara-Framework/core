"""``make:data-migration`` — scaffolds a PLAIN hand-written migration.

A data migration is NOT a special class (the Laravel way): it's a plain
``Migration`` whose ``up()``/``down()`` mutate rows via ``DB.table(...).update()``
or ``DB.statement(...)`` instead of changing schema. This command just scaffolds
that blank ``Migration``, named with the SHARED migration counter so it sorts —
and therefore runs — AFTER every existing schema migration, and WITHOUT a
``_table`` suffix so the schema --overwrite globs can never clobber it.

Pure command/unit logic — no live database. The migrations directory is pointed
at a tmp dir via ``PathManager.set_path_override`` so the generator's counter +
filename machinery resolve through the real ``paths()`` plumbing.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from cara.commands.core.MakeDataMigrationCommand import MakeDataMigrationCommand
from cara.eloquent.migrations import Migration
from cara.eloquent.migrations.MigrationFileManager import MigrationFileManager
from cara.support.PathManager import PathManager


def _make_command() -> MakeDataMigrationCommand:
    cmd = MakeDataMigrationCommand(application=None)
    cmd.set_parsed_options({})
    cmd.console = MagicMock()
    return cmd


@pytest.fixture()
def migrations_dir(tmp_path):
    target = tmp_path / "migrations"
    target.mkdir()
    PathManager.set_path_override("migrations", str(target))
    try:
        yield target
    finally:
        PathManager._overrides.pop("migrations", None)


def _load_class(file_path):
    return MigrationFileManager(str(file_path.parent)).load_migration_class(str(file_path))


def test_scaffold_uses_the_raw_name_no_special_prefix(migrations_dir):
    cmd = _make_command()
    cmd.handle("backfill product slug")

    files = list(migrations_dir.glob("*.py"))
    assert len(files) == 1
    name = files[0].name
    # NNNN_01_01_NNNNNN_<slug>.py — the user's name verbatim (Laravel-style),
    # no "data_" prefix, no "_table" suffix.
    assert name.endswith("_backfill_product_slug.py")
    assert "_data_" not in name and "_table" not in name
    head = name[: -len("_backfill_product_slug.py")]
    parts = head.split("_")
    assert parts[0].isdigit() and len(parts[0]) == 4  # 4-digit sequence
    assert parts[1] == "01" and parts[2] == "01"
    assert parts[3].isdigit() and len(parts[3]) == 6  # 6-digit micro-order


def test_scaffold_sorts_after_existing_schema_migrations(migrations_dir):
    (migrations_dir / "0001_01_01_000000_create_widget_table.py").write_text("# schema")
    (migrations_dir / "0002_01_01_000001_create_gadget_table.py").write_text("# schema")
    (migrations_dir / ".migration_counter").write_text("2")

    cmd = _make_command()
    cmd.handle("backfill gadget price")

    names = sorted(p.name for p in migrations_dir.glob("*.py"))
    assert names[-1].startswith("0003_")  # lexicographically last → runs last
    assert names[-1].endswith("_backfill_gadget_price.py")


def test_scaffold_slugifies_camelcase(migrations_dir):
    cmd = _make_command()
    cmd.handle("BackfillSellerRating")
    name = next(migrations_dir.glob("*.py")).name
    assert name.endswith("_backfill_seller_rating.py")


def test_scaffolded_file_is_an_importable_plain_migration(migrations_dir):
    cmd = _make_command()
    cmd.handle("backfill product slug")
    file_path = next(migrations_dir.glob("*.py"))

    cls = _load_class(file_path)
    # A PLAIN Migration (Laravel: no special data-migration class) the executor
    # picks up like any other.
    assert issubclass(cls, Migration)
    assert cls.__name__ == "BackfillProductSlug"
    assert callable(getattr(cls, "up", None))
    assert callable(getattr(cls, "down", None))
    # The body shows the Laravel data idiom — query builder + raw statement.
    body = file_path.read_text()
    assert "DB.table(" in body
    assert "DB.statement(" in body
    assert "from cara.facades import DB" in body
