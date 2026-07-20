"""Tests for ``MakeMigrationCommand`` — focused on the --overwrite purge
contract (one generated file per table, ``MODEL_LESS = True`` the only
exemption), the clobber safety guard, the hand-edit detector, and the summary.

These exercise pure command logic with no live database: the model
discoverer / comparator / generator are never invoked. The migrations
directory is pointed at a tmp dir via ``PathManager.set_path_override`` so
``_partition_migrations`` resolves through the real ``paths()`` machinery
without touching the consumer's real migrations.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from cara.commands.core.MakeMigrationCommand import MakeMigrationCommand
from cara.support.PathManager import PathManager


def _make_command(options=None) -> MakeMigrationCommand:
    cmd = MakeMigrationCommand(application=None)
    cmd.set_parsed_options(options or {})
    cmd.console = MagicMock()
    return cmd


@pytest.fixture()
def migrations_dir(tmp_path):
    """Point ``paths("migrations")`` at a tmp dir for the duration of a test."""
    target = tmp_path / "migrations"
    target.mkdir()
    PathManager.set_path_override("migrations", str(target))
    try:
        yield target
    finally:
        PathManager._overrides.pop("migrations", None)


# Content the generator actually emits — must NOT be flagged as hand-edited.
_GENERATED_CREATE = '''"""CreateWidgetTable Migration."""

from cara.eloquent.migrations import Migration


class CreateWidgetTable(Migration):
    def up(self):
        with self.schema.create("widget") as table:
            table.increments("id")
            table.string("name", 255)
            table.timestamps()

    def down(self):
        self.schema.drop("widget")
'''

# A generated update with the generator's own annotations — still NOT hand-edited.
_GENERATED_UPDATE_WITH_ANNOTATIONS = '''"""AddNoteToWidget Migration."""

from cara.eloquent.migrations import Migration


class AddNoteToWidget(Migration):
    def up(self):
        with self.schema.table("widget") as table:
            table.drop_column("old")  # DESTRUCTIVE: drops data — review before applying
            table.string("note").change()  # altered: nullable

    def down(self):
        with self.schema.table("widget") as table:
            table.string("note").change()
'''

# A generated migration with a raw __indexes__ DB.statement block — NOT hand-edited.
_GENERATED_WITH_DB_STATEMENT = '''"""CreateWidgetTable Migration."""

from cara.eloquent.migrations import Migration
from cara.facades import DB


class CreateWidgetTable(Migration):
    def up(self):
        with self.schema.create("widget") as table:
            table.increments("id")

        DB.statement("""
            CREATE UNIQUE INDEX widget_name_uq ON widget (name)
        """)

    def down(self):
        self.schema.drop("widget")
'''


# A hand-written incremental migration — the class --overwrite must now purge.
_HAND_WRITTEN = '''"""BackfillWidgetSlugs Migration."""

from cara.eloquent.migrations import Migration
from cara.facades import DB


class BackfillWidgetSlugs(Migration):
    def up(self):
        DB.statement("UPDATE widget SET slug = lower(name) WHERE slug IS NULL")

    def down(self):
        pass
'''

# A model-less object (materialized view) marked for preservation.
_MODEL_LESS_FILE = '''"""CreatePriceViews Migration."""

from cara.eloquent.migrations import Migration
from cara.facades import DB

MODEL_LESS = True


class CreatePriceViews(Migration):
    def up(self):
        DB.statement("CREATE MATERIALIZED VIEW price_daily AS SELECT 1")

    def down(self):
        DB.statement("DROP MATERIALIZED VIEW IF EXISTS price_daily")
'''


def _write(d, name, content):
    p = d / name
    p.write_text(content, encoding="utf-8")
    return p


# --- _looks_hand_edited -------------------------------------------------------


def test_generated_create_not_flagged(migrations_dir):
    cmd = _make_command()
    p = _write(
        migrations_dir, "0001_01_01_000000_create_widget_table.py", _GENERATED_CREATE
    )
    assert cmd._looks_hand_edited(p) is False


def test_generated_update_annotations_not_flagged(migrations_dir):
    cmd = _make_command()
    p = _write(
        migrations_dir,
        "0002_01_01_000000_add_note_to_widget_table.py",
        _GENERATED_UPDATE_WITH_ANNOTATIONS,
    )
    assert cmd._looks_hand_edited(p) is False


def test_generated_db_statement_not_flagged(migrations_dir):
    cmd = _make_command()
    p = _write(
        migrations_dir,
        "0003_01_01_000000_create_widget_table.py",
        _GENERATED_WITH_DB_STATEMENT,
    )
    assert cmd._looks_hand_edited(p) is False


def test_human_comment_flagged(migrations_dir):
    cmd = _make_command()
    edited = _GENERATED_CREATE.replace(
        "            table.timestamps()",
        "            table.timestamps()\n            # NOTE: keep this column for the legacy importer",
    )
    p = _write(migrations_dir, "0001_01_01_000000_create_widget_table.py", edited)
    assert cmd._looks_hand_edited(p) is True


def test_inline_human_comment_flagged(migrations_dir):
    cmd = _make_command()
    edited = _GENERATED_CREATE.replace(
        '            table.string("name", 255)',
        '            table.string("name", 255)  # keep for the legacy importer',
    )
    p = _write(migrations_dir, "0001_01_01_000000_create_widget_table.py", edited)
    assert cmd._looks_hand_edited(p) is True


def test_custom_down_logic_flagged(migrations_dir):
    cmd = _make_command()
    edited = _GENERATED_CREATE.replace(
        '        self.schema.drop("widget")',
        '        for t in ("widget", "widget_audit"):\n            self.schema.drop(t)',
    )
    p = _write(migrations_dir, "0001_01_01_000000_create_widget_table.py", edited)
    assert cmd._looks_hand_edited(p) is True


def test_unreadable_file_treated_as_hand_edited(migrations_dir):
    cmd = _make_command()
    missing = migrations_dir / "does_not_exist.py"
    # erring on the side of caution: can't read → assume worth protecting
    assert cmd._looks_hand_edited(missing) is True


# --- _partition_migrations: the one-file-per-table contract -------------------


def test_partition_dooms_every_unmarked_file_including_unrelated_tables(migrations_dir):
    # The old rule ("does this file touch a model table?") let hand-written
    # add_*/backfill_* migrations survive forever, so the directory stopped
    # being a function of the models. EVERY unmarked .py is now doomed.
    cmd = _make_command()
    _write(migrations_dir, "0001_01_01_000000_create_widget_table.py", _GENERATED_CREATE)
    _write(
        migrations_dir, "0002_01_01_000000_add_note_to_widget_table.py", _GENERATED_CREATE
    )
    _write(migrations_dir, "0003_01_01_000000_backfill_widget_slugs.py", _HAND_WRITTEN)
    _write(migrations_dir, "0004_01_01_000000_create_gadget_table.py", _GENERATED_CREATE)

    doomed, preserved = cmd._partition_migrations()
    assert sorted(p.name for p in doomed) == [
        "0001_01_01_000000_create_widget_table.py",
        "0002_01_01_000000_add_note_to_widget_table.py",
        "0003_01_01_000000_backfill_widget_slugs.py",
        "0004_01_01_000000_create_gadget_table.py",
    ]
    assert preserved == []


def test_partition_preserves_model_less_marked_files(migrations_dir):
    cmd = _make_command()
    _write(migrations_dir, "0001_01_01_000000_create_widget_table.py", _GENERATED_CREATE)
    _write(migrations_dir, "9982_01_01_000000_create_price_views.py", _MODEL_LESS_FILE)

    doomed, preserved = cmd._partition_migrations()
    assert [p.name for p in doomed] == ["0001_01_01_000000_create_widget_table.py"]
    assert [p.name for p in preserved] == ["9982_01_01_000000_create_price_views.py"]


def test_partition_ignores_package_init(migrations_dir):
    cmd = _make_command()
    _write(migrations_dir, "__init__.py", "")
    doomed, preserved = cmd._partition_migrations()
    assert doomed == [] and preserved == []


@pytest.mark.parametrize(
    "source",
    [
        "MODEL_LESS = False",
        "MODEL_LESS = 1",
        "MODEL_LESS: bool = True",
        "def f():\n    MODEL_LESS = True\n",
        "class C:\n    MODEL_LESS = True\n",
        "OTHER = True",
        "",
    ],
)
def test_model_less_marker_requires_module_level_literal_true(source, tmp_path):
    expected = source == "MODEL_LESS: bool = True"
    path = tmp_path / "m.py"
    assert MakeMigrationCommand._declares_model_less(source, path) is expected


def test_unparseable_file_is_not_treated_as_marked(tmp_path):
    # A broken migration must not dodge the sweep by failing to parse.
    assert (
        MakeMigrationCommand._declares_model_less("def up(:\n", tmp_path / "m.py")
        is False
    )


# --- _confirm_clobber gating --------------------------------------------------


def test_confirm_clobber_no_targets_proceeds(migrations_dir):
    cmd = _make_command()
    assert cmd._confirm_clobber(*cmd._partition_migrations()) is True


def test_confirm_clobber_clean_files_proceeds_without_prompt(migrations_dir):
    cmd = _make_command()
    cmd.confirm = MagicMock(
        side_effect=AssertionError("should not prompt for clean files")
    )
    _write(migrations_dir, "0001_01_01_000000_create_widget_table.py", _GENERATED_CREATE)
    assert cmd._confirm_clobber(*cmd._partition_migrations()) is True


def test_confirm_clobber_hand_edited_prompts_and_respects_no(migrations_dir):
    cmd = _make_command()
    cmd.confirm = MagicMock(return_value=False)
    edited = _GENERATED_CREATE.replace(
        "            table.timestamps()",
        "            table.timestamps()\n            # manual tweak",
    )
    _write(migrations_dir, "0001_01_01_000000_create_widget_table.py", edited)
    assert cmd._confirm_clobber(*cmd._partition_migrations()) is False
    cmd.confirm.assert_called_once()


def test_confirm_clobber_hand_edited_prompts_and_respects_yes(migrations_dir):
    cmd = _make_command()
    cmd.confirm = MagicMock(return_value=True)
    edited = _GENERATED_CREATE.replace(
        "            table.timestamps()",
        "            table.timestamps()\n            # manual tweak",
    )
    _write(migrations_dir, "0001_01_01_000000_create_widget_table.py", edited)
    assert cmd._confirm_clobber(*cmd._partition_migrations()) is True
    cmd.confirm.assert_called_once()


def test_force_skips_prompt_even_when_hand_edited(migrations_dir):
    cmd = _make_command({"force": True})
    cmd.confirm = MagicMock(side_effect=AssertionError("--force must not prompt"))
    edited = _GENERATED_CREATE.replace(
        "            table.timestamps()",
        "            table.timestamps()\n            # manual tweak",
    )
    _write(migrations_dir, "0001_01_01_000000_create_widget_table.py", edited)
    assert cmd._confirm_clobber(*cmd._partition_migrations()) is True


def test_dry_run_skips_prompt_even_when_hand_edited(migrations_dir):
    cmd = _make_command({"dry_run": True})
    cmd.confirm = MagicMock(side_effect=AssertionError("--dry_run must not prompt"))
    edited = _GENERATED_CREATE.replace(
        "            table.timestamps()",
        "            table.timestamps()\n            # manual tweak",
    )
    _write(migrations_dir, "0001_01_01_000000_create_widget_table.py", edited)
    assert cmd._confirm_clobber(*cmd._partition_migrations()) is True


# --- the purge is honest: preserved files survive the atomic replace ----------


def test_model_less_file_survives_and_is_announced(migrations_dir):
    cmd = _make_command({"force": True})
    doomed_file = _write(
        migrations_dir, "0001_01_01_000000_create_widget_table.py", _GENERATED_CREATE
    )
    kept = _write(
        migrations_dir, "9982_01_01_000000_create_price_views.py", _MODEL_LESS_FILE
    )
    cmd.info = MagicMock()
    cmd.warning = MagicMock()

    doomed, preserved = cmd._partition_migrations()
    assert cmd._confirm_clobber(doomed, preserved) is True

    # Both lists are printed — the sweep is never silent about what it keeps.
    announced = " ".join(str(c.args[0]) for c in cmd.info.call_args_list)
    assert "9982_01_01_000000_create_price_views.py" in announced
    assert "MODEL_LESS" in announced

    cmd._replace_model_migrations_atomically(doomed, [])
    assert kept.exists()
    assert not doomed_file.exists()


# --- _print_summary -----------------------------------------------------------


def test_summary_reports_full_tally():
    cmd = _make_command()
    cmd.success = MagicMock()
    cmd.warning = MagicMock()
    cmd._print_summary(2, 1, 5, 0, dry_run=False)
    msg = cmd.success.call_args.args[0]
    assert "Created 2 new" in msg and "1 updated" in msg and "5 unchanged" in msg
    cmd.warning.assert_not_called()


def test_summary_dry_run_uses_would_create():
    cmd = _make_command()
    cmd.success = MagicMock()
    cmd._print_summary(1, 0, 0, 0, dry_run=True)
    assert cmd.success.call_args.args[0].startswith("Would create")


def test_summary_surfaces_errors():
    cmd = _make_command()
    cmd.success = MagicMock()
    cmd.warning = MagicMock()
    cmd._print_summary(0, 0, 3, 2, dry_run=False)
    cmd.warning.assert_called_once()
    assert "2 model(s)" in cmd.warning.call_args.args[0]


def test_overwrite_prepares_before_replacing_files():
    cmd = _make_command({"overwrite": True, "force": True})
    model = {
        "name": "Widget",
        "table": "widget",
        "has_fields_method": True,
    }
    cmd.discoverer.discover_models = MagicMock(return_value=[model])
    cmd.discoverer.resolve_dependency_order = MagicMock(return_value=[model])
    cmd.generator.generate_create_migration = MagicMock(
        return_value="class MigrationFile:\n    pass\n"
    )
    cmd._confirm_clobber = MagicMock(return_value=True)
    cmd._replace_model_migrations_atomically = MagicMock(return_value=1)

    result = cmd.handle()

    assert result is None
    prepared = cmd._replace_model_migrations_atomically.call_args.args[1]
    assert prepared == [(model, 0, "class MigrationFile:\n    pass\n")]


def test_overwrite_bad_generated_syntax_changes_nothing():
    cmd = _make_command({"overwrite": True, "force": True})
    model = {
        "name": "Widget",
        "table": "widget",
        "has_fields_method": True,
    }
    cmd.discoverer.discover_models = MagicMock(return_value=[model])
    cmd.discoverer.resolve_dependency_order = MagicMock(return_value=[model])
    cmd.generator.generate_create_migration = MagicMock(return_value="not python :")
    cmd._confirm_clobber = MagicMock(return_value=True)
    cmd._replace_model_migrations_atomically = MagicMock()

    assert cmd.handle() == 1
    cmd._confirm_clobber.assert_not_called()
    cmd._replace_model_migrations_atomically.assert_not_called()


def test_sql_style_is_rejected_before_discovery():
    cmd = _make_command({"style": "sql"})
    cmd.discoverer.discover_models = MagicMock()

    assert cmd.handle() == 2
    cmd.discoverer.discover_models.assert_not_called()


def test_finalize_counter_accounts_for_preserved_high_sequence(migrations_dir):
    cmd = _make_command()
    cmd.generator.migrations_dir = migrations_dir
    cmd.generator.counter_file = migrations_dir / ".migration_counter"
    _write(
        migrations_dir,
        "9984_01_01_000000_framework_data_migration.py",
        "class FrameworkDataMigration:\n    pass\n",
    )

    cmd.generator.reset_counter()
    first = cmd.generator.create_migration_file(
        "create_widget_table", "class WidgetMigration:\n    pass\n"
    )
    cmd.generator.finalize_counter()

    assert first.name.startswith("0001_")
    assert cmd.generator.counter_file.read_text() == "9984"
    following = cmd.generator.create_migration_file(
        "add_name_to_widget_table", "class AddName:\n    pass\n"
    )
    assert following.name.startswith("9985_")
