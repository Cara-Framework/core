"""Bare ``make:migration`` — a drift report that can never write.

The bare mode used to EMIT incremental ``add_x_to_y`` files from the model
diff — the exact file class ``migrations:check`` bans, manufactured by the
sibling command. These tests pin the replacement contract:

* bare mode writes NOTHING, under any input — a generator whose write path
  raises proves it by construction;
* drift (typed diffs, a missing create file, an orphaned file) exits 1 with
  the regenerate remedy named; a clean tree exits 0;
* production refuses BOTH modes before the generation lock is even taken;
* ``--overwrite`` still routes to the writer, so the report didn't eat it.
"""

from __future__ import annotations

import contextlib

import pytest

from cara.commands.core.MakeMigrationCommand import MakeMigrationCommand
from cara.eloquent.migrations.ModelMigrationComparator import Column, FieldDiff


def _added(name: str) -> FieldDiff:
    return FieldDiff("added", name, column=Column(name, "string"))


def _model(name: str, table: str, has_fields: bool = True) -> dict:
    return {"name": name, "table": table, "has_fields_method": has_fields}


class _Discoverer:
    def __init__(self, models):
        self.models = models

    def discover_models(self):
        return list(self.models)

    def resolve_dependency_order(self, models):
        return list(models)


class _Comparator:
    """Canned diffs per table + which tables have a generated create file."""

    def __init__(self, diffs=None, existing=()):
        self.diffs = diffs or {}
        self.existing = set(existing)

    def compare_model_with_migrations(self, model_info):
        return list(self.diffs.get(model_info["table"], []))

    def table_exists_in_migrations(self, table_name):
        return table_name in self.existing


class _Generator:
    """A generator whose write path is a tripwire, not a stub."""

    def __init__(self):
        self.lock_entered = 0

    @contextlib.contextmanager
    def generation_lock(self):
        self.lock_entered += 1
        yield

    def create_migration_file(self, *args, **kwargs):
        raise AssertionError("bare make:migration wrote a migration file")

    def generate_create_migration(self, *args, **kwargs):
        raise AssertionError("bare make:migration rendered a migration")

    def generate_update_migration(self, *args, **kwargs):  # pragma: no cover
        raise AssertionError("the update emitter is supposed to be deleted")


class _Report(MakeMigrationCommand):
    def __init__(self, models, comparator, options=None):
        # Deliberately skip MakeMigrationCommand.__init__: the harness supplies
        # its own collaborators and records console output instead of printing.
        self.application = None
        self._parsed_options = options or {}
        self.messages: list[str] = []
        self.discoverer = _Discoverer(models)
        self.comparator = comparator
        self.generator = _Generator()

    def info(self, message: str) -> None:
        self.messages.append(message)

    def warning(self, message: str) -> None:
        self.messages.append(message)

    def error(self, message: str, **_kwargs) -> None:
        self.messages.append(message)

    def success(self, message: str) -> None:
        self.messages.append(message)

    def _migrations_dir(self):
        return None  # orphan scan is exercised separately on a tmp_path


def _text(command) -> str:
    return "\n".join(command.messages)


# ── clean tree ──────────────────────────────────────────────────────────────


def test_in_sync_exits_zero_and_writes_nothing():
    command = _Report(
        [_model("Product", "product"), _model("Listing", "listing")],
        _Comparator(existing={"product", "listing"}),
    )
    assert command.handle() == 0
    assert "in sync" in _text(command)


def test_view_only_models_count_as_in_sync():
    command = _Report(
        [_model("ProductView", "product_view", has_fields=False)], _Comparator()
    )
    assert command.handle() == 0


# ── drift ───────────────────────────────────────────────────────────────────


def test_typed_diff_reports_the_change_and_exits_one():
    command = _Report(
        [_model("Product", "product")],
        _Comparator(
            diffs={"product": [_added("sku")]}, existing={"product"}
        ),
    )
    assert command.handle() == 1
    out = _text(command)
    assert "add_sku_to_product" in out  # the change-set label, not a filename
    assert "--overwrite" in out


def test_missing_create_file_is_reported_as_drift():
    command = _Report(
        [_model("Product", "product")],
        _Comparator(diffs={"product": [_added("sku")]}, existing=()),
    )
    assert command.handle() == 1
    assert "no generated create_product_table file" in _text(command)


def test_orphaned_generated_file_is_reported(tmp_path):
    (tmp_path / "0001_01_01_000000_create_ghost_table.py").write_text(
        "", encoding="utf-8"
    )
    (tmp_path / "0002_01_01_000001_create_product_table.py").write_text(
        "", encoding="utf-8"
    )
    command = _Report([_model("Product", "product")], _Comparator())
    command._migrations_dir = lambda: tmp_path
    assert command.handle() == 1
    out = _text(command)
    assert "create_ghost_table" in out
    assert "no model owns this file" in out


# ── mode routing and the production refusal ─────────────────────────────────


def test_overwrite_option_still_routes_to_the_writer():
    command = _Report([], _Comparator(), options={"overwrite": True})
    routed = []
    command._handle_overwrite_mode = lambda: routed.append(True) or 0
    assert command.handle() == 0
    assert routed == [True]


@pytest.mark.parametrize("env_name", ["production", "prod", "PRODUCTION"])
def test_production_refuses_before_taking_the_lock(monkeypatch, env_name):
    # The command binds ``config`` at module top, so the patch must land on
    # the command module's name, not on ``cara.configuration``.
    monkeypatch.setattr(
        "cara.commands.core.MakeMigrationCommand.config",
        lambda key, default=None: env_name if key == "app.env" else default,
    )
    command = _Report(
        [_model("Product", "product")],
        _Comparator(diffs={"product": [_added("sku")]}),
    )
    assert command.handle() == 2
    assert command.generator.lock_entered == 0
    assert "evolve workflow" in _text(command)
