"""``migrate --show`` must not write, and a refused ``migrate:reset`` must fail.

* ``migrate --show`` printed "DRY RUN (no changes will be made)" and then
  called ``create_table_if_not_exists`` — which CREATEs the migrations
  ledger and upgrades an older one (checksum column, unique index). A
  preview against a database without a ledger now reads it as empty.
* ``migrate:reset`` refused to run in production and exited 0, so
  ``migrate:reset && migrate`` carried on as if the reset had happened.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from cara.commands.core.MigrateCommand import MigrateCommand
from cara.commands.core.MigrateResetCommand import MigrateResetCommand

from ._fixtures import make_command


def _preview(monkeypatch, *, ledger_exists: bool):
    command = make_command(MigrateCommand, {"show": True, "force": True})
    manager = MagicMock()
    manager.file_manager.get_migration_files.return_value = [
        "/m/0002_b.py",
        "/m/0001_a.py",
    ]
    manager.get_ran_migrations.return_value = []
    manager.get_unran_migrations.return_value = ["/m/0001_a.py", "/m/0002_b.py"]
    previewed: list[list[str]] = []
    monkeypatch.setattr(command, "_is_production", lambda: False)
    monkeypatch.setattr(command, "_build_migration", lambda: manager)
    monkeypatch.setattr(command, "_ledger_exists", lambda _manager: ledger_exists)
    monkeypatch.setattr(command, "_validate_applied_migrations", lambda _manager: None)
    monkeypatch.setattr(
        command, "_show_sql_preview", lambda _manager, pending: previewed.append(pending)
    )
    # ``handle`` binds the lazy ``Migration`` import; keep it off the DB extra.
    monkeypatch.setitem(
        __import__("sys").modules,
        "cara.eloquent.migrations",
        MagicMock(Migration=MagicMock()),
    )
    result = command.handle()
    return result, manager, previewed


def test_show_never_creates_or_upgrades_the_ledger(monkeypatch) -> None:
    for ledger_exists in (False, True):
        _result, manager, _previewed = _preview(monkeypatch, ledger_exists=ledger_exists)
        manager.create_table_if_not_exists.assert_not_called()
        manager.migrate.assert_not_called()


def test_show_without_a_ledger_previews_every_file_in_order(monkeypatch) -> None:
    result, manager, previewed = _preview(monkeypatch, ledger_exists=False)
    assert result is None
    assert previewed == [["/m/0001_a.py", "/m/0002_b.py"]]
    # The ledger was never read either: there is none to read.
    manager.get_ran_migrations.assert_not_called()
    manager.get_unran_migrations.assert_not_called()


def test_show_with_a_ledger_previews_what_it_has_not_applied(monkeypatch) -> None:
    _result, manager, previewed = _preview(monkeypatch, ledger_exists=True)
    assert previewed == [["/m/0001_a.py", "/m/0002_b.py"]]
    manager.get_unran_migrations.assert_called_once()


def test_a_real_run_still_bootstraps_the_ledger(monkeypatch) -> None:
    command = make_command(MigrateCommand, {"show": False, "force": True})
    manager = MagicMock()
    manager.get_unran_migrations.return_value = []
    monkeypatch.setattr(command, "_is_production", lambda: False)
    monkeypatch.setattr(command, "_build_migration", lambda: manager)
    monkeypatch.setattr(command, "_validate_applied_migrations", lambda _manager: None)
    monkeypatch.setitem(
        __import__("sys").modules,
        "cara.eloquent.migrations",
        MagicMock(Migration=MagicMock()),
    )
    command.handle()
    manager.create_table_if_not_exists.assert_called_once()


def test_reset_refused_in_production_exits_non_zero(monkeypatch) -> None:
    command = make_command(MigrateResetCommand, {"confirm": True})
    executed: list[dict] = []
    monkeypatch.setattr(command, "_is_production", lambda: True)
    monkeypatch.setattr(command, "_execute_reset", executed.append)

    assert command.handle(force=True) == 1
    assert executed == []
