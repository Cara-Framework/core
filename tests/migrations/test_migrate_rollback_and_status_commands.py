"""Regression tests for ``migrate:rollback`` and ``migrate:status``.

Three defects, each of which told the operator something the database did
not agree with:

  * ``migrate:rollback --show`` printed "SQL Preview Mode - No changes will be
    made" and then called ``Migration.rollback`` — the REAL rollback. Every
    ``down()`` ran and the ledger rows were deleted under a preview banner.
  * ``migrate:rollback --step N`` parsed N, echoed it ("Rolling back last N
    batch(es)") and dropped it: every value rolled back exactly one batch.
  * ``migrate:status`` read each batch through ``tracker.get_migration_details``,
    a method ``MigrationTracker`` has never had. The ``AttributeError`` was
    swallowed into a fabricated "1", so every applied migration was reported
    as batch 1; and a status that could not be read at all exited 0.

The tests are mock-driven (no live database): ``_Ledger`` stands in for
``Migration`` with the same read surface the commands use, and records every
``rollback`` call so "nothing was rolled back" is an assertion, not a hope.
"""

from __future__ import annotations

import importlib
from types import SimpleNamespace
from unittest.mock import MagicMock

from cara.commands.core.MigrateRollbackCommand import MigrateRollbackCommand
from cara.commands.core.MigrateStatusCommand import MigrateStatusCommand
from cara.eloquent.migrations.MigrationTracker import MigrationTracker
from cara.eloquent.schema.SchemaQueryExecutor import SchemaQueryExecutor
from tests.commands._fixtures import make_command


class _DownMigration:
    """A migration whose ``down()`` compiles through a DRY executor.

    Mirrors the real base ``Migration``: a non-dry build here means the
    preview was about to open a connection, so it fails the test loudly.
    """

    def __init__(self, **kwargs):
        assert kwargs.get("dry") is True, "a preview must build the migration dry"
        self.schema = MagicMock()
        self.schema.query_executor = SchemaQueryExecutor(
            connection_manager=MagicMock(), dry=True
        )

    def up(self):
        raise AssertionError("a rollback preview must never run up()")

    def down(self):
        self.schema.query_executor.execute_query("DROP TABLE bar")


class _Ledger:
    """Stands in for ``Migration``: batches of applied names + a rollback log."""

    def __init__(self, batches: dict[int, list[str]]):
        self.batches = {batch: list(names) for batch, names in batches.items()}
        self.rollback_calls: list[str] = []
        # Only methods the real tracker has — see the surface test below.
        self.tracker = SimpleNamespace(
            get_last_batch_number=self.get_last_batch_number,
            get_migrations_by_batch=lambda batch: list(self.batches.get(batch, [])),
            get_ran_migration_records=self.get_ran_migration_records,
        )
        self.file_manager = MagicMock()
        self.file_manager.get_migration_files.return_value = [
            f"/m/{name}.py" for names in self.batches.values() for name in names
        ]
        self.file_manager.get_migration_name_from_file.side_effect = lambda path: (
            path.rsplit("/", 1)[-1][: -len(".py")]
        )
        self.file_manager.load_migration_class.return_value = _DownMigration

    def create_table_if_not_exists(self) -> None:
        pass

    def get_last_batch_number(self) -> int:
        return max(self.batches, default=0)

    def get_ran_migration_records(self) -> list[dict]:
        """The tracker's one-query read: every applied row with its batch."""
        return [
            {"migration": name, "checksum": None, "batch": batch}
            for batch in sorted(self.batches)
            for name in self.batches[batch]
        ]

    def get_rollback_migrations(self) -> list[str]:
        return list(self.batches.get(self.get_last_batch_number(), []))

    def rollback(self, migration: str = "all", output: bool = False) -> None:
        self.rollback_calls.append(migration)
        if migration == "all":
            self.batches.pop(self.get_last_batch_number(), None)


def _rollback_command(monkeypatch, ledger: _Ledger, **options) -> MigrateRollbackCommand:
    """A ``migrate:rollback`` whose ``Migration`` is ``ledger``."""
    module = importlib.import_module("cara.eloquent.migrations.Migration")
    monkeypatch.setattr(module, "Migration", lambda **kwargs: ledger)
    # No ``call`` on the stand-in application, so ``handle`` runs directly;
    # ``make("DB")`` is all the preview's facade guard asks of it.
    application = SimpleNamespace(make=lambda name: MagicMock())
    return make_command(
        MigrateRollbackCommand,
        options={
            "migration": "all",
            "connection": "default",
            "force": False,
            "show": False,
            "schema": None,
            "directory": "/m",
            "step": 1,
            **options,
        },
        application=application,
        _is_production=lambda: False,
    )


def _printed(command) -> list[str]:
    return [
        str(call.args[0]) for call in command.console.print.call_args_list if call.args
    ]


# ── migrate:rollback --show ──────────────────────────────────────────────


def test_show_previews_the_down_sql_and_rolls_nothing_back(monkeypatch):
    ledger = _Ledger({1: ["0001_a"], 2: ["0002_b"]})
    cmd = _rollback_command(monkeypatch, ledger, show=True)

    assert cmd.handle() is None

    assert ledger.rollback_calls == [], "--show ran the real rollback"
    assert ledger.batches == {1: ["0001_a"], 2: ["0002_b"]}
    printed = _printed(cmd)
    assert "DROP TABLE bar;" in printed
    assert any("No actual changes were made" in line for line in printed)
    # Only the newest batch is previewed, exactly what a real run would undo.
    ledger.file_manager.load_migration_class.assert_called_once_with("/m/0002_b.py")


def test_show_with_step_previews_every_batch_it_would_undo(monkeypatch):
    # Batch 2 was rolled back earlier and left a gap: 4 and 3 are "the last two".
    ledger = _Ledger({1: ["0001_a"], 3: ["0003_c"], 4: ["0004_d", "0004_e"]})
    cmd = _rollback_command(monkeypatch, ledger, show=True, step=2)

    cmd.handle()

    assert ledger.rollback_calls == []
    loaded = [
        call.args[0] for call in ledger.file_manager.load_migration_class.call_args_list
    ]
    assert loaded == ["/m/0004_d.py", "/m/0004_e.py", "/m/0003_c.py"]


def test_show_reports_a_migration_whose_file_is_gone(monkeypatch):
    ledger = _Ledger({1: ["0001_a"]})
    ledger.file_manager.get_migration_files.return_value = []
    cmd = _rollback_command(monkeypatch, ledger, show=True)

    cmd.handle()

    assert ledger.rollback_calls == []
    assert any("missing from disk" in line for line in _printed(cmd))
    ledger.file_manager.load_migration_class.assert_not_called()


# ── migrate:rollback --step ──────────────────────────────────────────────


def test_default_step_rolls_back_one_batch(monkeypatch):
    ledger = _Ledger({1: ["0001_a"], 2: ["0002_b"]})
    cmd = _rollback_command(monkeypatch, ledger)

    cmd.handle()

    assert ledger.rollback_calls == ["all"]
    assert ledger.batches == {1: ["0001_a"]}


def test_step_rolls_back_that_many_batches(monkeypatch):
    ledger = _Ledger({1: ["0001_a"], 2: ["0002_b"], 3: ["0003_c"]})
    cmd = _rollback_command(monkeypatch, ledger, step=2)

    cmd.handle()

    assert ledger.rollback_calls == ["all", "all"]
    assert ledger.batches == {1: ["0001_a"]}


def test_step_stops_when_the_ledger_runs_out(monkeypatch):
    ledger = _Ledger({1: ["0001_a"], 2: ["0002_b"]})
    cmd = _rollback_command(monkeypatch, ledger, step=5)

    cmd.handle()

    assert ledger.rollback_calls == ["all", "all"]
    assert ledger.batches == {}


def test_a_non_positive_step_is_refused_before_anything_runs(monkeypatch):
    ledger = _Ledger({1: ["0001_a"]})
    cmd = _rollback_command(monkeypatch, ledger, step=-1)

    assert cmd.handle() == 1
    assert ledger.rollback_calls == []


def test_a_named_migration_rolls_back_only_that_migration(monkeypatch):
    ledger = _Ledger({1: ["0001_a"], 2: ["0002_b"]})
    cmd = _rollback_command(monkeypatch, ledger, migration="0001_a", step=3)

    cmd.handle()

    assert ledger.rollback_calls == ["0001_a"]


# ── migrate:status ───────────────────────────────────────────────────────


def _status_row(command, migration: str) -> list[str]:
    line = next(line for line in _printed(command) if migration in line)
    return line.replace("[#fff]", "").replace("[/#fff]", "").split()


def test_status_reports_each_migration_under_its_real_batch():
    ledger = _Ledger({1: ["0001_a"], 3: ["0003_c"]})
    cmd = make_command(MigrateStatusCommand)

    cmd._show_migration_status(
        ["/m/0003_c.py", "/m/0001_a.py", "/m/0004_d.py"],
        ["0001_a", "0003_c"],
        ledger,
    )

    assert _status_row(cmd, "0001_a")[-1] == "1"
    assert _status_row(cmd, "0003_c")[-1] == "3"  # was a fabricated "1"
    assert _status_row(cmd, "0004_d")[-1] == "-"
    assert any("Ran: 2 | Pending: 1" in line for line in _printed(cmd))


def test_status_asks_the_tracker_only_for_what_the_tracker_has():
    """The stand-in tracker above is honest only while this holds."""
    for name in (
        "get_last_batch_number",
        "get_migrations_by_batch",
        "get_ran_migration_records",
    ):
        assert callable(getattr(MigrationTracker, name, None)), name


def test_status_exits_non_zero_when_the_ledger_cannot_be_read():
    def _unreachable():
        raise RuntimeError("connection refused")

    cmd = make_command(MigrateStatusCommand, _build_migration=_unreachable)

    assert cmd.handle() == 1
