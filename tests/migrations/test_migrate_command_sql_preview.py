"""Regression tests for ``MigrateCommand._show_sql_preview``.

Background
~~~~~~~~~~
``MigrateCommand.handle`` calls ``self._show_sql_preview(...)`` whenever the
operator runs ``migrate --show`` (SQL preview, no execution). That method was
referenced but never defined — every ``--show`` invocation died with
``AttributeError: 'MigrateCommand' object has no attribute '_show_sql_preview'``.

The fix builds each pending migration in DRY mode and collects the SQL its
``up()`` *would* run, without ever opening a write connection. These tests pin:

  * ``_install_sql_recorder`` captures EVERY statement routed through a dry
    ``SchemaQueryExecutor`` (the executor itself retains only the last), and
    delegates to the real dry methods (which store + return, never execute).
  * ``_collect_migration_sql`` instantiates the migration in dry mode, runs
    ``up()``, and returns the ordered statements — with NO DB writes.

The tests are mock-driven (no live database): the dry executor's contract is
"store the SQL and return it instead of touching the pool", which we assert by
giving it a non-dry stand-in that would raise if it ever tried to execute.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from cara.commands.core.MigrateCommand import MigrateCommand
from cara.eloquent.schema.SchemaQueryExecutor import SchemaQueryExecutor


def _make_command() -> MigrateCommand:
    """Build a MigrateCommand without going through Typer wiring."""
    cmd = MigrateCommand(application=None)
    cmd.set_parsed_options({})
    # Silence Rich console output during the test run.
    cmd.console = MagicMock()
    return cmd


def test_install_sql_recorder_captures_every_statement_in_order():
    """A single CREATE TABLE migration emits the table DDL plus its
    index/constraint statements; the recorder must capture all of them,
    not just the last (which is all ``_sql`` retains)."""
    # A real dry executor: ``execute_query`` STORES sql and returns it,
    # never opening a connection (connection_manager unused in dry mode).
    executor = SchemaQueryExecutor(connection_manager=MagicMock(), dry=True)
    schema = MagicMock()
    schema.query_executor = executor

    sink: list[str] = []
    MigrateCommand._install_sql_recorder(schema, sink)

    executor.execute_query("CREATE TABLE foo (id BIGSERIAL)")
    executor.execute_query("CREATE INDEX idx_foo ON foo (id)")
    # A list of statements (some platforms compile multiple) is flattened.
    executor.execute_query(["ALTER TABLE foo ADD COLUMN a INT", "  "])
    executor.get_query_result("SELECT 1")

    assert sink == [
        "CREATE TABLE foo (id BIGSERIAL)",
        "CREATE INDEX idx_foo ON foo (id)",
        "ALTER TABLE foo ADD COLUMN a INT",
        "SELECT 1",
    ]
    # The connection manager was never asked to create a connection.
    executor.connection_manager.create_connection_instance.assert_not_called()


def test_collect_migration_sql_runs_up_in_dry_mode_and_collects_no_writes():
    """``_collect_migration_sql`` instantiates the migration in dry mode and
    returns the SQL ``up()`` would run — without any DB write."""
    cmd = _make_command()

    captured_dry_flag = {}

    class _FakeMigration:
        def __init__(self, **kwargs):
            captured_dry_flag["dry"] = kwargs.get("dry")
            # Mirror the real base ``Migration``: a dry Schema whose executor
            # collects (never runs) SQL. A non-dry executor here would try to
            # open a connection and fail the test loudly.
            self.schema = MagicMock()
            self.schema.query_executor = SchemaQueryExecutor(
                connection_manager=MagicMock(), dry=True
            )

        def up(self):
            self.schema.query_executor.execute_query("CREATE TABLE bar (id BIGSERIAL)")
            self.schema.query_executor.execute_query("CREATE INDEX ix_bar ON bar (id)")

    migration_manager = MagicMock()
    migration_manager.file_manager.load_migration_class.return_value = _FakeMigration

    statements = cmd._collect_migration_sql(
        migration_manager,
        "/fake/0001_create_bar_table.py",
        connection="default",
        directory="/fake/migrations",
        schema=None,
    )

    assert captured_dry_flag["dry"] is True
    assert statements == [
        "CREATE TABLE bar (id BIGSERIAL)",
        "CREATE INDEX ix_bar ON bar (id)",
    ]


def test_collect_migration_sql_records_db_facade_statements_without_executing():
    """Migrations that bypass ``self.schema`` and run raw SQL through the
    ``DB`` facade (``DB.statement(...)``) must have that SQL CAPTURED but NOT
    executed during a dry preview — the facade has no dry-run awareness."""
    from cara.eloquent.DatabaseManager import DatabaseManager
    from cara.facades import DB

    cmd = _make_command()

    db = DatabaseManager.get_instance()
    # Make the real ``statement`` blow up if it's ever actually called — the
    # guard must intercept it before that happens.
    original_statement = db.statement
    sentinel_statement = MagicMock(
        side_effect=AssertionError("DB.statement executed during dry preview!")
    )
    db.statement = sentinel_statement

    class _FacadeMigration:
        def __init__(self, **kwargs):
            self.schema = MagicMock()
            from cara.eloquent.schema.SchemaQueryExecutor import SchemaQueryExecutor

            self.schema.query_executor = SchemaQueryExecutor(
                connection_manager=MagicMock(), dry=True
            )

        def up(self):
            DB.statement("ALTER TABLE listing ADD COLUMN IF NOT EXISTS foo jsonb")
            DB.statement("UPDATE listing SET foo = '{}'::jsonb WHERE foo IS NULL")

    migration_manager = MagicMock()
    migration_manager.file_manager.load_migration_class.return_value = _FacadeMigration

    try:
        statements = cmd._collect_migration_sql(
            migration_manager,
            "/fake/0099_facade_migration.py",
            connection="default",
            directory="/fake/migrations",
            schema=None,
        )
    finally:
        db.statement = original_statement

    assert statements == [
        "ALTER TABLE listing ADD COLUMN IF NOT EXISTS foo jsonb",
        "UPDATE listing SET foo = '{}'::jsonb WHERE foo IS NULL",
    ]
    # The sentinel (exploding) statement method was never reached — the guard
    # intercepted every call — and the original was restored afterwards.
    sentinel_statement.assert_not_called()
    assert db.statement is original_statement


def test_collect_migration_sql_swallows_compile_errors_without_aborting():
    """A migration that explodes during ``up()`` must not crash the whole
    preview loop — the command reports it and moves on."""
    cmd = _make_command()

    class _BrokenMigration:
        def __init__(self, **kwargs):
            self.schema = MagicMock()
            self.schema.query_executor = SchemaQueryExecutor(
                connection_manager=MagicMock(), dry=True
            )

        def up(self):
            raise RuntimeError("boom")

    migration_manager = MagicMock()
    migration_manager.file_manager.load_migration_class.return_value = _BrokenMigration

    statements = cmd._collect_migration_sql(
        migration_manager,
        "/fake/0002_broken_table.py",
        connection="default",
        directory="/fake/migrations",
        schema=None,
    )

    # No statements collected, but no exception bubbled up either.
    assert statements == []


def test_show_sql_preview_prints_collected_sql_and_makes_no_writes():
    """End-to-end (mock DB): ``_show_sql_preview`` iterates pending
    migrations, prints each one's SQL, and never executes anything."""
    cmd = _make_command()
    cmd.set_parsed_options({"connection": "default", "show": True})
    cmd.console = MagicMock()

    class _FakeMigration:
        def __init__(self, **kwargs):
            self.schema = MagicMock()
            self.schema.query_executor = SchemaQueryExecutor(
                connection_manager=MagicMock(), dry=True
            )

        def up(self):
            self.schema.query_executor.execute_query("CREATE TABLE baz (id BIGSERIAL)")

    migration_manager = MagicMock()
    migration_manager.file_manager.load_migration_class.return_value = _FakeMigration
    migration_manager.file_manager.get_migration_name_from_file.side_effect = (
        lambda p: p.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    )

    pending = ["/fake/migrations/0001_create_baz_table.py"]

    # Must not raise (the original bug was an AttributeError here).
    cmd._show_sql_preview(migration_manager, pending)

    # The migrate manager was never asked to migrate / write.
    migration_manager.migrate.assert_not_called()
    # The compiled SQL reached the console.
    printed = " ".join(
        str(call.args[0]) for call in cmd.console.print.call_args_list if call.args
    )
    assert "CREATE TABLE baz" in printed
