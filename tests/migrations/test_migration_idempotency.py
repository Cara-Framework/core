"""Regression tests for ``Migration.migrate`` and ``Migration.rollback``
specific-name idempotency.

Background
----------
The ``run_pending_migrations`` bulk path consults
``tracker.get_ran_migrations`` and filters out anything already
applied — so ``python craft migrate`` is naturally idempotent.

The specific-name paths (``Migration.migrate(migration='foo')`` and
``Migration.rollback(migration='foo')``) used to skip that check and
run ``up()`` / ``down()`` unconditionally. Consequences:

* ``python craft migrate --m=create_widgets_table`` twice ran
  ``CREATE TABLE widgets`` twice; the second call crashed with
  ``relation "widgets" already exists`` and left a duplicate tracker
  row from the first INSERT (a brief window where the table existed
  and was already recorded).
* ``python craft migrate:rollback --m=create_widgets_table`` twice
  ran ``DROP TABLE widgets`` twice; the second crashed with
  ``table does not exist`` and masked the real state ("already
  rolled back").

These tests pin the new behavior: the specific-name paths consult the
tracker first and short-circuit when the migration is in the wrong
state for the requested direction.
"""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

from cara.eloquent.migrations import Migration

# ── Test plumbing ────────────────────────────────────────────────────


@contextmanager
def _patched_migration(ran_migrations, files):
    """Build a ``Migration`` instance with its components mocked out.

    ``ran_migrations`` is the list returned by the tracker.
    ``files`` is the list of migration file paths the file manager
    will return; the file manager strips ``.py`` from the basename
    to derive the migration name.
    """
    with patch.object(Migration, "__init__", lambda self, *a, **k: None):
        mig = Migration()
        mig.command_class = None
        mig.connection = "test"

        mig.tracker = MagicMock(name="tracker")
        mig.tracker.ensure_migrations_table = MagicMock()
        mig.tracker.get_ran_migrations.return_value = list(ran_migrations)
        mig.tracker.get_last_batch_number.return_value = len(ran_migrations)

        mig.file_manager = MagicMock(name="file_manager")
        mig.file_manager.get_migration_files.return_value = list(files)
        mig.file_manager.get_migration_name_from_file.side_effect = lambda p: p.rsplit(
            "/", 1
        )[-1].rsplit(".", 1)[0]

        mig.executor = MagicMock(name="executor")
        # Default: pretend everything is transactional. Tests that
        # care about the DB.transaction wrapper override this.
        mig.executor._migration_is_transactional.return_value = False
        mig.executor.run_pending_migrations = MagicMock()
        mig.executor.rollback_last_batch = MagicMock()
        mig.executor._run_migration = MagicMock()
        yield mig


# ── migrate: specific-name idempotency ───────────────────────────────


def test_migrate_specific_already_ran_short_circuits():
    """If the named migration is already in the tracker, ``up()``
    must not run again. Previously this path called ``up()``
    unconditionally — typically a ``CREATE TABLE`` whose second
    invocation crashed with ``already exists``."""
    with _patched_migration(
        ran_migrations=["0001_create_widgets_table"],
        files=["/m/0001_create_widgets_table.py"],
    ) as mig:
        mig.migrate(migration="0001_create_widgets_table")

        # No ``up()`` run, no duplicate record write.
        mig.executor._run_migration.assert_not_called()
        mig.tracker.record_migration.assert_not_called()


def test_migrate_specific_unran_runs_up_and_records():
    """The happy path: not in tracker → run + record exactly once."""
    with _patched_migration(
        ran_migrations=[],
        files=["/m/0001_create_widgets_table.py"],
    ) as mig:
        mig.migrate(migration="0001_create_widgets_table")

        mig.executor._run_migration.assert_called_once_with(
            "/m/0001_create_widgets_table.py", "up"
        )
        mig.tracker.record_migration.assert_called_once()


def test_migrate_all_delegates_to_run_pending_migrations():
    """Bulk path is unchanged — still delegates to the executor's
    natively-idempotent ``run_pending_migrations``."""
    with _patched_migration(ran_migrations=[], files=[]) as mig:
        mig.migrate(migration="all")
        mig.executor.run_pending_migrations.assert_called_once()


# ── rollback: specific-name idempotency ──────────────────────────────


def test_rollback_specific_not_in_tracker_short_circuits():
    """If the named migration is NOT in the tracker (never ran, or
    already rolled back), ``down()`` must not run. Previously this
    path ran ``down()`` regardless — typically a ``DROP TABLE`` whose
    second invocation crashed with ``does not exist``."""
    with _patched_migration(
        ran_migrations=[],
        files=["/m/0001_create_widgets_table.py"],
    ) as mig:
        mig.rollback(migration="0001_create_widgets_table")

        mig.executor._run_migration.assert_not_called()
        mig.tracker.remove_migration.assert_not_called()


def test_rollback_specific_in_tracker_runs_down_and_removes():
    """The happy path: in tracker → run down + remove exactly once."""
    with _patched_migration(
        ran_migrations=["0001_create_widgets_table"],
        files=["/m/0001_create_widgets_table.py"],
    ) as mig:
        mig.rollback(migration="0001_create_widgets_table")

        mig.executor._run_migration.assert_called_once_with(
            "/m/0001_create_widgets_table.py", "down"
        )
        mig.tracker.remove_migration.assert_called_once_with("0001_create_widgets_table")


def test_rollback_all_delegates_to_rollback_last_batch():
    """Bulk path unchanged."""
    with _patched_migration(ran_migrations=[], files=[]) as mig:
        mig.rollback(migration="all")
        mig.executor.rollback_last_batch.assert_called_once()


# ── Property: running migrate then rollback in a loop is idempotent ──


def test_repeated_migrate_then_rollback_loop_is_idempotent():
    """Two cycles of (migrate, migrate, rollback, rollback) must
    invoke ``up()`` and ``down()`` exactly once each. Any drift here
    means an operator's ``Ctrl+R``-rerun of a failed migration could
    desync the schema."""
    ran: list[str] = []
    name = "0042_add_widget_color"
    file_path = "/m/0042_add_widget_color.py"

    def make():
        return _patched_migration(ran_migrations=list(ran), files=[file_path])

    # First migrate: runs up.
    with make() as mig:
        mig.migrate(migration=name)
        if mig.executor._run_migration.called:
            ran.append(name)
        assert mig.executor._run_migration.call_count == 1

    # Second migrate: no-op.
    with make() as mig:
        mig.migrate(migration=name)
        assert mig.executor._run_migration.call_count == 0

    # First rollback: runs down.
    with make() as mig:
        mig.rollback(migration=name)
        if mig.executor._run_migration.called:
            ran.remove(name)
        assert mig.executor._run_migration.call_count == 1

    # Second rollback: no-op.
    with make() as mig:
        mig.rollback(migration=name)
        assert mig.executor._run_migration.call_count == 0
