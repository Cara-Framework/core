"""Regression tests for MigrationExecutor transaction wrapping.

Background
~~~~~~~~~~
The companion test file ``test_migration_tracker.py`` pins the
``CREATE TABLE IF NOT EXISTS`` + structure-preservation bugs in the
tracker. This file pins the EXECUTOR-side contract that grew alongside
those fixes:

  * Each transactional migration runs inside its own
    ``db_manager.transaction()`` context. A crash between ``up()`` and
    ``record_migration`` must roll back the schema change AND skip the
    tracker write, so the next run re-applies cleanly instead of
    leaving a half-applied + unrecorded migration.
  * ``transactional = False`` (class-level opt-out) must SKIP the
    wrapping. ``CREATE INDEX CONCURRENTLY`` and a handful of other
    Postgres DDL ops cannot run inside a transaction — wrapping them
    would crash the migration.
  * Rollback path mirrors the up-path: tx wrapping when
    ``transactional`` (default True), bare execution when opted out.

These tests use mocks because the framework target is unit-level
behavior — the SQL semantics of the wrapped block are postgres's
concern, not the executor's. We assert the EXECUTOR's CALL ORDER and
LIFECYCLE, which is the bit the fix actually changed.
"""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest

from cara.eloquent.migrations.MigrationExecutor import MigrationExecutor


class _NoOpMigration:
    """Minimal Migration stand-in. Records up/down calls."""

    transactional = True  # default
    up_calls: list[str] = []
    down_calls: list[str] = []

    def up(self):
        self.up_calls.append("up")

    def down(self):
        self.down_calls.append("down")


def _setup(transactional=True, up_raises=False, down_raises=False):
    """Build the moving parts MigrationExecutor needs.

    Returns ``(executor, calls, db_manager, tracker)`` where ``calls``
    is an in-order log of significant events for assertion. The
    transaction context manager logs ``begin`` / ``commit`` /
    ``rollback`` so test bodies can assert wrapping behavior.
    """
    calls: list[str] = []
    _trans = transactional  # rebind so the inner class can see it
    _up_raises = up_raises
    _down_raises = down_raises

    @contextmanager
    def fake_tx():
        calls.append("begin")
        try:
            yield None
        except BaseException:
            calls.append("rollback")
            raise
        calls.append("commit")

    db_manager = MagicMock(name="db_manager")
    db_manager.transaction.side_effect = lambda *_a, **_k: fake_tx()

    file_manager = MagicMock(name="file_manager")
    file_manager.get_migration_files.return_value = ["/fake/0001_a.py"]
    file_manager.get_migration_name_from_file.side_effect = lambda p: p.rsplit("/", 1)[
        -1
    ].rsplit(".", 1)[0]

    class _Mig:
        transactional = _trans

        def up(self):
            calls.append("up")
            if _up_raises:
                raise RuntimeError("up failed")

        def down(self):
            calls.append("down")
            if _down_raises:
                raise RuntimeError("down failed")

    file_manager.load_migration_class.return_value = _Mig

    tracker = MagicMock(name="tracker")
    tracker.ensure_migrations_table = MagicMock()
    tracker.get_ran_migrations.return_value = []
    # ``run_pending_migrations`` reads this to compute the next batch
    # number (last+1). Returning 0 here means "fresh DB" for the
    # up-path. The down-path tests need a non-zero return so
    # ``rollback_last_batch`` does not short-circuit on "Nothing to
    # rollback"; they override it explicitly.
    tracker.get_last_batch_number.return_value = 1
    tracker.record_migration.side_effect = lambda *a, **k: calls.append(f"record:{a[0]}")
    tracker.remove_migration.side_effect = lambda *a, **k: calls.append(f"remove:{a[0]}")
    tracker.get_migrations_by_batch.return_value = ["0001_a"]

    executor = MigrationExecutor(db_manager, file_manager, tracker)
    return executor, calls, db_manager, tracker


# ── Up path: transactional vs. opt-out ───────────────────────────────


def test_transactional_up_wraps_up_and_record_migration_together():
    """The whole ``up() + record_migration`` pair runs inside one
    transaction so an INSERT failure rolls back the DDL too.
    Pre-fix: ``record_migration`` ran AFTER commit, so a crash there
    left the schema modified but unrecorded."""
    executor, calls, _, _ = _setup(transactional=True)

    executor.run_pending_migrations()

    assert calls == ["begin", "up", "record:0001_a", "commit"]


def test_non_transactional_up_skips_transaction_wrapper():
    """``CREATE INDEX CONCURRENTLY`` and friends declare
    ``transactional = False``; the executor must run them OUTSIDE any
    transaction. Wrapping them would crash with
    ``CREATE INDEX CONCURRENTLY cannot run inside a transaction block``."""
    executor, calls, db_manager, _ = _setup(transactional=False)

    executor.run_pending_migrations()

    assert calls == ["up", "record:0001_a"]
    db_manager.transaction.assert_not_called()


def test_failed_transactional_up_rolls_back_and_does_not_record():
    """When ``up()`` raises, the transaction context manager triggers
    rollback, and ``record_migration`` MUST NOT run. The next sweep
    will then re-apply the migration cleanly."""
    executor, calls, _, tracker = _setup(transactional=True, up_raises=True)

    with pytest.raises(RuntimeError, match="up failed"):
        executor.run_pending_migrations()

    assert calls == ["begin", "up", "rollback"]
    tracker.record_migration.assert_not_called()


def test_failed_non_transactional_up_still_does_not_record():
    """No tx to roll back — but record_migration must still not fire,
    or the tracker would lie about migration state."""
    executor, calls, _, tracker = _setup(transactional=False, up_raises=True)

    with pytest.raises(RuntimeError, match="up failed"):
        executor.run_pending_migrations()

    assert calls == ["up"]
    tracker.record_migration.assert_not_called()


# ── Down path mirrors up path ────────────────────────────────────────


def test_transactional_down_wraps_down_and_remove_migration_together():
    executor, calls, _, _ = _setup(transactional=True)

    executor.rollback_last_batch()

    assert calls == ["begin", "down", "remove:0001_a", "commit"]


def test_non_transactional_down_skips_transaction_wrapper():
    executor, calls, db_manager, _ = _setup(transactional=False)

    executor.rollback_last_batch()

    assert calls == ["down", "remove:0001_a"]
    db_manager.transaction.assert_not_called()


def test_failed_transactional_down_rolls_back_and_does_not_remove():
    executor, calls, _, tracker = _setup(transactional=True, down_raises=True)

    with pytest.raises(RuntimeError, match="down failed"):
        executor.rollback_last_batch()

    assert calls == ["begin", "down", "rollback"]
    tracker.remove_migration.assert_not_called()


# ── Defensive default: unknown opt-out → assumes transactional ───────


def test_unloadable_migration_defaults_to_transactional_wrapping():
    """If the migration class fails to load while probing the
    ``transactional`` flag, the executor assumes transactional = True
    so we never accidentally LOSE tx safety on a migration we could
    have run wrapped. The subsequent real load (inside the tx) will
    raise the actual import error to the user.
    """
    calls: list[str] = []

    @contextmanager
    def fake_tx():
        calls.append("begin")
        try:
            yield None
        except BaseException:
            calls.append("rollback")
            raise
        calls.append("commit")

    db_manager = MagicMock()
    db_manager.transaction.side_effect = lambda *_a, **_k: fake_tx()

    file_manager = MagicMock()
    file_manager.get_migration_files.return_value = ["/fake/0001_a.py"]
    file_manager.get_migration_name_from_file.return_value = "0001_a"

    load_calls = {"n": 0}

    def loader(_path):
        load_calls["n"] += 1
        if load_calls["n"] == 1:
            # Probe call from _migration_is_transactional — fail.
            raise ImportError("module not found")

        # Subsequent call from _run_migration — succeed so the
        # transactional wrapping path is observable.
        class _Mig:
            transactional = True

            def up(self):
                calls.append("up")

        return _Mig

    file_manager.load_migration_class.side_effect = loader

    tracker = MagicMock()
    tracker.ensure_migrations_table = MagicMock()
    tracker.get_ran_migrations.return_value = []
    tracker.get_last_batch_number.return_value = 0
    tracker.record_migration.side_effect = lambda *a, **k: calls.append(f"record:{a[0]}")

    MigrationExecutor(db_manager, file_manager, tracker).run_pending_migrations()

    # Wrapper still ran — defensive default in action.
    assert calls == ["begin", "up", "record:0001_a", "commit"]


# ── Multi-migration sweep: each migration is its own transaction ─────


def test_each_pending_migration_runs_in_its_own_transaction():
    """A failure in migration N must NOT roll back migration N-1's
    already-committed DDL. The executor's per-migration tx wrapping
    is what makes this safe."""
    calls: list[str] = []

    @contextmanager
    def fake_tx():
        calls.append("begin")
        try:
            yield None
        except BaseException:
            calls.append("rollback")
            raise
        calls.append("commit")

    db_manager = MagicMock()
    db_manager.transaction.side_effect = lambda *_a, **_k: fake_tx()

    file_manager = MagicMock()
    file_manager.get_migration_files.return_value = [
        "/fake/0001_a.py",
        "/fake/0002_b.py",
        "/fake/0003_c.py",
    ]
    file_manager.get_migration_name_from_file.side_effect = lambda p: p.rsplit("/", 1)[
        -1
    ].rsplit(".", 1)[0]

    class _OK:
        transactional = True

        def up(self):
            calls.append("up-ok")

    class _Bad:
        transactional = True

        def up(self):
            calls.append("up-bad")
            raise RuntimeError("0002 fails")

    # ``_migration_is_transactional`` loads the class once to probe
    # the opt-out flag; ``_run_migration`` loads it again to actually
    # run it. So every file is loaded TWICE per sweep — keep the
    # mapping by file path instead of an iterator that runs out.
    class_by_file = {
        "/fake/0001_a.py": _OK,
        "/fake/0002_b.py": _Bad,
        "/fake/0003_c.py": _OK,
    }
    file_manager.load_migration_class.side_effect = lambda path: class_by_file[path]

    tracker = MagicMock()
    tracker.ensure_migrations_table = MagicMock()
    tracker.get_ran_migrations.return_value = []
    tracker.get_last_batch_number.return_value = 0
    tracker.record_migration.side_effect = lambda *a, **k: calls.append(f"record:{a[0]}")

    with pytest.raises(RuntimeError, match="0002 fails"):
        MigrationExecutor(db_manager, file_manager, tracker).run_pending_migrations()

    # Migration 1 committed; migration 2 rolled back; migration 3
    # never started.
    assert calls == [
        "begin",
        "up-ok",
        "record:0001_a",
        "commit",
        "begin",
        "up-bad",
        "rollback",
    ]


def test_run_pending_calls_ensure_migrations_table_before_any_work():
    """Bootstrap order: the tracker table MUST exist before
    ``get_ran_migrations`` reads from it. Pre-fix sweeps that ran
    against a virgin database crashed on the very first SELECT."""
    executor, _, _, tracker = _setup(transactional=True)
    executor.run_pending_migrations()

    # ensure_migrations_table must have been called.
    tracker.ensure_migrations_table.assert_called_once()
    # And it must precede the read — assert via call order on the
    # tracker mock.
    ensure_idx = next(
        i for i, c in enumerate(tracker.method_calls) if c[0] == "ensure_migrations_table"
    )
    read_idx = next(
        i for i, c in enumerate(tracker.method_calls) if c[0] == "get_ran_migrations"
    )
    assert ensure_idx < read_idx
