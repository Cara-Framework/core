"""Regression tests for the migration tracker / runner.

These pin the data-loss and concurrency bugs uncovered in the audit:
- ``ensure_migrations_table`` previously DROPPED the existing tracker
  whenever ``_table_has_correct_structure`` returned False — which it
  does on any exception, including transient connection blips. A
  single startup hiccup could wipe migration history.
- The CREATE TABLE call lacked ``IF NOT EXISTS``, so two workers
  bootstrapping in parallel could race and crash the loser.
"""

from unittest.mock import MagicMock

import pytest

from cara.eloquent.migrations import MigrationTracker
from cara.exceptions import ORMException


def _fake_db_manager(driver="postgres", queries_made=None, query_handler=None):
    """Construct a stand-in for the DB manager that records SQL.

    ``query_handler`` lets a test inject side effects per query
    string (e.g. raise on the structure probe, succeed on create).
    """
    queries_made = queries_made if queries_made is not None else []

    def default_query(sql, *args, **kwargs):
        queries_made.append(sql.strip())
        return []

    conn = MagicMock()
    if query_handler is not None:
        conn.query.side_effect = query_handler
    else:
        conn.query.side_effect = default_query
    conn.close_connection = MagicMock()

    manager = MagicMock()
    manager.create_connection_instance = MagicMock(return_value=conn)
    manager.get_connection_info = MagicMock(return_value={"driver": driver})
    return manager, conn, queries_made


# ── ensure_migrations_table: must not drop existing data ─────────────


def test_ensure_table_does_not_drop_existing_table():
    """A real existing table with a valid structure must be left alone."""
    queries = []

    def handler(sql, *a, **k):
        queries.append(sql.strip())
        # First call is the structure probe; succeed → table is healthy.
        return [{"id": 1, "migration": "x", "batch": 1}]

    manager, _, _ = _fake_db_manager(query_handler=handler)
    tracker = MigrationTracker(manager)
    tracker.ensure_migrations_table()

    assert not any("DROP TABLE" in q.upper() for q in queries), (
        f"ensure should not DROP when table is valid; queries={queries}"
    )
    assert any("CREATE TABLE IF NOT EXISTS" in q.upper() for q in queries)


def test_ensure_table_raises_on_structure_mismatch_instead_of_dropping():
    """An existing table with a broken structure must surface as an
    error, never as a silent DROP — the audit history is too valuable
    to delete on a misconfiguration. The old behavior dropped + recreated.
    """
    call_count = {"n": 0}
    queries = []

    def handler(sql, *a, **k):
        queries.append(sql.strip())
        call_count["n"] += 1
        upper = sql.upper()
        if "CREATE TABLE" in upper:
            return []
        # _table_exists probe (SELECT 1 FROM ... LIMIT 1) → table exists
        if "SELECT 1 FROM" in upper:
            return []
        # _table_has_correct_structure probe (SELECT id, migration, batch ...)
        # → fails because the table is misshapen
        if upper.startswith("SELECT ID, MIGRATION, BATCH"):
            raise RuntimeError("column does not exist")
        return []

    manager, _, _ = _fake_db_manager(query_handler=handler)
    tracker = MigrationTracker(manager)
    # The tracker raises the framework's ORM-domain exception (ORMException),
    # not a bare RuntimeError — a schema mismatch on the migration table is an
    # ORM concern and callers catch the domain type. Assert the correct type.
    with pytest.raises(ORMException, match="unexpected schema"):
        tracker.ensure_migrations_table()

    assert not any("DROP TABLE" in q.upper() for q in queries), (
        "must not destroy migration history on structure mismatch"
    )


def test_ensure_table_creates_with_if_not_exists_clause():
    """When the table is missing, the CREATE TABLE must include
    ``IF NOT EXISTS`` so two workers racing to bootstrap do not
    crash each other."""
    queries = []

    def handler(sql, *a, **k):
        queries.append(sql.strip())
        return []

    manager, _, _ = _fake_db_manager(query_handler=handler)
    tracker = MigrationTracker(manager)
    tracker.ensure_migrations_table()

    create_statements = [q for q in queries if "CREATE TABLE" in q.upper()]
    assert create_statements, f"expected CREATE TABLE; queries={queries}"
    assert all("IF NOT EXISTS" in q.upper() for q in create_statements), (
        f"CREATE TABLE must use IF NOT EXISTS; got: {create_statements}"
    )


def test_ensure_table_no_drop_on_fresh_install():
    """Fresh DB: both probes fail. ensure must NOT issue a DROP
    (there's nothing to drop, and emitting one logs a confusing
    error in the previous implementation)."""
    queries = []

    def handler(sql, *a, **k):
        queries.append(sql.strip())
        return []

    manager, _, _ = _fake_db_manager(query_handler=handler)
    MigrationTracker(manager).ensure_migrations_table()

    assert not any("DROP TABLE" in q.upper() for q in queries), (
        f"fresh install must not DROP; queries={queries}"
    )


def test_create_table_sql_is_sqlite_compatible_when_driver_is_sqlite():
    queries = []

    def handler(sql, *a, **k):
        queries.append(sql.strip())
        return []

    manager, _, _ = _fake_db_manager(driver="sqlite", query_handler=handler)
    MigrationTracker(manager).ensure_migrations_table()

    create_stmt = next(q for q in queries if "CREATE TABLE" in q.upper())
    assert "IF NOT EXISTS" in create_stmt.upper()
    assert "AUTOINCREMENT" in create_stmt.upper(), (
        "SQLite driver should use AUTOINCREMENT, not SERIAL"
    )


def test_tracker_does_not_close_executor_owned_transaction_connection():
    manager, connection, _queries = _fake_db_manager()
    connection.transaction_level = 1

    MigrationTracker(manager).record_migration("0112_demo", 6, "a" * 64)

    connection.query.assert_called_once()
    connection.close_connection.assert_not_called()


def test_mssql_tracker_uses_native_datetime_and_top_syntax():
    manager, _connection, queries = _fake_db_manager(driver="mssql")
    MigrationTracker(manager).ensure_migrations_table()

    sql = "\n".join(queries).upper()
    assert "DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()" in sql
    assert "SELECT TOP 1 ID, MIGRATION, BATCH" in sql
    assert " LIMIT 1" not in sql
    assert "ADD COLUMN CHECKSUM" not in sql


def test_migration_lock_rejects_invalid_timeout():
    manager, _connection, _queries = _fake_db_manager(driver="sqlite")
    tracker = MigrationTracker(manager)

    with pytest.raises(ORMException, match="positive integer"):
        with tracker.migration_lock(timeout_seconds=0):
            pass


def test_replace_migration_history_is_atomic():
    manager, connection, queries = _fake_db_manager()
    tracker = MigrationTracker(manager)

    tracker.replace_migration_history(
        [("0001_create_users", "a" * 64), ("0002_create_jobs", "b" * 64)]
    )

    connection.begin.assert_called_once()
    connection.commit.assert_called_once()
    connection.rollback.assert_not_called()
    assert queries[0] == "DELETE FROM migrations"
    assert sum(query.startswith("INSERT INTO migrations") for query in queries) == 2


def test_replace_migration_history_rolls_back_on_failure():
    calls = {"count": 0}

    def handler(sql, *_args, **_kwargs):
        calls["count"] += 1
        if calls["count"] == 2:
            raise RuntimeError("write failed")
        return []

    manager, connection, _queries = _fake_db_manager(query_handler=handler)
    tracker = MigrationTracker(manager)

    with pytest.raises(RuntimeError, match="write failed"):
        tracker.replace_migration_history([("0001_create_users", "a" * 64)])

    connection.rollback.assert_called_once()
    connection.commit.assert_not_called()
