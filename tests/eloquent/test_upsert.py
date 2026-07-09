"""Regression pins for ``upsert()`` — compile shape and E2E semantics.

Fixed defects being pinned:

* Heterogeneous rows silently took row 0's column list, misaligning
  values under the wrong columns → now a loud QueryException.
* The default update-column list came from an unordered set → SQL text
  churned run-to-run; now sorted/deterministic.
* ``EXCLUDED`` update identifiers were hardcoded double quotes instead
  of the grammar's own quoting.
* ``update=[]`` compiled an empty ``DO UPDATE SET`` (syntax error) →
  now compiles ``DO NOTHING`` (insert-if-missing).
* The return value claimed "affected rows" but was always
  ``len(values)`` → now the real count (RETURNING rows or rowcount).
"""

from __future__ import annotations

import pytest

from cara.eloquent import DatabaseManager
from cara.eloquent.connections import SQLiteConnection
from cara.eloquent.query import QueryBuilder
from cara.eloquent.query.grammars import SQLiteGrammar
from cara.exceptions import QueryException


@pytest.fixture(scope="module", autouse=True)
def _register_sqlite_connection():
    dm = DatabaseManager.get_instance()
    _saved_config = dm._database_config
    _saved_default = dm._default_connection
    _saved_connections = dm._connections
    dm.set_database_config(
        "test_upsert", {"test_upsert": {"driver": "sqlite", "database": ":memory:"}}
    )
    try:
        yield
    finally:
        dm._database_config = _saved_config
        dm._default_connection = _saved_default
        dm._connections = _saved_connections


def _qb(table: str = "receipt") -> QueryBuilder:
    return QueryBuilder(grammar=SQLiteGrammar, connection="test_upsert", table=table)


class TestUpsertCompile:
    def test_compiles_on_conflict_do_update(self):
        builder = _qb()
        builder.dry = True
        builder.upsert(
            [{"receipt_id": "123", "status": "processed"}],
            unique_by=["receipt_id"],
        )
        sql = builder.get_grammar().compile("upsert").to_sql()

        assert 'ON CONFLICT ("receipt_id") DO UPDATE SET' in sql
        assert '"status" = EXCLUDED."status"' in sql

    def test_default_update_list_is_deterministic(self):
        def compile_once():
            builder = _qb()
            builder.dry = True
            builder.upsert(
                [{"receipt_id": "1", "b": 1, "a": 2, "c": 3}],
                unique_by=["receipt_id"],
            )
            return builder.get_grammar().compile("upsert").to_sql()

        assert compile_once() == compile_once()
        assert (
            '"a" = EXCLUDED."a", "b" = EXCLUDED."b", "c" = EXCLUDED."c"'
            in compile_once()
        )

    def test_empty_update_list_compiles_do_nothing(self):
        builder = _qb()
        builder.dry = True
        builder.upsert(
            [{"receipt_id": "123", "status": "new"}],
            unique_by=["receipt_id"],
            update=[],
        )
        sql = builder.get_grammar().compile("upsert").to_sql()

        assert "DO NOTHING" in sql
        assert "DO UPDATE" not in sql

    def test_heterogeneous_rows_raise(self):
        builder = _qb()
        builder.dry = True

        with pytest.raises(QueryException, match="must share the same columns"):
            builder.upsert(
                [
                    {"receipt_id": "1", "status": "a"},
                    {"receipt_id": "2", "amount": 5},
                ],
                unique_by=["receipt_id"],
            )


class TestMySQLUpsertCompile:
    def _compile(self, update):
        from cara.eloquent.query.grammars import MySQLGrammar

        builder = QueryBuilder(
            grammar=MySQLGrammar, connection="test_upsert", table="receipt"
        )
        builder.dry = True
        builder.upsert(
            [{"receipt_id": "1", "status": "a"}],
            unique_by=["receipt_id"],
            update=update,
        )
        return builder.get_grammar().compile("upsert").to_sql()

    def test_compiles_on_duplicate_key_update(self):
        sql = self._compile(update=["status"])

        assert "ON DUPLICATE KEY UPDATE" in sql
        assert "`status` = VALUES(`status`)" in sql

    def test_empty_update_compiles_insert_ignore(self):
        sql = self._compile(update=[])

        assert sql.startswith("INSERT IGNORE INTO")
        assert "ON DUPLICATE KEY" not in sql


class TestUpsertEndToEnd:
    @pytest.fixture
    def conn(self):
        connection = SQLiteConnection(database=":memory:")
        connection.make_connection()
        connection.transaction_level = 1  # pin the handle open across query()
        cur = connection._connection.cursor()
        cur.execute(
            "CREATE TABLE receipt("
            "receipt_id TEXT PRIMARY KEY, status TEXT, amount INTEGER)"
        )
        cur.execute("INSERT INTO receipt VALUES ('1', 'old', 10)")
        connection._connection.commit()
        cur.close()
        return connection

    def _run_upsert(self, conn, rows, unique_by, update=None):
        # Compile via a dry builder, execute on the seeded raw connection
        # (the builder's own pool would open a separate blank :memory:
        # database).
        builder = _qb()
        builder.dry = True
        builder.upsert(rows, unique_by=unique_by, update=update, cast=False)
        grammar = builder.get_grammar()
        sql = grammar.compile("upsert", qmark=True).to_sql()
        return conn.query(sql, grammar._bindings)

    def test_upsert_inserts_and_updates(self, conn):
        affected = self._run_upsert(
            conn,
            [
                {"receipt_id": "1", "status": "updated", "amount": 99},
                {"receipt_id": "2", "status": "fresh", "amount": 20},
            ],
            unique_by=["receipt_id"],
        )

        assert affected == 2
        rows = conn.query("SELECT * FROM receipt ORDER BY receipt_id", ())
        assert rows[0]["status"] == "updated"
        assert rows[0]["amount"] == 99
        assert rows[1]["status"] == "fresh"

    def test_do_nothing_leaves_existing_rows(self, conn):
        self._run_upsert(
            conn,
            [
                {"receipt_id": "1", "status": "clobbered", "amount": 0},
                {"receipt_id": "3", "status": "new", "amount": 30},
            ],
            unique_by=["receipt_id"],
            update=[],
        )

        rows = conn.query("SELECT * FROM receipt ORDER BY receipt_id", ())
        assert rows[0]["status"] == "old"  # untouched
        assert rows[-1]["receipt_id"] == "3"  # inserted
