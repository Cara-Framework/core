"""Regression pins for affected-row-count reporting.

Non-result statements (UPDATE/DELETE/INSERT without RETURNING) used to
come back as an empty rowset from ``Connection.query`` — ``DB.statement``
callers saw ``{}``/``None`` and chunked-prune loops keyed on "rows
affected" stalled after the first batch. The connections now surface
``cursor.rowcount`` for statements that produce no rowset, and the
non-model (table-level) ``update()`` passes it through (Laravel parity).
"""

from __future__ import annotations

from cara.eloquent.connections import SQLiteConnection


def _seeded_connection(rows: int = 5) -> SQLiteConnection:
    conn = SQLiteConnection(database=":memory:")
    conn.make_connection()
    # Pin the handle open across query() calls — query()'s finally block
    # closes at transaction level 0, so emulate an active transaction.
    conn.transaction_level = 1
    cur = conn._connection.cursor()
    cur.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, flag INTEGER DEFAULT 0)")
    cur.executemany("INSERT INTO t(id) VALUES (?)", [(i,) for i in range(rows)])
    cur.close()
    return conn


class TestNonResultStatementsReturnRowcount:
    def test_delete_reports_affected_rows(self):
        conn = _seeded_connection(5)

        affected = conn.query("DELETE FROM t WHERE id < 3", ())

        assert affected == 3

    def test_update_reports_affected_rows(self):
        conn = _seeded_connection(5)

        affected = conn.query("UPDATE t SET flag = 1 WHERE id >= 2", ())

        assert affected == 3

    def test_zero_match_reports_zero(self):
        conn = _seeded_connection(2)

        affected = conn.query("DELETE FROM t WHERE id > 999", ())

        assert affected == 0

    def test_select_still_returns_rows(self):
        conn = _seeded_connection(2)

        rows = conn.query("SELECT id FROM t ORDER BY id", ())

        assert [row["id"] for row in rows] == [0, 1]
