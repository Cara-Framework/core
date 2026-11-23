"""PostgreSQL catalogue reads follow the session's schema, not ``public``.

Live defect: with no schema configured, ``compile_table_exists``,
``compile_get_all_tables`` and ``get_current_schema`` all filtered on a
hard-coded ``table_schema = 'public'``. The framework's own statements are
unqualified and follow the search_path, so on a role or database whose
search_path is not ``public`` the two disagreed: the migration tracker wrote
a ledger ``has_table`` reported missing, ``get_all_tables`` listed another
schema's tables for ``DROP TABLE`` statements that resolved here, and a
model's columns read as empty. With no schema named the reads now resolve
through the session (``to_regclass`` / ``current_schema()``); a named schema
is still asked about by name.

Only the compiled SQL is asserted: no PostgreSQL needed.
"""

from __future__ import annotations

import pytest

from cara.eloquent.schema.platforms import PostgresPlatform
from cara.exceptions import InvalidArgumentException


@pytest.fixture
def platform() -> PostgresPlatform:
    return PostgresPlatform()


class _Recorder:
    """A connection that answers every query with no rows and keeps the SQL."""

    def __init__(self):
        self.sent: list[str] = []

    def query(self, sql, bindings=()):
        self.sent.append(sql)
        return []


def test_table_exists_without_a_schema_resolves_through_the_search_path(platform):
    sql = platform.compile_table_exists("migrations")

    assert sql == (
        "SELECT c.relname AS table_name FROM pg_catalog.pg_class c "
        "WHERE c.oid = to_regclass('\"migrations\"') "
        "AND c.relkind IN ('r', 'p', 'v', 'f')"
    )
    assert "public" not in sql


def test_table_exists_quotes_each_part_of_a_qualified_name(platform):
    sql = platform.compile_table_exists("audit.events")

    assert 'to_regclass(\'"audit"."events"\')' in sql


def test_table_exists_with_a_named_schema_asks_that_schema(platform):
    sql = platform.compile_table_exists("migrations", schema="tenant_a")

    assert sql == (
        "SELECT * from information_schema.tables "
        "where table_name='migrations' AND table_schema = 'tenant_a'"
    )


def test_get_all_tables_without_a_schema_lists_the_sessions_schema(platform):
    sql = platform.compile_get_all_tables(database="app")

    assert sql == (
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = current_schema() AND table_catalog = 'app'"
    )


def test_get_all_tables_with_a_named_schema_lists_that_schema(platform):
    sql = platform.compile_get_all_tables(database="app", schema="tenant_a")

    assert "WHERE table_schema = 'tenant_a' AND table_catalog = 'app'" in sql


def test_get_current_schema_without_a_schema_reads_the_sessions_columns(platform):
    connection = _Recorder()

    platform.get_current_schema(connection, "orders")

    assert connection.sent == [
        "SELECT * FROM information_schema.columns "
        "WHERE table_schema = current_schema() AND table_name = 'orders'"
    ]


def test_get_current_schema_with_a_named_schema_reads_that_schema(platform):
    connection = _Recorder()

    platform.get_current_schema(connection, "orders", schema="tenant_a")

    (sql,) = connection.sent
    assert "table_schema = 'tenant_a'" in sql
    assert "table_name = 'orders'" in sql


@pytest.mark.parametrize("schema", ["x'; DROP TABLE t; --", "a b"])
def test_a_named_schema_is_still_validated(platform, schema):
    with pytest.raises(InvalidArgumentException):
        platform.compile_get_all_tables(schema=schema)
