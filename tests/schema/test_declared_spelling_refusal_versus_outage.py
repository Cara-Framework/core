"""A refused probe statement is "cannot normalise"; a lost database propagates.

Live defect (the probe now lives in ``cara/schema/DeclaredSpelling.py``):
every step of the CHECK / index spelling probe was wrapped in
``except Exception``. A statement the server refused and a database that
had gone away read the same — the declaration was left unspelled, which is
never called changed — so an outage mid-``schema:check`` reported "nothing
drifted". Now only ``QueryException`` (a refused statement) is caught and
logged; ``DatabaseUnavailableException`` propagates, and the probe's
connection is still released.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest

from cara.exceptions import DatabaseUnavailableException, QueryException
from cara.schema import LiveSchema, normalize_declared_checks, normalize_declared_indexes

TABLE = "orders"


def _live() -> LiveSchema:
    return LiveSchema(
        tables={TABLE: {"a": {"data_type": "integer", "is_nullable": True}}},
        checks={TABLE: {"orders_a_check"}},
        indexes={TABLE: {"orders_a_index"}},
        constraint_indexes={TABLE: set()},
        constraint_definitions={TABLE: {"orders_a_check": "CHECK ((a > 0))"}},
        index_definitions={
            TABLE: {
                "orders_a_index": "CREATE INDEX orders_a_index ON public.orders USING btree (a)"
            }
        },
    )


class _Connection:
    """Answers every statement, except the first one starting ``fail_on``."""

    def __init__(self, fail_on: str, error: Exception) -> None:
        self.fail_on = fail_on
        self.error = error
        self.statements: list[str] = []

    def query(self, sql: str, _params):
        self.statements.append(sql)
        if sql.startswith(self.fail_on):
            raise self.error
        if "pg_get_constraintdef" in sql:
            return [{"definition": "CHECK ((a > 0))"}]
        if "pg_get_indexdef" in sql:
            return [{"definition": "CREATE INDEX x ON pg_temp.p USING btree (a)"}]
        return []


def _introspector(connection: _Connection):
    released: list = []
    manager = SimpleNamespace(
        create_connection_instance=lambda: connection,
        release=released.append,
    )
    return (
        SimpleNamespace(
            get_schema=lambda: "public",
            query_executor=SimpleNamespace(connection_manager=manager),
        ),
        released,
    )


def _checks(connection):
    introspector, released = _introspector(connection)
    result = normalize_declared_checks(
        introspector, _live(), {TABLE: [("orders_a_check", "a > 0")]}
    )
    return result, released


def _indexes(connection):
    introspector, released = _introspector(connection)
    result = normalize_declared_indexes(
        introspector, _live(), {TABLE: [("orders_a_index", False, ' ("a")')]}
    )
    return result, released


def test_a_working_probe_spells_both_sides() -> None:
    result, _ = _checks(_Connection("never", RuntimeError()))
    assert result.declared_check_definitions == {
        TABLE: {"orders_a_check": "CHECK ((a > 0))"}
    }


@pytest.mark.parametrize("run", [_checks, _indexes], ids=["checks", "indexes"])
def test_a_refused_temp_table_is_logged_and_leaves_nothing_spelled(run, caplog) -> None:
    connection = _Connection("CREATE TEMP TABLE", QueryException("permission denied"))

    with caplog.at_level(logging.DEBUG, logger="cara.schema"):
        result, released = run(connection)

    assert not (result.declared_check_definitions or result.declared_index_definitions)
    assert released == [connection]
    assert any(
        r.levelno == logging.WARNING and "refused" in r.getMessage()
        for r in caplog.records
    )


@pytest.mark.parametrize(
    ("run", "statement"),
    [(_checks, "ALTER TABLE"), (_indexes, "CREATE INDEX")],
    ids=["checks", "indexes"],
)
def test_a_refused_declaration_is_logged_and_left_unspelled(
    run, statement, caplog
) -> None:
    connection = _Connection(statement, QueryException("syntax error"))

    with caplog.at_level(logging.DEBUG, logger="cara.schema"):
        run(connection)

    assert any(
        r.levelno == logging.DEBUG and "could not" in r.getMessage()
        for r in caplog.records
    )


@pytest.mark.parametrize(
    ("run", "statement"),
    [
        (_checks, "CREATE TEMP TABLE"),
        (_checks, "ALTER TABLE"),
        (_indexes, "CREATE TEMP TABLE"),
        (_indexes, "CREATE INDEX"),
    ],
    ids=["checks-probe", "checks-attach", "indexes-probe", "indexes-build"],
)
def test_a_lost_database_propagates_instead_of_reading_clean(run, statement) -> None:
    connection = _Connection(statement, DatabaseUnavailableException("server closed"))
    introspector, released = _introspector(connection)

    with pytest.raises(DatabaseUnavailableException):
        if run is _checks:
            normalize_declared_checks(
                introspector, _live(), {TABLE: [("orders_a_check", "a > 0")]}
            )
        else:
            normalize_declared_indexes(
                introspector, _live(), {TABLE: [("orders_a_index", False, ' ("a")')]}
            )
    assert released == [connection], "the probe connection still goes back"
