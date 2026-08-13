"""Cutover day: the first evolve run against a database with no ledger.

Every other test in this tree assumes ``schema_operation`` exists. The one run
where it does not is the one that matters most — a production database created
before evolve mode, on the day it stops being regenerated. Before these two
rules it died with a raw ``UndefinedTable`` traceback out of ``_already_applied``,
which is to say evolve mode had never once been exercised on the only kind of
database it exists for.
"""

from __future__ import annotations

import importlib

from cara.schema import ADDITIVE, DESTRUCTIVE, LEDGER_TABLE, Operation, sort_operations

_APPLY_MODULE = importlib.import_module("cara.commands.core.SchemaApplyCommand")


def _operation(table, kind="create_table", safety=ADDITIVE):
    return Operation(
        kind=kind,
        table=table,
        key=table if kind == "create_table" else f"{table}.column",
        forward_sql=f"-- {kind} {table}",
        reverse_sql=None,
        safety=safety,
        reason="test",
    )


def test_ledger_table_name_matches_the_model():
    """``LEDGER_TABLE`` is a literal so ``cara.schema`` stays importable
    without the ``db`` extra. That is only safe while it cannot drift."""
    from cara.models.SchemaOperation import SchemaOperation

    assert SchemaOperation.__table__ == LEDGER_TABLE


def test_the_ledger_creates_itself_before_anything_it_must_record():
    """Adversarial by alphabet: three new tables, the ledger sorting LAST by
    name and by nothing else distinguishing them.

    Every operation is recorded the instant it succeeds, so one ordered ahead
    of the ledger would run and then fail to record — a schema that moved with
    no entry saying so, which is the single worst outcome for a command whose
    whole promise is an auditable trail.
    """
    operations = sort_operations(
        [
            _operation("zzz_last"),
            _operation(LEDGER_TABLE),
            _operation("aaa_first"),
        ]
    )

    assert operations[0].table == LEDGER_TABLE


def test_the_ledger_still_precedes_work_that_is_otherwise_safer():
    """Safety ordering is deliberate — harmless work first — but it does not
    outrank the dependency. A destructive ledger creation cannot happen, so
    this pins the rule rather than a plausible plan: nothing sorts above it."""
    operations = sort_operations(
        [
            _operation("users", kind="add_column", safety=ADDITIVE),
            _operation(LEDGER_TABLE, safety=DESTRUCTIVE),
        ]
    )

    assert operations[0].table == LEDGER_TABLE


def test_a_missing_ledger_reads_as_nothing_applied_not_as_an_error():
    """The absence of the ledger is a legitimate answer to "what has been
    applied?" — nothing has, because there was nowhere to record it."""
    from cara.commands.core.SchemaApplyCommand import SchemaApplyCommand

    queries: list[str] = []

    class _NoLedger:
        def select(self, query, bindings=None):
            queries.append(query)
            return [{"oid": None}]

    command = SchemaApplyCommand.__new__(SchemaApplyCommand)
    original = _APPLY_MODULE.DB
    _APPLY_MODULE.DB = _NoLedger()
    try:
        assert command._already_applied("plan123") == set()
    finally:
        _APPLY_MODULE.DB = original

    # It ASKED whether the table exists rather than discovering it by failing.
    assert len(queries) == 1
    assert "to_regclass" in queries[0]


def test_a_broken_connection_is_not_read_as_an_empty_ledger():
    """The dangerous shortcut here is ``except: return set()``. It would turn
    an unreachable database into "nothing has been applied" and re-run a plan
    that already ran — so the existence question must be asked explicitly and
    every other failure must still propagate."""
    import pytest

    from cara.commands.core.SchemaApplyCommand import SchemaApplyCommand

    class _Broken:
        def select(self, query, bindings=None):
            raise RuntimeError("connection refused")

    command = SchemaApplyCommand.__new__(SchemaApplyCommand)
    original = _APPLY_MODULE.DB
    _APPLY_MODULE.DB = _Broken()
    try:
        with pytest.raises(RuntimeError, match="connection refused"):
            command._already_applied("plan123")
    finally:
        _APPLY_MODULE.DB = original
