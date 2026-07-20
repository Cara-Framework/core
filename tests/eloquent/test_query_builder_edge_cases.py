"""Regression tests for the public ``QueryBuilder`` API.

This file pins behavior of the user-facing ``QueryBuilder`` (the one Models
reach through ``Model.where_in`` etc.) where several silent foot-guns were
discovered:

* ``where_not_in([])`` used to ``return self`` and silently drop the
  exclusion clause — a ``Model.where_not_in('id', external_ids).update(
  {...})`` with an empty ``external_ids`` then updated *every* row.
* ``where_in([None, None])`` emitted ``IN ('None', 'None')`` — literal
  Python repr strings, never matching anything (but bound as data,
  not NULL).
* ``where_not_in([None, None])`` likewise emitted ``NOT IN ('None',
  'None')`` which would match every real row.
* ``update()`` had no empty-WHERE guard despite ``delete()`` having one
  since 2025 — letting a missed-where mass-update slip through.
* ``limit(0)`` silently rendered as "no LIMIT" because the grammar's
  ``if not self._limit`` check treated ``0`` and the False sentinel
  identically.
* ``offset(-1)`` rendered ``OFFSET -1`` (a hard SQL error) instead of
  being rejected at the builder.
"""

from __future__ import annotations

import pytest

from cara.eloquent import DatabaseManager
from cara.eloquent.query import QueryBuilder
from cara.eloquent.query.grammars import SQLiteGrammar
from cara.exceptions import QueryException


@pytest.fixture(scope="module", autouse=True)
def _register_sqlite_connection():
    """Register a single in-memory SQLite connection for the suite.

    QueryBuilder.__init__ calls ``DatabaseManager.validate_connection``
    even when we never execute SQL — we only assemble it via
    ``to_sql()`` / ``to_qmark()``. A throwaway connection is enough.

    The DatabaseManager is a process-wide singleton, and
    ``set_database_config`` REPLACES its whole connection registry +
    default. Other suites share the same process — notably a fully
    booted app whose ``app`` connection this would otherwise wipe for
    the rest of the session. Snapshot the prior config and restore it on
    teardown so this module's throwaway sqlite registry never leaks."""
    dm = DatabaseManager.get_instance()
    _saved_config = dm._database_config
    _saved_default = dm._default_connection
    _saved_connections = dm._connections
    dm.set_database_config(
        "test_qb", {"test_qb": {"driver": "sqlite", "database": ":memory:"}}
    )
    try:
        yield
    finally:
        dm._database_config = _saved_config
        dm._default_connection = _saved_default
        dm._connections = _saved_connections


def _qb(table: str = "users") -> QueryBuilder:
    return QueryBuilder(grammar=SQLiteGrammar, connection="test_qb", table=table)


# ── where_in: empty list, None values ────────────────────────────────


def test_where_in_empty_list_collapses_to_match_nothing():
    qb = _qb().where_in("id", [])
    sql = qb.to_sql()
    # Empty list → ``value_equals(0, 1)`` sentinel → ``0 = 1``.
    assert "0 = 1" in sql, f"empty IN should be 0=1, got: {sql}"


def test_where_in_all_none_collapses_to_match_nothing():
    """All-None previously rendered ``IN ('None', 'None')`` —
    literal repr strings, not NULL — silently matching zero rows
    with two phantom bindings."""
    qb = _qb().where_in("id", [None, None])
    sql = qb.to_sql()
    assert "0 = 1" in sql, f"all-None IN should be 0=1, got: {sql}"
    assert "None" not in sql, f"None literal must not be spliced into SQL, got: {sql}"


def test_where_in_mixed_none_keeps_non_none_values():
    qb = _qb().where_in("id", [1, None, 2, None])
    sql = qb.to_sql()
    # Non-None values survive; Nones dropped.
    assert " IN ('1','2')" in sql, f"unexpected IN clause: {sql}"


def test_where_in_with_values_unchanged():
    qb = _qb().where_in("id", [1, 2, 3])
    sql = qb.to_sql()
    assert " IN ('1','2','3')" in sql, sql


def test_where_in_large_list_keeps_all_bindings():
    """A 1500-element IN list must not be truncated or paginated by
    the builder. Drivers handle binding limits."""
    qb = _qb().where_in("id", list(range(1500)))
    qb.to_qmark()
    assert len(qb._bindings) == 1500


# ── where_not_in: empty / None safety ────────────────────────────────


def test_where_not_in_empty_list_emits_explicit_tautology():
    """Used to silently drop the predicate, turning
    ``Model.where_not_in('id', external_ids).update({...})`` with empty
    ``external_ids`` into "update every row". Now emits ``1 = 1`` so
    the SQL still reflects the (no-op) intent and downstream guards
    can see a WHERE clause exists."""
    qb = _qb().where_not_in("id", [])
    sql = qb.to_sql()
    assert "WHERE 1 = 1" in sql, f"expected explicit 1=1, got: {sql}"


def test_where_not_in_all_none_emits_explicit_tautology():
    qb = _qb().where_not_in("id", [None, None])
    sql = qb.to_sql()
    assert "WHERE 1 = 1" in sql, sql
    assert "NOT IN" not in sql, f"all-None NOT IN must not generate 'NOT IN', got: {sql}"


def test_where_not_in_with_values_unchanged():
    qb = _qb().where_not_in("status", ["banned", "deleted"])
    sql = qb.to_sql()
    assert "NOT IN ('banned','deleted')" in sql, sql


# ── update() empty-WHERE guard (mirrors delete() guard) ──────────────


def test_update_without_where_raises():
    """``delete()`` has refused empty-WHERE since 2025. ``update()``
    must do the same — otherwise a missed where silently becomes a
    mass mutation."""
    qb = _qb()
    with pytest.raises(QueryException, match="without a WHERE clause"):
        qb.update({"status": "banned"})


def test_update_with_explicit_where_raw_passes_guard():
    """The escape hatch: callers who genuinely intend mass-update can
    opt in with an explicit ``where_raw('1 = 1')``. Verify the guard
    accepts that (dry-run so we don't actually hit the DB)."""
    qb = _qb().where_raw("1 = 1")
    qb.update({"status": "ok"}, dry=True)
    sql = qb.to_qmark()
    assert "UPDATE" in sql and "1 = 1" in sql, sql


def test_update_with_where_not_in_empty_still_emits_safe_sql():
    """After the where_not_in fix, the SQL contains an explicit
    ``1 = 1`` predicate. The update guard sees a where exists (so it
    doesn't raise), but the SQL itself shows the no-op intent in
    audit logs."""
    qb = _qb().where_not_in("id", []).update({"status": "x"}, dry=True)
    sql = qb.to_qmark()
    assert "WHERE 1 = 1" in sql, sql


# ── limit / offset: 0, None, negative ────────────────────────────────


def test_limit_zero_renders_limit_zero():
    """Pre-fix, ``process_limit`` used a truthiness check that treated
    ``0`` (a legitimate "return zero rows") as "no limit" — quietly
    returning every row in the table."""
    qb = _qb().where("a", 1).limit(0)
    sql = qb.to_sql()
    assert "LIMIT 0" in sql, f"limit(0) must render LIMIT 0, got: {sql}"


def test_limit_none_clears_limit():
    qb = _qb().where("a", 1).limit(None)
    sql = qb.to_sql()
    assert "LIMIT" not in sql, f"limit(None) should clear, got: {sql}"


def test_limit_negative_raises():
    with pytest.raises(ValueError, match=">= 0"):
        _qb().limit(-1)


def test_limit_bool_rejected():
    """``True``/``False`` would silently coerce to ``1``/``0`` via int
    promotion. Reject explicitly so a callsite like ``limit(some_flag)``
    surfaces the typo instead of silently returning 1 row."""
    with pytest.raises(ValueError):
        _qb().limit(True)
    with pytest.raises(ValueError):
        _qb().limit(False)


def test_offset_negative_raises():
    """``OFFSET -1`` is a hard SQL error — fail
    fast at the builder rather than waiting for the DB round-trip."""
    with pytest.raises(ValueError, match=">= 0"):
        _qb().offset(-1)


def test_offset_zero_is_silently_dropped():
    """``OFFSET 0`` is the SQL default — emitting it is harmless but
    noisy in logs, so the grammar omits it."""
    qb = _qb().where("a", 1).limit(10).offset(0)
    sql = qb.to_sql()
    assert "OFFSET" not in sql, f"OFFSET 0 should be omitted, got: {sql}"


# ── or_where keyword case-insensitivity (smoke test on QB path) ──────


def test_or_where_emits_or_keyword_via_query_builder():
    qb = _qb().where("a", 1).or_where("b", 2)
    sql = qb.to_sql()
    assert " OR " in sql, f"or_where must produce OR, got: {sql}"


# ── nested or_where through closures ─────────────────────────────────


def test_nested_or_where_three_levels_deep():
    """Closure-based nested where groups must compose correctly with
    or_where at multiple depths. Pre-existing case-sensitivity bug
    used to downgrade inner OR keywords to AND silently."""
    qb = (
        _qb()
        .where("a", 1)
        .where(
            lambda q: q.where("b", 2).or_where(
                lambda q2: q2.where("c", 3).or_where("d", 4)
            )
        )
    )
    sql = qb.to_sql()
    assert sql.count(" OR ") == 2, (
        f"expected exactly two OR keywords across the nested groups, got: {sql}"
    )
    assert " AND " in sql, sql
