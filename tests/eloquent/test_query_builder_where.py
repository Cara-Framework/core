"""Regression tests for the query builder / grammar WHERE pipeline.

Each test pins behavior that was demonstrably broken before the fixes
shipped alongside this file. Run with ``pytest tests/eloquent``.
"""

from cara.eloquent.expressions import QueryExpression
from cara.eloquent.query.builders import WhereBuilder
from cara.eloquent.query.grammars import SQLiteGrammar


def _make_grammar(wheres):
    """Build a grammar with a pre-populated where list for SQL assembly."""
    grammar = SQLiteGrammar(wheres=tuple(wheres))
    return grammar


# ── BaseGrammar OR keyword case-insensitivity ────────────────────────


def test_or_where_uppercase_keyword_emits_or_in_sql():
    """WhereBuilder stores ``keyword="OR"``; the grammar previously
    only routed to the OR branch on lowercase ``"or"``, silently
    downgrading every or_where to AND."""
    wheres = [
        QueryExpression("a", "=", 1, "value", keyword="AND"),
        QueryExpression("b", "=", 2, "value", keyword="OR"),
    ]
    sql = _make_grammar(wheres).process_wheres()
    assert " OR " in sql, f"expected OR keyword, got: {sql}"
    assert " AND b " not in sql, f"or_where downgraded to AND: {sql}"


def test_or_where_lowercase_keyword_still_works():
    """QueryBuilder uses ``keyword="or"`` (lowercase). The case-
    insensitive fix must not break that historical path."""
    wheres = [
        QueryExpression("a", "=", 1, "value", keyword="AND"),
        QueryExpression("b", "=", 2, "value", keyword="or"),
    ]
    sql = _make_grammar(wheres).process_wheres()
    assert " OR " in sql


def test_first_where_uses_where_keyword():
    wheres = [QueryExpression("a", "=", 1, "value", keyword="AND")]
    sql = _make_grammar(wheres).process_wheres()
    assert sql.lstrip().startswith("WHERE"), f"first clause should be WHERE: {sql}"


def test_subsequent_and_keyword_uses_and():
    wheres = [
        QueryExpression("a", "=", 1, "value", keyword="AND"),
        QueryExpression("b", "=", 2, "value", keyword="AND"),
    ]
    sql = _make_grammar(wheres).process_wheres()
    assert " AND " in sql


# ── WhereBuilder.where_in: empty / all-None safety ───────────────────


def test_where_in_empty_list_produces_match_nothing():
    """An empty IN-list must collapse to ``1 = 0``, never ``IN ()``."""
    wb = WhereBuilder().where_in("id", [])
    assert len(wb.get_wheres()) == 1
    expr = wb.get_wheres()[0]
    assert expr.raw is True
    assert "1 = 0" in expr.column


def test_where_in_all_none_collapses_to_match_nothing():
    """All-None list previously generated invalid ``IN ()`` SQL."""
    wb = WhereBuilder().where_in("id", [None, None, None])
    # Should produce exactly one raw "1 = 0" predicate
    assert len(wb.get_wheres()) == 1
    expr = wb.get_wheres()[0]
    assert expr.raw is True
    assert "1 = 0" in expr.column
    # No bindings — all values were dropped
    assert wb.get_bindings() == []


def test_where_in_mixed_none_filters_none_values():
    """Non-empty cleaned list keeps the IN clause with surviving values."""
    wb = WhereBuilder().where_in("id", [1, None, 2])
    exprs = wb.get_wheres()
    assert len(exprs) == 1
    assert exprs[0].equality == "IN"
    assert exprs[0].value == [1, 2]
    assert wb.get_bindings() == [1, 2]


def test_where_in_with_values_produces_in_clause():
    wb = WhereBuilder().where_in("id", [1, 2, 3])
    exprs = wb.get_wheres()
    assert exprs[0].equality == "IN"
    assert exprs[0].value == [1, 2, 3]
    assert wb.get_bindings() == [1, 2, 3]


# ── WhereBuilder.where_not_in: must NOT silently no-op ───────────────


def test_where_not_in_empty_list_emits_explicit_tautology():
    """Empty exclusion list previously silently dropped the clause —
    catastrophic in ``Model.where_not_in('id', []).delete()``. Now it
    emits ``1 = 1`` so the predicate is explicit and the chain is
    not silently empty."""
    wb = WhereBuilder().where_not_in("id", [])
    exprs = wb.get_wheres()
    assert len(exprs) == 1, "where_not_in([]) must add an explicit predicate"
    assert exprs[0].raw is True
    assert "1 = 1" in exprs[0].column


def test_where_not_in_all_none_emits_explicit_tautology():
    wb = WhereBuilder().where_not_in("id", [None, None])
    exprs = wb.get_wheres()
    assert len(exprs) == 1
    assert exprs[0].raw is True
    assert "1 = 1" in exprs[0].column
    assert wb.get_bindings() == []


def test_where_not_in_with_values_produces_not_in_clause():
    wb = WhereBuilder().where_not_in("status", ["banned", "deleted"])
    exprs = wb.get_wheres()
    assert exprs[0].equality == "NOT IN"
    assert exprs[0].value == ["banned", "deleted"]
    assert wb.get_bindings() == ["banned", "deleted"]


# ── WhereBuilder NULL handling: IS NULL / IS NOT NULL ────────────────


def test_where_null_uses_is_null_value_type():
    wb = WhereBuilder().where_null("deleted_at")
    expr = wb.get_wheres()[0]
    assert expr.value_type == "NULL"
    assert expr.value is None


def test_where_not_null_uses_is_not_null_value_type():
    wb = WhereBuilder().where_not_null("deleted_at")
    expr = wb.get_wheres()[0]
    assert expr.value_type == "NOT NULL"


# ── Mixed AND/OR chains ──────────────────────────────────────────────


def test_mixed_and_or_chain_routes_correctly():
    """``.where('a', 1).where('b', 2).or_where('c', 3)`` — verify the
    grammar emits one OR (between b and c) and one AND (between a and
    b)."""
    wb = WhereBuilder().where("a", 1).where("b", 2).or_where("c", 3)
    sql = _make_grammar(wb.get_wheres()).process_wheres()
    assert " AND " in sql
    assert " OR " in sql
    # Exactly one OR — the or_where, not anything else
    assert sql.count(" OR ") == 1
