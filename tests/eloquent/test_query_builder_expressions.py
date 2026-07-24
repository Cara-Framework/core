"""Tests for the query-builder expressiveness features.

Covers four additions to the Cara query layer, all exercised through the
SQLite grammar (no live DB needed — we only assemble SQL via ``to_sql`` /
``to_qmark``) plus a few Postgres-grammar assertions where the dialect
matters (row locks):

1. ``F()`` column-reference expressions in writes and filters.
2. ``select_window`` window functions.
3. ``GREATEST`` / ``LEAST`` SELECT helpers + use inside F-style updates.
4. ``lock_for_update`` row-lock modifiers (SKIP LOCKED / NOWAIT / OF).

The connection-registration fixture mirrors
``test_query_builder_edge_cases`` so the process-wide DatabaseManager
singleton is snapshotted and restored — the throwaway sqlite registry must
never leak into a co-resident booted app.
"""

from __future__ import annotations

import pytest

from cara.eloquent import DatabaseManager
from cara.eloquent.expressions import F, Greatest, Least, Operation
from cara.eloquent.query import QueryBuilder
from cara.eloquent.query.grammars import PostgresGrammar, SQLiteGrammar
from cara.exceptions import InvalidArgumentException


@pytest.fixture(scope="module", autouse=True)
def _register_connections():
    dm = DatabaseManager.get_instance()
    _saved_config = dm._database_config
    _saved_default = dm._default_connection
    _saved_connections = dm._connections
    dm.set_database_config(
        "test_expr",
        {
            "test_expr": {"driver": "sqlite", "database": ":memory:"},
            "test_expr_pg": {"driver": "sqlite", "database": ":memory:"},
        },
    )
    try:
        yield
    finally:
        dm._database_config = _saved_config
        dm._default_connection = _saved_default
        dm._connections = _saved_connections


def _qb(table: str = "listing") -> QueryBuilder:
    return QueryBuilder(grammar=SQLiteGrammar, connection="test_expr", table=table)


def _pg(table: str = "job") -> QueryBuilder:
    return QueryBuilder(grammar=PostgresGrammar, connection="test_expr", table=table)


# ════════════════════════════════════════════════════════════════════
# Feature 1 — F() column-reference expressions
# ════════════════════════════════════════════════════════════════════


def test_f_column_renders_as_quoted_identifier():
    g = SQLiteGrammar(table="listing")
    assert g.compile_expression(F("click_count")) == '"listing"."click_count"'


def test_f_plus_literal_in_update_self_references_column():
    sql = (
        _qb()
        .where("id", 5)
        .update({"click_count": F("click_count") + 1}, dry=True)
        .to_sql()
    )
    assert '"click_count" = "click_count" + \'1\'' in sql, sql
    # The increment value is INLINED (matches increment_string convention),
    # not a bound %s param.
    assert "?" not in sql


def test_f_minus_f_in_update():
    # In UPDATE context the column_strings template is unqualified
    # (``"col"`` — no table prefix), which is correct SQL: ``UPDATE listing
    # SET ...`` columns implicitly belong to the target table.
    sql = (
        _qb()
        .where("id", 1)
        .update({"price_low": F("price_low") - F("discount")}, dry=True)
        .to_sql()
    )
    assert '"price_low" = "price_low" - "discount"' in sql, sql


def test_f_arithmetic_supports_all_four_operators():
    g = SQLiteGrammar(table="t")
    assert g.compile_expression(F("a") + 1) == '"t"."a" + \'1\''
    assert g.compile_expression(F("a") - 1) == '"t"."a" - \'1\''
    assert g.compile_expression(F("a") * 2) == '"t"."a" * \'2\''
    assert g.compile_expression(F("a") / 2) == '"t"."a" / \'2\''


def test_f_reverse_arithmetic_keeps_operand_order():
    g = SQLiteGrammar(table="t")
    # ``1 + F('x')`` must render with the literal on the LEFT.
    assert g.compile_expression(1 + F("x")) == '\'1\' + "t"."x"'
    assert g.compile_expression(10 - F("x")) == '\'10\' - "t"."x"'


def test_nested_operation_is_parenthesized():
    g = SQLiteGrammar(table="t")
    assert g.compile_expression((F("a") + 1) * 2) == "(\"t\".\"a\" + '1') * '2'"


def test_where_f_compared_to_f_emits_two_quoted_columns_no_binding():
    qb = _qb().where(F("price_low"), ">", F("price_high"))
    sql = qb.to_qmark()
    assert '"listing"."price_low" > "listing"."price_high"' in sql, sql
    # Neither side is a bound value — the qmark pass collected no bindings.
    assert list(qb._bindings) == []


def test_where_f_compared_to_literal_escapes_literal():
    sql = _qb().where(F("price_low"), "<", 100).to_sql()
    assert '"listing"."price_low" < \'100\'' in sql, sql


def test_or_where_f_expression_emits_or_keyword():
    sql = _qb().where("id", 1).or_where(F("a"), "=", F("b")).to_sql()
    assert " OR " in sql
    assert '"listing"."a" = "listing"."b"' in sql, sql


def test_f_update_does_not_break_mixed_literal_columns():
    """A dict mixing an F expression with a plain value must bind only the
    plain value and inline the expression."""
    qb = _qb().where("id", 5)
    qb.update({"click_count": F("click_count") + 1, "name": "x"}, dry=True)
    sql = qb.to_qmark()
    assert '"click_count" = "click_count" + \'1\'' in sql, sql
    # qmark placeholders render as ``'?'`` in the SQL string (later swapped
    # for the driver param marker); the F operand stays inlined.
    assert "\"name\" = '?'" in sql, sql
    # Only the literal 'x' and the where value 5 are bound — not the F operand.
    assert list(qb._bindings) == ["x", 5]


def test_increment_still_works_unchanged():
    """The generalization must not regress the existing increment path."""
    sql = _qb().where("id", 1).increment("views", 3, dry=True)
    assert '"views" = "views" + \'3\'' in sql, sql


def test_operation_rejects_unknown_operator():
    with pytest.raises(InvalidArgumentException):
        Operation(F("a"), "%", F("b"))


# ════════════════════════════════════════════════════════════════════
# Feature 2 — window functions
# ════════════════════════════════════════════════════════════════════


def test_select_window_row_number_partition_and_order():
    qb = (
        _qb()
        .select("*")
        .select_window(
            "ROW_NUMBER()",
            partition_by=["product_id"],
            order_by=[("price_low", "asc")],
            alias="rn",
        )
    )
    sql = qb.to_sql()
    assert (
        'ROW_NUMBER() OVER (PARTITION BY "listing"."product_id" '
        'ORDER BY "listing"."price_low" ASC) AS "rn"' in sql
    ), sql


def test_select_window_rank_with_string_partition_and_order():
    qb = (
        _qb()
        .select("id")
        .select_window("RANK()", partition_by="brand_id", order_by="created_at")
    )
    sql = qb.to_sql()
    # Default direction is ASC, default alias is "rn".
    assert (
        'RANK() OVER (PARTITION BY "listing"."brand_id" '
        'ORDER BY "listing"."created_at" ASC) AS "rn"' in sql
    ), sql


def test_select_window_lag_descending():
    qb = (
        _qb()
        .select("id")
        .select_window(
            "LAG(price_low)",
            partition_by=["product_id"],
            order_by=[("created_at", "desc")],
            alias="prev_price",
        )
    )
    sql = qb.to_sql()
    assert "LAG(price_low) OVER (" in sql, sql
    assert 'ORDER BY "listing"."created_at" DESC) AS "prev_price"' in sql, sql


def test_select_window_multiple_partition_columns():
    qb = _qb().select_window(
        "ROW_NUMBER()", partition_by=["a", "b"], order_by=[("c", "asc")]
    )
    sql = qb.to_sql()
    assert 'PARTITION BY "listing"."a", "listing"."b"' in sql, sql


def test_select_window_rejects_injection_in_partition():
    with pytest.raises(InvalidArgumentException):
        _qb().select_window("ROW_NUMBER()", partition_by=["id; DROP TABLE x"])


def test_select_window_rejects_bad_direction():
    with pytest.raises(InvalidArgumentException):
        _qb().select_window("ROW_NUMBER()", order_by=[("price", "sideways")])


# ════════════════════════════════════════════════════════════════════
# Feature 3 — GREATEST / LEAST
# ════════════════════════════════════════════════════════════════════


def test_select_greatest_with_alias():
    sql = (
        _qb().select_greatest("price_low", "floor_price", alias="effective_low").to_sql()
    )
    assert (
        'GREATEST("listing"."price_low", "listing"."floor_price") AS "effective_low"'
        in sql
    ), sql


def test_select_least_without_alias():
    sql = _qb().select_least("a", "b").to_sql()
    assert 'LEAST("listing"."a", "listing"."b")' in sql, sql


def test_least_inside_f_style_update():
    sql = (
        _qb()
        .where("id", 1)
        .update({"price_low": Least(F("price_low"), 10)}, dry=True)
        .to_sql()
    )
    assert '"price_low" = LEAST("price_low", \'10\')' in sql, sql
    assert "?" not in sql


def test_greatest_inside_f_style_update():
    # UPDATE context → unqualified column identifiers (see
    # ``test_f_minus_f_in_update``).
    sql = (
        _qb()
        .where("id", 1)
        .update({"price_high": Greatest(F("price_high"), F("ceiling"))}, dry=True)
        .to_sql()
    )
    assert '"price_high" = GREATEST("price_high", "ceiling")' in sql, sql


def test_greatest_compiles_literal_argument_as_value():
    g = SQLiteGrammar(table="t")
    assert g.compile_expression(Greatest(F("a"), 5)) == 'GREATEST("t"."a", \'5\')'


def test_least_requires_at_least_one_argument():
    with pytest.raises(InvalidArgumentException):
        Least()


def test_greatest_composes_with_arithmetic():
    g = SQLiteGrammar(table="t")
    expr = Greatest(F("a"), F("b")) + 1
    assert g.compile_expression(expr) == 'GREATEST("t"."a", "t"."b") + \'1\''


# ════════════════════════════════════════════════════════════════════
# Feature 4 — row-lock modifiers
# ════════════════════════════════════════════════════════════════════


def test_lock_for_update_skip_locked():
    sql = _pg().where("status", "queued").lock_for_update(skip_locked=True).to_sql()
    assert sql.rstrip().endswith("FOR UPDATE SKIP LOCKED"), sql


def test_lock_for_update_nowait():
    sql = _pg().where("status", "queued").lock_for_update(nowait=True).to_sql()
    assert sql.rstrip().endswith("FOR UPDATE NOWAIT"), sql


def test_lock_for_update_of_tables():
    sql = _pg().where("status", "queued").lock_for_update(of=["job"]).to_sql()
    assert 'FOR UPDATE OF "job"' in sql, sql


def test_lock_for_update_of_single_string():
    sql = _pg().where("status", "queued").lock_for_update(of="job").to_sql()
    assert 'FOR UPDATE OF "job"' in sql, sql


def test_lock_for_update_plain_unchanged():
    """No modifiers → plain FOR UPDATE, the historical behavior."""
    sql = _pg().where("status", "queued").lock_for_update().to_sql()
    assert sql.rstrip().endswith("FOR UPDATE"), sql
    assert "SKIP LOCKED" not in sql and "NOWAIT" not in sql


def test_shared_lock_unchanged():
    sql = _pg().where("status", "queued").shared_lock().to_sql()
    assert "FOR SHARE" in sql, sql


def test_skip_locked_and_nowait_are_mutually_exclusive():
    with pytest.raises(InvalidArgumentException):
        _pg().lock_for_update(skip_locked=True, nowait=True)


def test_lock_modifiers_noop_on_sqlite_lock():
    """SQLite's lock map is empty — modifiers must not emit invalid SQL."""
    sql = _qb().where("status", "queued").lock_for_update(skip_locked=True).to_sql()
    assert "SKIP LOCKED" not in sql, sql
