"""``read_scope``: the statement a scoped relation name reads, without a database.

The database half (a write refused with 25006, the relation's rows) is the
product's: ``commons/tests/test_as_of_relations_postgres.py``.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from cara.eloquent import (
    READ_CLOCK_BOUND_SQL,
    BuiltRelation,
    ReadScopeRefused,
    current_read_clock,
    current_read_scope,
    read_now,
    read_once,
    read_scope,
    read_snapshot,
)
from cara.eloquent.ReadScope import read_only_statement, render_literals

AT = datetime(2026, 11, 20, 12, 0, tzinfo=UTC)
RELATION = ("SELECT h.* FROM public.product h WHERE h.tenant_id = %s AND h.at <= %s", [7, AT])


def _scoped(sql: str) -> str:
    with read_scope({"product": RELATION}) as scope:
        return scope.statement(sql)


def test_a_built_relation_is_named_to_the_session_and_read_from_what_it_built():
    built = BuiltRelation(*RELATION, bound=("tenant_id",))
    rendered = render_literals(*RELATION)
    with read_scope({"product": built, "listing": ("SELECT 1 AS id", [])}) as scope:
        sql = "SELECT 1 FROM product p JOIN listing l ON l.id = p.id"
        # What a session must have built before it runs the statement: the
        # built relations the statement reads, never an inline one.
        assert scope.built_in(sql) == (("product", rendered, ("tenant_id",)),)
        assert scope.built_in("SELECT 1 FROM listing") == ()
        assert scope.built_in("UPDATE product SET x = 1") == ()
        assert scope.built_in("WITH product AS (SELECT 1) SELECT * FROM product") == ()
        # Read from the built rows where the session has them, inline elsewhere.
        assert scope.statement(sql, {"product": 'pg_temp."built"'}) == (
            'WITH "listing" AS NOT MATERIALIZED (SELECT 1 AS id), '
            '"product" AS NOT MATERIALIZED (SELECT * FROM pg_temp."built") ' + sql
        )
        assert f'"product" AS NOT MATERIALIZED ({rendered})' in scope.statement(sql)
        # An inner scope's word replaces the outer's, its form included.
        with read_scope({"product": RELATION}) as inner:
            assert inner.built_in(sql) == ()
        with read_scope({"listing": BuiltRelation("SELECT 2 AS id")}) as inner:
            assert [name for name, _sql, _bound in inner.built_in(sql)] == [
                "listing",
                "product",
            ]
    with (
        pytest.raises(ValueError, match="plain column name"),
        read_scope({"product": BuiltRelation("SELECT 1", (), bound=("a b",))}),
    ):
        pass


def test_read_once_keeps_a_value_for_the_block_and_the_scope_it_was_derived_in():
    calls: list[int] = []

    def compute() -> int:
        calls.append(1)
        return len(calls)

    # No block to keep it in: computed by every call.
    assert (read_once("k", compute), read_once("k", compute)) == (1, 2)
    with read_scope({"product": RELATION}):
        assert (read_once("k", compute), read_once("k", compute)) == (3, 4)
    with read_snapshot():
        with read_scope({"product": RELATION}):
            assert (read_once("k", compute), read_once("k", compute)) == (5, 5)
            assert read_once("another key", compute) == 6
        # Another scope of the block (its own clock) derives its own…
        with read_scope({"product": RELATION}, clock=AT):
            assert (read_once("k", compute), read_once("k", compute)) == (7, 7)
        # …and the first scope, met again, still has its value.
        with read_scope({"product": RELATION}):
            assert read_once("k", compute) == 5
    with read_snapshot(), read_scope({"product": RELATION}):
        assert read_once("k", compute) == 8  # another block


def test_a_mentioned_name_reads_the_relation_and_nothing_else_changes():
    sql = 'SELECT "product".* FROM "product" WHERE "product"."tenant_id" = %s'
    scoped = _scoped(sql)
    assert scoped.endswith(sql)
    assert scoped.startswith('WITH "product" AS NOT MATERIALIZED (SELECT h.* FROM public.product h')
    assert "h.tenant_id = 7" in scoped and "'2026-11-20T12:00:00+00:00'::timestamptz" in scoped


def test_a_statement_with_its_own_with_keeps_it_and_its_recursion():
    assert _scoped("WITH x AS (SELECT 1) SELECT * FROM product p, x").startswith(
        'WITH "product" AS NOT MATERIALIZED ('
    )
    assert ", x AS (SELECT 1) SELECT * FROM product p, x" in _scoped(
        "WITH x AS (SELECT 1) SELECT * FROM product p, x"
    )
    assert _scoped("WITH RECURSIVE t AS (SELECT 1) SELECT * FROM product").startswith(
        'WITH RECURSIVE "product" AS NOT MATERIALIZED ('
    )
    own = "WITH product AS (SELECT 1 AS id) SELECT * FROM product"
    assert _scoped(own) == own


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM product_stock_ledger(%s) q",  # another name
        "SELECT * FROM public.product",  # the table itself, qualified
        "SELECT l.product FROM listing l",  # a column
        "UPDATE product SET quantity = 1",  # not a read: refused by the database
        "  -- a comment\n  INSERT INTO product (id) VALUES (1)",
    ],
)
def test_what_is_not_a_read_of_the_name_is_left_as_it_is(sql):
    assert _scoped(sql) == sql


def test_a_name_that_begins_a_longer_one_is_told_apart():
    relations = {
        "product": RELATION,
        "product_kit": ("SELECT k.* FROM public.product_kit k WHERE k.tenant_id = %s", [7]),
    }
    with read_scope(relations) as scope:
        only_kit = scope.statement("SELECT * FROM product_kit WHERE tenant_id = %s")
        both = scope.statement('SELECT * FROM "product" p JOIN product_kit k ON k.id = p.id')
    assert only_kit.startswith('WITH "product_kit" AS NOT MATERIALIZED (')
    assert '"product" AS' not in only_kit
    assert '"product" AS NOT MATERIALIZED (' in both and '"product_kit" AS' in both


def test_a_leading_comment_or_space_does_not_hide_a_read():
    assert _scoped("\n  /* page */ SELECT * FROM product").startswith('WITH "product"')


def test_literals_render_once_and_never_leave_a_bind():
    assert render_literals("a = %s AND b = %s", [1, "o'k"]) == "a = 1 AND b = 'o''k'"
    with pytest.raises(ValueError, match="'%'"):
        render_literals("c LIKE 'x%%'", [])
    with pytest.raises(ValueError, match="placeholders"):
        render_literals("a = %s", [])
    with pytest.raises(ValueError, match="parameters"):
        render_literals("a = 1", [2])


def test_a_relation_that_renders_a_percent_is_refused():
    with pytest.raises(ValueError, match="'%'"), read_scope({"product": ("SELECT %s", ["50%"])}):
        pass


def test_the_scope_is_ambient_nests_and_ends():
    assert current_read_scope() is None
    with read_scope({"product": RELATION}):
        with read_scope({"listing": ("SELECT * FROM public.listing", [])}) as inner:
            assert inner.names() == ("listing", "product")
        assert current_read_scope().names() == ("product",)
    assert current_read_scope() is None
    with pytest.raises(ValueError, match="plain relation name"), read_scope(
        {"public.product": RELATION}
    ):
        pass


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM product WHERE created_at > now() - interval '1 day'",
        "SELECT * FROM product WHERE created_at::date = CURRENT_DATE",
        "SELECT CURRENT_TIMESTAMP",
        "SELECT * FROM product WHERE at < pg_catalog.now()",
        "SELECT clock_timestamp()",
        "WITH x AS (SELECT LOCALTIMESTAMP AS t) SELECT * FROM x",
        "SELECT NOW ()",
    ],
)
def test_a_fixed_clock_refuses_a_read_of_the_database_clock(sql):
    with read_scope(clock=AT) as scope, pytest.raises(ReadScopeRefused, match="clock"):
        scope.statement(sql)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM product WHERE created_at > %s",  # the bound clock
        "SELECT 'now()' AS label, known_now FROM product",  # a literal, a longer word
        "SELECT 1 -- compared with now()\nFROM product",  # a comment
        "SELECT p.current_date_label FROM product p",  # a column
    ],
)
def test_a_fixed_clock_lets_a_bound_instant_and_mere_words_through(sql):
    with read_scope(clock=AT) as scope:
        assert scope.statement(sql) == sql
    # Without a clock nothing is refused: today's reads keep now().
    with read_scope() as scope:
        assert scope.statement("SELECT now()") == "SELECT now()"


def test_the_clock_as_a_bound_is_bound_at_t_and_bounds_nothing_outside():
    """``READ_CLOCK_BOUND_SQL`` holds a log read to the scope's instant; it
    reads no database clock (never refused), and outside a clocked scope it
    is ``'infinity'`` — a row the same transaction just wrote stays seen."""
    sql = f"SELECT * FROM ledger(%s) h WHERE h.recorded_at <= {READ_CLOCK_BOUND_SQL}"
    assert "now(" not in READ_CLOCK_BOUND_SQL and "'infinity'" in READ_CLOCK_BOUND_SQL
    with read_scope(clock=AT) as scope:
        assert scope.statement(sql) == f"SET LOCAL cara.read_clock = '{AT.isoformat()}'; {sql}"
    with read_scope() as scope:
        assert scope.statement(sql) == f"SET LOCAL cara.read_clock = ''; {sql}"


def test_the_clock_is_ambient_aware_and_inherited():
    assert current_read_clock() is None
    with read_scope(clock=AT):
        assert current_read_clock() == AT
        with read_scope({"product": RELATION}):
            assert current_read_clock() == AT
    assert current_read_clock() is None
    with pytest.raises(ValueError, match="aware"), read_scope(clock=AT.replace(tzinfo=None)):
        pass


def test_the_reads_now_is_the_scopes_clock_else_the_aware_wall_clock():
    before = datetime.now(UTC)
    outside = read_now()
    assert outside.tzinfo is not None and before <= outside <= datetime.now(UTC)
    with read_scope(clock=AT):
        assert read_now() == AT
        with read_scope({"product": RELATION}):
            assert read_now() == AT
    with read_scope({"product": RELATION}):
        assert read_now() != AT and read_now().tzinfo is not None


def test_a_refused_relation_is_refused_in_any_read_and_nesting_adds_to_it():
    with read_scope(refuse=("payout_summary",)) as outer:
        with pytest.raises(ReadScopeRefused, match="payout_summary"):
            outer.statement("SELECT * FROM payout_summary WHERE tenant_id = %s")
        with pytest.raises(ReadScopeRefused):
            outer.statement('SELECT 1 FROM x JOIN "payout_summary" s ON true')
        # Another name, the qualified table, a column: not a read of it.
        assert outer.statement("SELECT * FROM payout_summary_kind") == (
            "SELECT * FROM payout_summary_kind"
        )
        assert outer.statement("SELECT s.payout_summary FROM s") == "SELECT s.payout_summary FROM s"
        with read_scope(refuse=("tenant_counter",)) as inner:
            assert inner.refused == ("payout_summary", "tenant_counter")
    with pytest.raises(ValueError, match="plain relation name"), read_scope(refuse=("a.b",)):
        pass


def test_a_snapshot_block_is_kept_by_the_scopes_inside_it():
    with read_snapshot() as snapshot:
        assert current_read_scope().snapshot is snapshot
        with read_scope({"product": RELATION}, clock=AT) as inner:
            assert inner.snapshot is snapshot and inner.clock == AT
            with read_snapshot() as again:
                assert again is snapshot
        assert current_read_scope().snapshot is snapshot
    assert current_read_scope() is None


class _Cursor:
    def __init__(self, log: list[str], fail_on: str | None = None) -> None:
        self.log, self.fail_on = log, fail_on

    def execute(self, sql: str) -> None:
        self.log.append(sql)

    def close(self) -> None:
        self.log.append("close")


class _Connection:
    def __init__(self, autocommit: bool) -> None:
        self.autocommit, self.log = autocommit, []

    def cursor(self) -> _Cursor:
        return _Cursor(self.log)


def test_autocommit_runs_the_statement_in_a_read_only_transaction():
    connection = _Connection(autocommit=True)
    with read_only_statement(connection):
        connection.log.append("statement")
    assert connection.log == ["BEGIN TRANSACTION READ ONLY", "statement", "ROLLBACK", "close"]


def test_an_open_transaction_runs_it_in_a_read_only_savepoint_and_keeps_going():
    connection = _Connection(autocommit=False)
    with pytest.raises(RuntimeError), read_only_statement(connection):
        connection.log.append("statement")
        raise RuntimeError("25006")
    assert connection.log == [
        "SAVEPOINT cara_read_scope",
        "SET LOCAL transaction_read_only = on",
        "statement",
        "ROLLBACK TO SAVEPOINT cara_read_scope",
        "RELEASE SAVEPOINT cara_read_scope",
        "close",
    ]
