"""Global scopes constrain EVERY caller where, OR-joined ones included.

A scope ANDs its predicate onto the builder's wheres, and the grammar joins
each where with its own keyword. With a top-level ``or_where`` that compiled
to ``a OR b AND tenant_id = ?`` — which SQL reads as ``a OR (b AND
tenant_id = ?)``: every tenant's rows matching ``a``, trashed ones too, for a
SELECT, an UPDATE and a soft DELETE alike. ``run_scopes`` now groups the
caller's wheres first (Laravel's ``addNewWheresWithinGroup``).
"""

from __future__ import annotations

import pytest

from cara.context import Tenancy
from cara.eloquent import DatabaseManager
from cara.eloquent.query import QueryBuilder
from cara.eloquent.query.grammars import PostgresGrammar
from cara.eloquent.scopes import SoftDeleteScope, TenantScope
from cara.testing.FacadeSwap import swap


@pytest.fixture(autouse=True)
def _tenant_five():
    """A throwaway connection (the builder validates one even when it only
    compiles) and tenant 5 in context."""
    manager = DatabaseManager(
        "test_scopes", {"test_scopes": {"driver": "sqlite", "database": ":memory:"}}
    )
    token = Tenancy.set(5)
    with swap("DB", manager):
        yield
    Tenancy.reset(token)


def _listing(scoped: bool = True) -> QueryBuilder:
    builder = QueryBuilder(
        grammar=PostgresGrammar, connection="test_scopes", table="listing"
    )
    if scoped:
        TenantScope().on_boot(builder)
        SoftDeleteScope().on_boot(builder)
    return builder


def test_a_select_keeps_the_or_inside_the_tenant_and_trash_filters():
    sql = _listing().where("status", "active").or_where("sku", "X").to_sql()

    assert sql == (
        'SELECT * FROM "listing" WHERE ("listing"."status" = \'active\' OR '
        '"listing"."sku" = \'X\') AND "listing"."tenant_id" = \'5\' AND '
        '"listing"."deleted_at" IS NULL'
    )


def test_the_bindings_follow_the_grouped_placeholders():
    builder = _listing().where("status", "active").or_where("sku", "X")

    sql = builder.to_qmark()

    assert sql.startswith(
        'SELECT * FROM "listing" WHERE ("listing"."status" = \'?\' OR '
        '"listing"."sku" = \'?\') AND "listing"."tenant_id" = \'?\''
    )
    assert list(builder._bindings) == ["active", "X", 5]


def test_an_update_never_reaches_another_tenants_rows():
    sql = (
        _listing()
        .where("channel_id", 1)
        .or_where("external_sku", "X")
        .update({"price": 1}, dry=True)
        .to_sql()
    )

    assert sql.endswith(
        'WHERE ("listing"."channel_id" = \'1\' OR "listing"."external_sku" = \'X\') '
        "AND \"tenant_id\" = '5'"
    )


def test_a_soft_delete_never_reaches_another_tenants_rows():
    sql = (
        _listing()
        .where("channel_id", 1)
        .or_where_null("external_sku")
        .delete(query=True)
        .to_sql()
    )

    assert sql.startswith('UPDATE "listing" SET "deleted_at" = ')
    assert sql.endswith(
        'WHERE ("listing"."channel_id" = \'1\' OR "listing"."external_sku" IS NULL) '
        "AND \"tenant_id\" = '5'"
    )


def test_an_or_raw_where_is_grouped_too():
    sql = _listing().where("status", "active").or_where_raw("price > 10").to_sql()

    assert '("listing"."status" = \'active\' OR price > 10) AND' in sql


def test_and_only_wheres_compile_unchanged():
    sql = _listing().where("a", 1).where("b", 2).to_sql()

    assert sql == (
        'SELECT * FROM "listing" WHERE "listing"."a" = \'1\' AND "listing"."b" = '
        '\'2\' AND "listing"."tenant_id" = \'5\' AND "listing"."deleted_at" IS NULL'
    )


def test_a_builder_without_scopes_compiles_unchanged():
    sql = _listing(scoped=False).where("a", 1).or_where("b", 2).to_sql()

    assert sql == (
        'SELECT * FROM "listing" WHERE "listing"."a" = \'1\' OR "listing"."b" = \'2\''
    )
