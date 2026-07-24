"""Compiled-DDL tests for the schema-builder constraint DSL:

  1. ``Blueprint.check(expression, name=None)`` — CHECK constraints as code,
     emitted inline in CREATE TABLE and as ``ALTER TABLE ... ADD CONSTRAINT
     ... CHECK (...)`` on an existing table. Replaces the raw
     ``__indexes__`` / ``DB.statement`` escape hatch (see
     ``commons/models/core/Listing.py``'s ``listing_*_non_negative`` checks).

  2. ``Blueprint.unique(columns, name=None, where=...)`` /
     ``Blueprint.partial_unique(...)`` — conditional / partial UNIQUE that
     compiles to a Postgres partial unique index
     (``CREATE UNIQUE INDEX <name> ON <table> (...) WHERE <predicate>``), the
     "unique only among non-deleted / active rows" pattern.

Every assertion pins the COMPILED DDL string the platform produces.
The plain ``unique()`` path is pinned byte-identical so the new ``where=``
kwarg is strictly additive.
"""

from __future__ import annotations

import pytest

from cara.eloquent.schema.Blueprint import Blueprint
from cara.eloquent.schema.platforms.PostgresPlatform import PostgresPlatform


@pytest.fixture
def platform() -> PostgresPlatform:
    return PostgresPlatform()


# --------------------------------------------------------------------------
# CHECK constraints — CREATE TABLE
# --------------------------------------------------------------------------


def test_check_renders_inline_constraint_in_create_table(platform):
    blueprint = Blueprint(None, table="listing", platform=platform, action="create")
    blueprint.decimal("current_price", 10, 2).nullable()
    blueprint.check("current_price IS NULL OR current_price >= 0")

    sql = blueprint.to_sql()[0]
    assert (
        "CONSTRAINT listing_current_price_is_null_or_current_price_0_check "
        "CHECK (current_price IS NULL OR current_price >= 0)"
    ) in sql
    assert sql.startswith('CREATE TABLE "listing" (')


def test_check_uses_explicit_name_when_given(platform):
    blueprint = Blueprint(None, table="listing", platform=platform, action="create")
    blueprint.decimal("current_price", 10, 2).nullable()
    blueprint.check(
        "current_price IS NULL OR current_price >= 0",
        name="listing_current_price_non_negative",
    )

    sql = blueprint.to_sql()[0]
    assert (
        "CONSTRAINT listing_current_price_non_negative "
        "CHECK (current_price IS NULL OR current_price >= 0)"
    ) in sql


def test_check_auto_name_is_table_slug_check(platform):
    blueprint = Blueprint(None, table="product", platform=platform, action="create")
    blueprint.integer("rating")
    blueprint.check("rating BETWEEN 0 AND 5")

    # <table>_<slug-of-expression>_check
    constraint = blueprint.table.get_added_constraints()[
        "product_rating_between_0_and_5_check"
    ]
    assert constraint.constraint_type == "check"
    assert constraint.expression == "rating BETWEEN 0 AND 5"


# --------------------------------------------------------------------------
# CHECK constraints — ALTER TABLE (existing table)
# --------------------------------------------------------------------------


def test_check_renders_alter_add_constraint(platform):
    blueprint = Blueprint(None, table="listing", platform=platform, action="alter")
    blueprint.check(
        "current_total_amount IS NULL OR current_total_amount >= 0",
        name="listing_current_total_non_negative",
    )

    sql = blueprint.to_sql()
    assert sql == [
        'ALTER TABLE "listing" ADD CONSTRAINT listing_current_total_non_negative '
        "CHECK (current_total_amount IS NULL OR current_total_amount >= 0)"
    ]


# --------------------------------------------------------------------------
# Partial / conditional UNIQUE — CREATE TABLE
# --------------------------------------------------------------------------


def test_partial_unique_via_where_kwarg_emits_partial_index(platform):
    blueprint = Blueprint(None, table="listing", platform=platform, action="create")
    blueprint.unsigned_big_integer("marketplace_id").nullable()
    blueprint.string("external_id", 255).nullable()
    blueprint.unique(
        ["marketplace_id", "external_id"],
        name="listing_marketplace_external_unique",
        where="marketplace_id IS NOT NULL AND external_id IS NOT NULL",
    )

    sql = blueprint.to_sql()
    create = sql[0]
    # The conditional UNIQUE must NOT appear inline in the CREATE TABLE body.
    assert "UNIQUE" not in create
    # It is a standalone partial unique index instead.
    assert sql[1] == (
        "CREATE UNIQUE INDEX listing_marketplace_external_unique "
        'ON "listing" ("marketplace_id", "external_id") '
        "WHERE marketplace_id IS NOT NULL AND external_id IS NOT NULL"
    )


def test_partial_unique_helper_matches_where_kwarg(platform):
    blueprint = Blueprint(None, table="listing", platform=platform, action="create")
    blueprint.unsigned_big_integer("marketplace_id").nullable()
    blueprint.string("external_id", 255).nullable()
    blueprint.partial_unique(
        ["marketplace_id", "external_id"],
        where="deleted_at IS NULL",
        name="listing_active_external_unique",
    )

    sql = blueprint.to_sql()
    assert sql[1] == (
        "CREATE UNIQUE INDEX listing_active_external_unique "
        'ON "listing" ("marketplace_id", "external_id") '
        "WHERE deleted_at IS NULL"
    )


def test_partial_unique_auto_name(platform):
    blueprint = Blueprint(None, table="listing", platform=platform, action="create")
    blueprint.string("slug", 500).nullable()
    blueprint.unique("slug", where="deleted_at IS NULL")

    sql = blueprint.to_sql()
    assert sql[1] == (
        "CREATE UNIQUE INDEX listing_slug_unique "
        'ON "listing" ("slug") '
        "WHERE deleted_at IS NULL"
    )


# --------------------------------------------------------------------------
# Partial / conditional UNIQUE — ALTER TABLE
# --------------------------------------------------------------------------


def test_partial_unique_alter_emits_create_unique_index(platform):
    blueprint = Blueprint(None, table="listing", platform=platform, action="alter")
    blueprint.unique(
        ["marketplace_id", "external_id"],
        name="listing_marketplace_external_unique",
        where="deleted_at IS NULL",
    )

    sql = blueprint.to_sql()
    assert sql == [
        "CREATE UNIQUE INDEX listing_marketplace_external_unique "
        'ON "listing" ("marketplace_id", "external_id") '
        "WHERE deleted_at IS NULL"
    ]


# --------------------------------------------------------------------------
# Plain UNIQUE is unchanged when ``where`` is omitted (regression pins)
# --------------------------------------------------------------------------


def test_plain_unique_create_is_byte_identical(platform):
    blueprint = Blueprint(None, table="users", platform=platform, action="create")
    blueprint.string("email", 255)
    blueprint.unique("email")

    sql = blueprint.to_sql()
    # Single statement; inline UNIQUE constraint, no separate index.
    assert len(sql) == 1
    assert "CONSTRAINT users_email_unique UNIQUE (email)" in sql[0]
    assert "CREATE UNIQUE INDEX" not in sql[0]


def test_plain_unique_alter_is_byte_identical(platform):
    blueprint = Blueprint(None, table="users", platform=platform, action="alter")
    blueprint.unique("email")

    sql = blueprint.to_sql()
    assert sql == ['ALTER TABLE "users" ADD CONSTRAINT users_email_unique UNIQUE(email)']
