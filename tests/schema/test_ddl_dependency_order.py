"""Named-DDL creates order by dependency, not by table name.

The real failure this pins: ``Role`` declares the shared referee function
``enforce_role_tenant_scope()`` and ``Invitation`` declares a trigger that
EXECUTEs it. Both are LOCKING named-DDL creates, and ``invitation`` sorts
before ``role`` alphabetically — so the trigger ran first and the whole plan
died mid-apply with ``function enforce_role_tenant_scope() does not exist``.
Rehearsal caught it; this keeps it caught.
"""

from __future__ import annotations

from cara.schema import ADDITIVE, LOCKING, Operation, sort_operations


def _named_ddl(table, name, sql):
    return Operation(
        kind="create_index",
        table=table,
        key=f"{table}:{name}",
        forward_sql=sql,
        reverse_sql=None,
        safety=LOCKING,
        reason="named DDL declared by the model, absent from the database",
    )


def test_a_function_creates_before_the_trigger_that_executes_it():
    operations = sort_operations(
        [
            _named_ddl(
                "invitation",
                "invitation_role_scope_guard",
                "CREATE OR REPLACE TRIGGER invitation_role_scope_guard "
                "BEFORE INSERT OR UPDATE OF role_id, tenant_id ON invitation "
                "FOR EACH ROW EXECUTE FUNCTION enforce_role_tenant_scope()",
            ),
            _named_ddl(
                "role",
                "enforce_role_tenant_scope",
                "CREATE OR REPLACE FUNCTION enforce_role_tenant_scope() "
                "RETURNS trigger AS $$ BEGIN RETURN NEW; END; $$ "
                "LANGUAGE plpgsql",
            ),
        ]
    )

    assert [op.table for op in operations] == ["role", "invitation"]


def test_extensions_precede_functions_and_plain_ddl_sits_between():
    operations = sort_operations(
        [
            _named_ddl(
                "aaa_guarded",
                "aaa_guard",
                "CREATE TRIGGER aaa_guard BEFORE INSERT ON aaa_guarded "
                "FOR EACH ROW EXECUTE FUNCTION bbb_check()",
            ),
            _named_ddl(
                "mmm_table",
                "mmm_partial_unique",
                "CREATE UNIQUE INDEX mmm_partial_unique ON mmm_table (key) "
                "WHERE deleted_at IS NULL",
            ),
            _named_ddl(
                "bbb_checks",
                "bbb_check",
                "CREATE FUNCTION bbb_check() RETURNS trigger AS $$ "
                "BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql",
            ),
            _named_ddl(
                "zzz_search",
                "pg_trgm",
                "CREATE EXTENSION IF NOT EXISTS pg_trgm",
            ),
        ]
    )

    assert [op.table for op in operations] == [
        "zzz_search",
        "bbb_checks",
        "mmm_table",
        "aaa_guarded",
    ]


def test_an_extension_precedes_every_safety_class():
    """A locking extension entry (an older classification, or a DO block)
    still installs before the additive CONCURRENTLY index that uses it."""
    concurrent_index = Operation(
        kind="create_index",
        table="aaa_listing",
        key="aaa_listing:aaa_listing_sku_trgm_idx",
        forward_sql="CREATE INDEX CONCURRENTLY IF NOT EXISTS aaa_listing_sku_trgm_idx "
        "ON aaa_listing USING GIN (sku gin_trgm_ops)",
        reverse_sql=None,
        safety=ADDITIVE,
        reason="named DDL declared by the model, absent from the database",
        transactional=False,
    )
    extension = _named_ddl(
        "zzz_product", "ext_pg_trgm", "CREATE EXTENSION IF NOT EXISTS pg_trgm"
    )

    operations = sort_operations([concurrent_index, extension])

    assert [op.table for op in operations] == ["zzz_product", "aaa_listing"]


def test_an_entry_creating_both_function_and_trigger_ranks_as_a_trigger():
    """A self-contained DO-block entry needs only the stand-alone functions
    to exist first — ranking it as a trigger keeps that true."""
    operations = sort_operations(
        [
            _named_ddl(
                "aaa_bundle",
                "aaa_bundle_guard",
                "DO $$ BEGIN "
                "CREATE FUNCTION aaa_bundle_check() RETURNS trigger AS $x$ "
                "BEGIN RETURN NEW; END; $x$ LANGUAGE plpgsql; "
                "CREATE TRIGGER aaa_bundle_guard BEFORE INSERT ON aaa_bundle "
                "FOR EACH ROW EXECUTE FUNCTION aaa_bundle_check(); "
                "END $$",
            ),
            _named_ddl(
                "zzz_checks",
                "zzz_check",
                "CREATE FUNCTION zzz_check() RETURNS trigger AS $$ "
                "BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql",
            ),
        ]
    )

    assert [op.table for op in operations] == ["zzz_checks", "aaa_bundle"]
