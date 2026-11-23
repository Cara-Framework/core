"""Four defects in generated DDL and drift detection.

* Index and constraint names were emitted UNQUOTED, so a declared
  ``ordersByTenant`` was case-folded by PostgreSQL to ``ordersbytenant``,
  never matched its declaration again, and every plan re-added it.
* ``defaults_match`` read quoted literals as numbers, so on a TEXT column
  ``'01'`` equalled ``'1'`` and a changed zero-padded default stayed green.
* The reverse of a "replace CHECK / FOREIGN KEY" re-added the old body
  ``NOT VALID`` even when the forward found it validated; after a rollback
  the next plan asked to validate it again.
* ``live_type_sql`` dropped an array's element length (``varchar(20)[]``
  came back ``varchar[]``) and a bit string's length (``bit varying(8)``
  unbounded, ``bit(4)`` as ``bit`` — which is ``bit(1)``).
"""

from __future__ import annotations

from cara.schema import (
    LiveSchema,
    changed_checks,
    defaults_match,
    introspect,
    live_type_sql,
    missing_checks,
    orphaned_indexes,
)
from cara.schema.ConstraintDrift import changed_foreign_key
from cara.schema.DeclaredIndex import DeclaredIndex, quoted_name

_OLD_KIND = "CHECK ((kind = ANY (ARRAY['a'::text, 'b'::text])))"
_NEW_KIND = "CHECK ((kind = ANY (ARRAY['a'::text])))"


def _model(table="orders", **extra):
    return {
        "table": table,
        "has_fields_method": True,
        "fields": {"id": {"type": "big_increments", "params": {}}},
        "indexes": [],
        **extra,
    }


def _live(table="orders", *, checks=(), indexes=(), definitions=None, declared=None):
    return LiveSchema(
        tables={
            table: {
                "id": {"data_type": "bigint", "is_nullable": False, "max_length": None}
            }
        },
        checks={table: set(checks)},
        indexes={table: set(indexes)},
        constraint_indexes={table: set()},
        constraints={table: set(checks)},
        relation_kinds={table: "BASE TABLE"},
        constraint_definitions={table: dict(definitions or {})},
        declared_check_definitions={table: dict(declared or {})},
    )


# ── (a) names keep their case ────────────────────────────────────────────


def test_a_plain_name_reads_exactly_as_before():
    assert quoted_name("orders_tenant_idx") == "orders_tenant_idx"


def test_a_mixed_case_or_odd_name_is_quoted():
    assert quoted_name("ordersByTenant") == '"ordersByTenant"'
    assert quoted_name('we"ird') == '"we""ird"'


def test_a_mixed_case_check_is_added_and_validated_by_its_exact_name():
    model = _model(checks=[{"name": "kindIsKnown", "expression": "kind IN ('a')"}])

    add, validate = missing_checks(model, "orders", _live())

    assert 'ADD CONSTRAINT "kindIsKnown" CHECK' in add.forward_sql
    assert add.reverse_sql.endswith('DROP CONSTRAINT IF EXISTS "kindIsKnown"')
    assert validate.forward_sql.endswith('VALIDATE CONSTRAINT "kindIsKnown"')


def test_a_mixed_case_orphan_index_is_dropped_by_its_exact_name():
    live = _live(indexes=("ByTenant",))

    (drop,) = orphaned_indexes(_model(), "orders", live)

    assert drop.forward_sql == 'DROP INDEX CONCURRENTLY IF EXISTS "ByTenant"'


def test_a_mixed_case_declared_index_is_built_by_its_exact_name():
    index = DeclaredIndex("ByTenant", False, ' ("id")')

    assert index.statement("orders", concurrently=True) == (
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS "ByTenant" ON "orders" ("id")'
    )


# ── (b) text defaults compare as text ────────────────────────────────────


def test_zero_padded_text_defaults_differ():
    assert not defaults_match("'01'", "'1'::text")
    assert not defaults_match("'007'", "'7'::character varying")


def test_identical_text_defaults_match():
    assert defaults_match("'01'", "'01'::character varying")


def test_numeric_defaults_still_compare_as_values():
    assert defaults_match("-1", "'-1'::integer")
    assert defaults_match("1.0", "1")
    assert defaults_match("'5'", "5")


# ── (c) a replace's reverse restores the state it found ─────────────────


def test_reversing_a_replaced_validated_check_restores_it_validated():
    model = _model(checks=[{"name": "kind_check", "expression": "kind IN ('a')"}])
    live = _live(
        checks=("kind_check",),
        definitions={"kind_check": _OLD_KIND},
        declared={"kind_check": _NEW_KIND},
    )

    replace = next(op for op in changed_checks(model, "orders", live) if op.reverse_sql)

    assert "'b'" in replace.reverse_sql
    assert not replace.reverse_sql.endswith("NOT VALID")


def test_reversing_a_replaced_unvalidated_check_keeps_it_unvalidated():
    model = _model(checks=[{"name": "kind_check", "expression": "kind IN ('a')"}])
    live = _live(
        checks=("kind_check",),
        definitions={"kind_check": _OLD_KIND + " NOT VALID"},
        declared={"kind_check": _NEW_KIND},
    )

    replace = next(op for op in changed_checks(model, "orders", live) if op.reverse_sql)

    assert replace.reverse_sql.endswith("NOT VALID")


def _key(on_delete):
    return {
        "name": "orders_customer_fk",
        "columns": ["customer_id"],
        "references": ["id"],
        "on": "customer",
        "on_delete": on_delete,
        "on_update": None,
    }


def test_reversing_a_replaced_validated_foreign_key_restores_it_validated():
    live = _live(
        definitions={
            "orders_customer_fk": "FOREIGN KEY (customer_id) REFERENCES customer(id) "
            "ON DELETE CASCADE"
        }
    )

    replace, _validate = changed_foreign_key("orders", _key("restrict"), live)

    assert replace.reverse_sql.endswith("ON DELETE CASCADE")


def test_reversing_a_replaced_unvalidated_foreign_key_keeps_it_unvalidated():
    live = _live(
        definitions={
            "orders_customer_fk": "FOREIGN KEY (customer_id) REFERENCES customer(id) "
            "ON DELETE CASCADE NOT VALID"
        }
    )

    replace, _validate = changed_foreign_key("orders", _key("restrict"), live)

    assert replace.reverse_sql.endswith("ON DELETE CASCADE NOT VALID")


# ── (d) the live type keeps its lengths ──────────────────────────────────


def test_an_array_keeps_its_element_length():
    live = {
        "data_type": "array",
        "udt_name": "_varchar",
        "formatted_type": "character varying(20)[]",
    }
    assert live_type_sql(live) == "character varying(20)[]"


def test_an_array_read_without_the_full_spelling_falls_back_to_its_element():
    assert live_type_sql({"data_type": "array", "udt_name": "_int4"}) == '"int4"[]'


def test_bit_strings_keep_their_length():
    assert (
        live_type_sql({"data_type": "bit varying", "max_length": 8}) == "BIT VARYING(8)"
    )
    assert live_type_sql({"data_type": "bit", "max_length": 4}) == "BIT(4)"


def test_introspection_reads_the_full_spelling_of_each_column():
    class _Executor:
        def get_query_result(self, sql):
            if "information_schema.columns" not in sql:
                return []
            assert "format_type(a.atttypid, a.atttypmod)" in sql
            return [
                {
                    "table_name": "orders",
                    "column_name": "tags",
                    "data_type": "ARRAY",
                    "is_nullable": "YES",
                    "udt_name": "_varchar",
                    "formatted_type": "character varying(20)[]",
                }
            ]

    class _Schema:
        query_executor = _Executor()

        def get_schema(self):
            return "public"

    live = introspect(_Schema())

    assert live_type_sql(live.tables["orders"]["tags"]) == "character varying(20)[]"
