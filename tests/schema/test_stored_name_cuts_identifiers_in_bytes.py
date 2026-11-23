"""Every declared name is compared as PostgreSQL stores it: cut at 63 BYTES.

Live defects (``stored_name`` now in ``cara/schema/DeclaredIndex.py``):

1. Names were cut at 63 CHARACTERS. PostgreSQL cuts at 63 bytes
   (``pg_mbcliplen``), dropping a multi-byte character that straddles the
   limit, so a declared name with a non-ASCII character near the end never
   matched its stored twin.
2. Only some routes truncated at all. Raw ``CREATE INDEX`` names, explicit
   composite names, CHECK / FOREIGN KEY names read from named DDL
   (``ConstraintDrift``), function / trigger names (``NamedDdlBodies``) and
   ``reshape_index_name`` compared or emitted the full name, so a declared
   name over 63 bytes showed as drift against its truncated twin on every
   plan: re-added, and the live one dropped as an orphan.
"""

from __future__ import annotations

from cara.schema import (
    LiveSchema,
    created_objects,
    declared_indexes,
    missing_indexes,
    orphaned_indexes,
    reshape_index_name,
    stored_name,
)
from cara.schema.ConstraintDrift import raw_checks, raw_foreign_keys
from cara.schema.NamedDdlBodies import declared_routines
from cara.schema.Objects import _declared_checks, declared_foreign_keys

#: 62 ASCII bytes and then two-byte characters: byte 63 is the FIRST byte
#: of an ``é``, so the server keeps the 62 ASCII bytes and drops the ``é``.
LONG = "x" * 62 + "éé"
STORED = "x" * 62
TABLE = "orders"


def test_stored_name_cuts_at_63_bytes_dropping_a_straddling_character() -> None:
    assert stored_name(LONG) == STORED
    assert len(stored_name("é" * 40).encode()) == 62  # 31 whole characters
    assert stored_name("a" * 70) == "a" * 63


def test_stored_name_leaves_a_short_name_alone() -> None:
    assert stored_name("orders_tenant_id_index") == "orders_tenant_id_index"
    assert stored_name("x" * 63) == "x" * 63


def test_created_objects_reports_the_stored_index_name() -> None:
    assert created_objects(f'CREATE INDEX "{LONG}" ON orders (a)') == {STORED}


def test_blueprint_index_names_are_stored_names() -> None:
    column = "c" * 60
    model = {"fields": {column: {"type": "string", "params": {"index": True}}}}

    (index,) = declared_indexes(model, TABLE)

    assert index.name == stored_name(f"{TABLE}_{column}_index")
    assert len(index.name.encode()) == 63


def test_explicit_composite_index_and_unique_names_are_stored_names() -> None:
    model = {
        "composite_indexes": [{"columns": ["a", "b"], "name": LONG}],
        "composite_uniques": [{"columns": ["c", "d"], "name": "u" * 62 + "üü"}],
    }

    names = {index.name for index in declared_indexes(model, TABLE)}

    assert names == {STORED, "u" * 62}


def test_raw_create_index_names_are_stored_names() -> None:
    model = {"indexes": [{"up": f'CREATE INDEX "{LONG}" ON "orders" (a)'}]}

    (index,) = declared_indexes(model, TABLE)

    assert index.name == STORED


def test_check_names_are_stored_names() -> None:
    model = {
        "checks": [{"name": LONG, "expression": "a > 0"}],
        "indexes": [{"up": f'ALTER TABLE orders ADD CONSTRAINT "{LONG}" CHECK (b > 0)'}],
    }

    assert [name for name, _ in _declared_checks(model, TABLE)] == [STORED]
    assert [name for name, _ in raw_checks(model)] == [STORED]


def test_foreign_key_names_are_stored_names() -> None:
    model = {
        "fields": {
            "customer_id": {
                "type": "foreign",
                "params": {},
                "foreign_key": {"references": "id", "on": "customers", "name": LONG},
            }
        },
        "composite_foreign_keys": [
            {"columns": ["a", "b"], "on": "parents", "references": ["a", "b"]}
        ],
        "indexes": [
            {
                "up": (
                    f'ALTER TABLE orders ADD CONSTRAINT "{LONG}" FOREIGN KEY '
                    "(customer_id) REFERENCES customers (id)"
                )
            }
        ],
    }

    assert [key["name"] for key in raw_foreign_keys(model)] == [STORED]
    long_column = "a" * 60
    model["composite_foreign_keys"][0]["columns"] = [long_column, "b"]
    assert [key["name"] for key in declared_foreign_keys(model, TABLE)] == [
        STORED,
        stored_name(f"{TABLE}_{long_column}_b_foreign"),
    ]
    assert len(declared_foreign_keys(model, TABLE)[1]["name"].encode()) == 63


def test_function_and_trigger_names_are_stored_names() -> None:
    up = (
        f'CREATE OR REPLACE FUNCTION "{LONG}"() RETURNS trigger AS $$ '
        "BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql; "
        f'CREATE TRIGGER "{LONG}" BEFORE UPDATE ON orders '
        f'FOR EACH ROW EXECUTE FUNCTION "{LONG}"();'
    )

    (routine,) = declared_routines({"indexes": [{"name": "audit", "up": up}]})

    assert [name for name, _body, _lang in routine["functions"]] == [STORED]
    assert [name for name, _statement in routine["triggers"]] == [STORED]


def test_reshape_index_name_is_stored_as_given() -> None:
    temp = reshape_index_name("x" * 50 + "é" * 10, "(a, b)")

    assert len(temp.encode()) <= 63
    assert stored_name(temp) == temp, "the server would store a different name"


def _live(index_name: str) -> LiveSchema:
    return LiveSchema(
        tables={TABLE: {"a": {"data_type": "text", "is_nullable": True}}},
        checks={},
        indexes={TABLE: {index_name}},
        constraint_indexes={TABLE: set()},
        constraints={TABLE: set()},
        relation_kinds={TABLE: "BASE TABLE"},
        constraint_definitions={TABLE: {}},
        unique_constraints={TABLE: set()},
        index_definitions={
            TABLE: {index_name: f'CREATE INDEX "{index_name}" ON public.orders (a, b)'}
        },
        invalid_indexes={TABLE: set()},
        declared_index_definitions={TABLE: {}},
    )


def test_a_long_declared_index_is_not_drift_against_its_truncated_twin() -> None:
    model = {
        "table": TABLE,
        "fields": {},
        "indexes": [],
        "composite_indexes": [{"columns": ["a", "b"], "name": LONG}],
        "composite_uniques": [],
    }
    live = _live(STORED)

    assert missing_indexes(model, TABLE, live) == []
    assert orphaned_indexes(model, TABLE, live) == []
