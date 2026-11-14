"""Evolve-mode defects a release would hit on a database it cannot reset.

1. **New tables are created alphabetically, not in reference order.** A
   ``create_table`` operation runs the table's generated migration, and that
   migration declares its FOREIGN KEYs inline (``table.foreign(...).on(...)``).
   The migration directory is numbered in dependency order, but the planner
   sorts creates by table name. A release that adds a child table whose name
   sorts before its new parent (``auth_notice_delivery`` -> ``users``) dies on
   ``relation "users" does not exist`` — and because the order is derived, the
   re-run plan dies the same way every time: the deploy cannot finish.

2. **Dropping a column cannot be rolled back to the shape it had.** The
   reverse of ``drop_column`` is rebuilt from ``information_schema.data_type``
   alone, so ``numeric(12,2) NOT NULL DEFAULT 0`` comes back as unbounded
   ``NUMERIC``, nullable, with no default, although the live read already
   collects ``numeric_precision``, ``numeric_scale`` and ``column_default``.

3. **``drop_not_null`` claims a lossless reverse.** Its ``SET NOT NULL``
   reverse fails once a NULL is written, yet ``restores_data=True`` lets it
   past the rollback gate, so ``schema:rollback`` stops half done.
"""

from __future__ import annotations

from cara.schema import LiveSchema, plan


def _empty_live() -> LiveSchema:
    return LiveSchema(tables={}, checks={}, indexes={}, constraint_indexes={})


def _table_model(table: str, fields: dict) -> dict:
    return {
        "name": table.title(),
        "table": table,
        "has_fields_method": True,
        "fields": {"id": {"type": "big_increments", "params": {}}, **fields},
        "indexes": [],
        "renamed_from": {},
    }


def test_a_new_child_table_is_created_after_the_new_parent_it_references():
    parent = _table_model("users", {})
    child = _table_model(
        "auth_notice_delivery",
        {
            "subject_user_id": {
                "type": "unsigned_big_integer",
                "params": {},
                "foreign_key": {"on": "users", "references": "id"},
            }
        },
    )

    operations, refusals, _ = plan([child, parent], _empty_live())

    assert refusals == []
    creates = [op.table for op in operations if op.kind == "create_table"]
    assert creates.index("users") < creates.index("auth_notice_delivery"), creates


def test_a_composite_tenant_key_also_orders_the_creates():
    parent = _table_model("tenant", {})
    child = _table_model(
        "channel",
        {"tenant_id": {"type": "unsigned_big_integer", "params": {}}},
    )
    child["composite_foreign_keys"] = [
        {"columns": ["tenant_id"], "references": ["id"], "on": "tenant"}
    ]

    operations, _, _ = plan([child, parent], _empty_live())

    creates = [op.table for op in operations if op.kind == "create_table"]
    assert creates.index("tenant") < creates.index("channel"), creates


def _live_with(column: dict) -> LiveSchema:
    return LiveSchema(
        tables={
            "order_line": {
                "id": {"data_type": "bigint", "is_nullable": False, "max_length": None},
                "dropped": column,
            }
        },
        checks={"order_line": set()},
        indexes={"order_line": set()},
        constraint_indexes={"order_line": set()},
    )


def _drop_reverse(column: dict) -> str:
    operations, _, _ = plan([_table_model("order_line", {})], _live_with(column))
    (drop,) = [op for op in operations if op.kind == "drop_column"]
    return drop.reverse_sql


def test_rolling_back_a_dropped_money_column_restores_its_width_and_default():
    reverse = _drop_reverse(
        {
            "data_type": "numeric",
            "is_nullable": False,
            "max_length": None,
            "numeric_precision": 12,
            "numeric_scale": 2,
            "default": "0",
        }
    )

    assert "NUMERIC(12,2)" in reverse.replace(" ", "").upper(), reverse
    assert "DEFAULT 0" in reverse.upper(), reverse
    # With a default every existing row gets a value, so NOT NULL is landable.
    assert "NOT NULL" in reverse.upper(), reverse


def test_relaxing_not_null_is_not_called_losslessly_reversible():
    """``DROP NOT NULL`` reverses to ``SET NOT NULL`` with no preflight.

    Once the relaxed column has taken a NULL row, the reverse fails partway
    through ``schema:rollback`` (each reverse is its own transaction), which
    leaves the schema half reversed. The rollback gate only stops rows marked
    ``restores_data=False`` or irreversible, so this one sails through it.
    """
    live = LiveSchema(
        tables={
            "order_line": {
                "id": {"data_type": "bigint", "is_nullable": False, "max_length": None},
                "note": {
                    "data_type": "character varying",
                    "is_nullable": False,
                    "max_length": 255,
                    "default": None,
                },
            }
        },
        checks={"order_line": set()},
        indexes={"order_line": set()},
        constraint_indexes={"order_line": set()},
    )
    model = _table_model(
        "order_line",
        {"note": {"type": "string", "params": {"length": 255, "nullable": True}}},
    )

    operations, _, _ = plan([model], live)

    (relax,) = [op for op in operations if op.kind == "drop_not_null"]
    assert relax.restores_data is False, relax
