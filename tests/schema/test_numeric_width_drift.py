"""A decimal that lost scale in the database is drift, not a detail.

`information_schema` reports numeric width in its own columns, and neither
gate read them. A live ``numeric(6,4)`` sat under a model declaring
``numeric(20,18)`` while ``schema:check`` printed "No drift" and
``schema:plan`` printed "Nothing to do" — so every write was silently rounded
to four decimals.

That became a P0 the moment ``invoice_amounts_check`` began re-deriving the
invoice total from the STORED factor: a prorated cancel-now invoice
(``total = round((base_fee + usage_amount) * proration_factor, 2)`` computed
from an 18-dp factor, then compared against the 4-dp coerced one) is rejected
with 23514. POST /api/billing/cancel-now 500s, the final invoice is never cut,
the provider cancellation that runs after it is never reached — the customer
cannot cancel while metering keeps running, and the bigger the tenant the more
often it fires.
"""

from __future__ import annotations

from cara.schema import LiveSchema, plan


def _model(precision, scale, table="invoice", column="proration_factor"):
    return {
        "table": table,
        "has_fields_method": True,
        "fields": {
            "id": {"type": "big_increments", "params": {}},
            column: {
                "type": "decimal",
                "params": {"precision": precision, "scale": scale, "nullable": False},
            },
        },
        "indexes": [],
    }


def _live(precision, scale, table="invoice", column="proration_factor"):
    return LiveSchema(
        tables={
            table: {
                "id": {"data_type": "bigint", "is_nullable": False, "max_length": None},
                column: {
                    "data_type": "numeric",
                    "is_nullable": False,
                    "max_length": None,
                    "numeric_precision": precision,
                    "numeric_scale": scale,
                },
            }
        },
        checks={},
        indexes={},
        constraint_indexes={},
        relation_kinds={table: "BASE TABLE"},
    )


def test_a_narrower_live_scale_is_planned_as_a_widening():
    operations, refusals, _ = plan([_model(20, 18)], _live(6, 4))

    assert refusals == []
    assert [op.kind for op in operations] == ["widen_column"]
    assert "TYPE NUMERIC(20,18)" in operations[0].forward_sql
    # The reverse re-rounds; it restores the shape, never the digits.
    assert "NUMERIC(6,4)" in operations[0].reverse_sql
    assert operations[0].restores_data is False


def test_a_matching_width_is_not_replanned():
    operations, refusals, _ = plan([_model(20, 18)], _live(20, 18))

    assert operations == [] and refusals == []


def test_a_wider_live_column_is_left_alone():
    """Lenient in the safe direction, exactly like the text and integer rules:
    a database column with MORE room than the model asks for loses nothing."""
    operations, refusals, _ = plan([_model(12, 2)], _live(20, 18))

    assert operations == [] and refusals == []


def test_a_widening_that_would_lose_integer_digits_is_not_derived():
    """numeric(6,4) holds 2 integer digits; numeric(20,18) holds 2 as well, so
    that one is safe. A model asking for more SCALE at the cost of integer
    room is a rewrite that can overflow existing values — the planner must not
    guess it."""
    operations, _refusals, _ = plan([_model(6, 5)], _live(6, 2))

    assert operations == []


def test_both_inspectors_read_the_numeric_width_columns():
    """The gates can only compare what they SELECT — this is where the blind
    spot actually lived."""
    from pathlib import Path

    planner_side = Path("cara/schema/LiveSchema.py").read_text()
    checker_side = Path("cara/commands/core/_LiveSchemaInspection.py").read_text()
    for source in (planner_side, checker_side):
        assert "numeric_precision, numeric_scale" in source
        assert '"numeric_scale": row.get("numeric_scale")' in source
