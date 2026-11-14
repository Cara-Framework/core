"""One reviewed EVOLVE plan must take a deployed database all the way to its models.

Found 2026-11-14 (overnight schema-deploy c1) evolving a day-old 100k clone
(``synk_stress_scale``) to the current models. ``schema:plan --rehearse``
printed "rehearsal clean", ``schema:apply`` applied the plan, and
``schema:check`` then still reported drift that only a SECOND plan found:

* ``sales_summary_daily.zip3`` (varchar(3)) → ``postal_prefix`` string(10),
  declared with ``__renamed_from__`` (extensibility B3, US-first but never
  US-only). The plan renamed the column and stopped: the width is compared
  only for columns present under their declared name, so the renamed column
  stayed varchar(3).
* The same table's ``sales_summary_daily_values_check`` still said
  ``postal_prefix ~ '^[0-9]{3}$'`` (and ``stock_policy_not_empty_check``
  still lacked ``on_order_until``). The server-spelling probe copies the LIVE
  table with ``LIKE``, a declared CHECK naming a column the same plan adds or
  renames cannot attach there, and ``changed_checks`` skips any CHECK whose
  declared spelling is missing — silently, with no refusal and no notice.

On a database evolved once, a Canadian or UK postal prefix ("K1A", "SW1A")
is refused by the old CHECK or the old width, and the daily sales summary of
that tenant fails to write; a stock policy that only sets "on order until"
is refused by the old not-empty CHECK. Every gate the runbook asks for
(plan, rehearse, apply) said clean.
"""

from __future__ import annotations

from cara.schema import LiveSchema, plan


def _col(data_type="character varying", nullable=True, max_length=None, default=None):
    return {
        "data_type": data_type,
        "is_nullable": nullable,
        "max_length": max_length,
        "default": default,
    }


def _field(field_type, **params):
    return {"type": field_type, "params": params}


def _summary_model():
    """The shape of ``SalesSummaryDaily`` that matters here."""
    return {
        "name": "SalesSummaryDaily",
        "table": "sales_summary_daily",
        "has_fields_method": True,
        "fields": {
            "id": _field("big_increments"),
            "postal_prefix": _field("string", length=10, nullable=True),
        },
        "indexes": [],
        "renamed_from": {"postal_prefix": "zip3"},
        "checks": [
            {
                "name": "sales_summary_daily_values_check",
                "expression": (
                    "postal_prefix IS NULL OR postal_prefix ~ '^[0-9A-Z -]{1,10}$'"
                ),
            }
        ],
    }


_OLD_CHECK = "CHECK (((zip3 IS NULL) OR ((zip3)::text ~ '^[0-9]{3}$'::text)))"


def _deployed(**extra):
    """The day-old database: ``zip3`` varchar(3) under the US-only CHECK.

    ``declared_check_definitions`` is empty for the CHECK, exactly what
    ``normalize_declared_checks`` leaves when the declared body names a column
    the live table does not have yet.
    """
    table = "sales_summary_daily"
    return LiveSchema(
        tables={
            table: {
                "id": _col("bigint", False),
                "zip3": _col(max_length=3),
            }
        },
        checks={table: {"sales_summary_daily_values_check"}},
        indexes={table: set()},
        constraint_indexes={table: set()},
        constraints={table: {"sales_summary_daily_values_check"}},
        constraint_definitions={table: {"sales_summary_daily_values_check": _OLD_CHECK}},
        **extra,
    )


def test_a_renamed_column_is_also_widened_to_its_declared_length():
    operations, refusals, _ = plan([_summary_model()], _deployed())

    kinds = {(op.kind, op.key) for op in operations}
    assert ("rename_column", "sales_summary_daily.postal_prefix") in kinds
    widened = [
        op
        for op in operations
        if op.kind == "widen_column"
        and op.key.startswith("sales_summary_daily.postal_prefix")
    ]
    assert widened or refusals, (
        "the plan renames zip3 → postal_prefix and leaves it varchar(3): a 4+ "
        "character postal prefix fails to write until somebody runs a second plan"
    )
    if widened:
        rename = next(op for op in operations if op.kind == "rename_column")
        assert operations.index(rename) < operations.index(widened[0])


def test_a_check_the_probe_could_not_spell_is_never_silently_called_unchanged():
    """The live body differs from the declaration; the only reason the planner
    does not know is that the probe failed. That must surface as an operation
    (replace the body after the rename) or a refusal — never as nothing."""
    operations, refusals, notices = plan([_summary_model()], _deployed())

    replaced = [
        op
        for op in operations
        if op.key == "sales_summary_daily:sales_summary_daily_values_check"
    ]
    mentioned = [
        line
        for line in (*refusals, *notices)
        if "sales_summary_daily_values_check" in line
    ]
    assert replaced or mentioned, (
        "the US-only CHECK (zip3 ~ '^[0-9]{3}$') survives the plan untouched and "
        "nothing in the review artifact says so"
    )
    if replaced:
        rename = next(op for op in operations if op.kind == "rename_column")
        assert operations.index(rename) < operations.index(replaced[0])
