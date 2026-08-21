"""Turn the model↔deployed-database difference into ordered operations.

This is evolve mode's core claim in one function: what a hand-written forward
migration would have said can be DERIVED, because the models already say what
the schema should be and the database already says what it is.

Three rules keep the derivation honest:

* **A rename is declared, never inferred.** A column that vanished from the
  models and one that appeared are indistinguishable from a diff — every
  autogenerating tool in this space emits DROP + ADD for them, which throws
  the data away. A model states ``__renamed_from__ = {"new": "old"}``, and
  only then does the planner emit ``RENAME COLUMN``. Undeclared, it refuses
  to guess: the DROP is reported as destructive and the ADD as additive, and
  a human decides.

* **Nothing is invented.** A type with no entry in the vocabulary's
  ``POSTGRES_TYPE_SQL``, a NOT NULL column with no default on a populated
  table, a raw ``__indexes__`` column — each produces a REFUSAL carrying the
  reason, not a best-effort statement. A plan that quietly guesses is the
  thing this replaces.

* **Adding a NOT NULL column is not additive.** On an empty table it is
  harmless; on a populated one Postgres must rewrite every row, and without a
  default it cannot even do that. The planner emits the safe three-step
  recipe (add nullable → backfill → set not null) and classes it ``locking``,
  because that is what it is.
"""

from __future__ import annotations

from cara.exceptions import SchemaPlanRefused
from cara.schema.LiveSchema import LiveSchema, declared_columns
from cara.schema.Objects import (
    missing_checks,
    missing_indexes,
    orphaned_checks,
    orphaned_indexes,
    orphaned_tables,
)
from cara.schema.Operation import (
    ADDITIVE,
    DESTRUCTIVE,
    LOCKING,
    RUN_MIGRATION_PREFIX,
    Operation,
    sort_operations,
)
from cara.schema.Vocabulary import (
    DB_INT_RANK,
    DB_TYPE_CATEGORY,
    MODEL_INT_RANK,
    MODEL_TYPE_CATEGORY,
    postgres_type,
)

#: Text-family model types with no length bound — widening a bounded live
#: column to one of these is always safe for the data.
_UNBOUNDED_TEXT_TYPES = frozenset({"text", "tiny_text", "long_text"})


def plan(
    models: list[dict], live: LiveSchema
) -> tuple[list[Operation], list[str], list[str]]:
    """Operations to move ``live`` to what ``models`` declare, plus findings.

    Returns ``(operations, refusals, notices)``, three deliberately different
    things:

    * **operations** — what apply will run.
    * **refusals** — a difference with no derivable statement. A non-empty
      list means the plan is INCOMPLETE, and apply must not run it as if it
      were the whole story, which is why these are returned rather than logged.
    * **notices** — something a human should see that is NOT an operation.
      An orphaned TABLE is the case that forced this category: the planner
      cannot tell an abandoned table from a partition child, an extension's
      table or the migration tracker itself, so emitting a DROP would be
      guessing at exactly the scale where guessing is unforgivable. It says
      what it found and stops.
    """
    operations: list[Operation] = []
    refusals: list[str] = []
    notices: list[str] = []

    planned = [
        model
        for model in models
        if model.get("has_fields_method") and model.get("table") and model.get("fields")
    ]

    for model in sorted(planned, key=lambda m: m["table"]):
        table = model["table"]
        declared = declared_columns(model)
        if not declared:
            continue

        live_columns = live.tables.get(table)
        if live_columns is None:
            # A table absent from the database is created by RUNNING its
            # generated migration, not by a CREATE TABLE rebuilt here — that
            # would be a second renderer of the same model, and the two would
            # drift. ``forward_sql`` names the file so apply can execute it and
            # the ledger records which artifact ran.
            operations.append(
                Operation(
                    kind="create_table",
                    table=table,
                    key=table,
                    forward_sql=f"{RUN_MIGRATION_PREFIX}create_{table}_table",
                    reverse_sql=f'DROP TABLE IF EXISTS "{table}"',
                    safety=ADDITIVE,
                    reason="table declared by a model but absent from the database",
                    notes=(
                        "runs the generated create_<table>_table migration; its "
                        "own DDL, not a re-render",
                    ),
                )
            )
            continue

        renames = _declared_renames(model)
        for new_name, old_name in sorted(renames.items()):
            if old_name in live_columns and new_name not in live_columns:
                operations.append(
                    Operation(
                        kind="rename_column",
                        table=table,
                        key=f"{table}.{new_name}",
                        forward_sql=(
                            f'ALTER TABLE "{table}" '
                            f'RENAME COLUMN "{old_name}" TO "{new_name}"'
                        ),
                        reverse_sql=(
                            f'ALTER TABLE "{table}" '
                            f'RENAME COLUMN "{new_name}" TO "{old_name}"'
                        ),
                        safety=ADDITIVE,
                        reason=(
                            f"model declares __renamed_from__: {old_name} → {new_name}"
                        ),
                    )
                )

        renamed_away = {old for new, old in renames.items() if old in live_columns}

        for name in sorted(set(declared) - set(live_columns) - set(renames)):
            try:
                operations.extend(_add_column(table, name, declared[name]))
            except SchemaPlanRefused as refusal:
                refusals.append(f"{table}.{name}: {refusal}")

        for name in sorted(set(live_columns) - set(declared) - renamed_away):
            operations.append(
                Operation(
                    kind="drop_column",
                    table=table,
                    key=f"{table}.{name}",
                    forward_sql=f'ALTER TABLE "{table}" DROP COLUMN "{name}"',
                    # Structurally reversible, semantically not: re-adding the
                    # column brings back an empty column, never its values.
                    reverse_sql=(
                        f'ALTER TABLE "{table}" ADD COLUMN "{name}" '
                        f"{_live_type_sql(live_columns[name])}"
                    ),
                    safety=DESTRUCTIVE,
                    reason="column present in the database, absent from the model",
                    restores_data=False,
                    notes=(
                        "if this is a rename, declare __renamed_from__ on the model "
                        "and re-plan — the rename keeps the data",
                    ),
                )
            )

        for name in sorted(set(declared) & set(live_columns)):
            try:
                operations.extend(
                    _alter_column(table, name, declared[name], live_columns[name])
                )
            except SchemaPlanRefused as refusal:
                refusals.append(f"{table}.{name}: {refusal}")

        try:
            operations.extend(missing_indexes(model, table, live))
        except SchemaPlanRefused as refusal:
            refusals.append(str(refusal))
        operations.extend(orphaned_indexes(model, table, live))
        operations.extend(missing_checks(model, table, live))
        operations.extend(orphaned_checks(model, table, live))

    notices.extend(orphaned_tables({m["table"] for m in planned}, live))

    return sort_operations(operations), refusals, notices


# ── column operations ───────────────────────────────────────────────────────


def _add_column(table: str, name: str, declared: dict) -> list[Operation]:
    if declared["type"] == "__raw__":
        raise SchemaPlanRefused(
            "declared through raw __indexes__ SQL — apply that entry's own "
            "statement rather than a generated ADD COLUMN"
        )

    type_sql = postgres_type(declared["type"], declared.get("params"))
    if type_sql is None:
        raise SchemaPlanRefused(
            f"no PostgreSQL mapping for model type '{declared['type']}'"
        )

    quoted = f'ALTER TABLE "{table}" ADD COLUMN "{name}" {type_sql}'
    reverse = f'ALTER TABLE "{table}" DROP COLUMN "{name}"'

    if declared["nullable"]:
        return [
            Operation(
                kind="add_column",
                table=table,
                key=f"{table}.{name}",
                forward_sql=f"{quoted} NULL",
                reverse_sql=reverse,
                safety=ADDITIVE,
                reason="nullable column declared by the model",
                # The column comes back empty; anything written into it since
                # the add is gone.
                restores_data=False,
            )
        ]

    params = declared.get("params") or {}
    default = params.get("default")
    backfill = params.get("backfill_from")
    if default is None and backfill is None:
        raise SchemaPlanRefused(
            "NOT NULL with no default and no backfill_from — an existing table "
            "has rows that would violate it. Declare .backfill_from(<sql>) on "
            "the field when the value comes from the row itself, give it a "
            "constant default, or add it nullable and tighten in a later "
            "deploy (expand/contract)"
        )

    # Add nullable → backfill → tighten. Postgres can fill a constant default
    # in place, but the three-step form is what stays safe on a large table and
    # is the same shape a reviewer already knows from expand/contract.
    #
    # ``backfill_from`` wins over ``default`` because it is the more specific
    # statement: a default says what a NEW row gets, a backfill says what the
    # EXISTING ones get, and when a model bothers to declare both it is
    # because those two answers differ.
    fill = backfill if backfill is not None else _sql_default(default)
    literal = _sql_default(default) if default is not None else "NULL"
    return [
        Operation(
            kind="add_column",
            table=table,
            key=f"{table}.{name}",
            forward_sql=f"{quoted} NULL",
            reverse_sql=reverse,
            safety=ADDITIVE,
            reason="step 1/3 of a NOT NULL addition: add it nullable",
            restores_data=False,
        ),
        Operation(
            kind="backfill_column",
            table=table,
            key=f"{table}.{name}:backfill",
            forward_sql=(
                f'UPDATE "{table}" SET "{name}" = {fill} WHERE "{name}" IS NULL'
            ),
            reverse_sql=None,
            safety=LOCKING,
            reason=(
                "step 2/3: fill existing rows from the declared backfill"
                if backfill is not None
                else "step 2/3: fill existing rows with the declared default"
            ),
            restores_data=False,
            notes=("rewrites every existing row; batch it by hand on a large table",),
        ),
        Operation(
            kind="set_not_null",
            table=table,
            key=f"{table}.{name}:not_null",
            forward_sql=(
                (
                    f'ALTER TABLE "{table}" ALTER COLUMN "{name}" '
                    f"SET DEFAULT {literal}, "
                    f'ALTER COLUMN "{name}" SET NOT NULL'
                )
                if default is not None
                else f'ALTER TABLE "{table}" ALTER COLUMN "{name}" SET NOT NULL'
            ),
            reverse_sql=f'ALTER TABLE "{table}" ALTER COLUMN "{name}" DROP NOT NULL',
            safety=LOCKING,
            reason="step 3/3: tighten to NOT NULL",
            preflight_sql=(f'SELECT 1 FROM "{table}" WHERE "{name}" IS NULL LIMIT 1'),
            preflight_failure=(
                f"{table}.{name} still has NULL rows — the backfill above did "
                f"not cover them"
            ),
            notes=("takes an ACCESS EXCLUSIVE lock for a full scan",),
        ),
    ]


def _alter_column(table: str, name: str, declared: dict, live: dict) -> list[Operation]:
    """Only the differences that are safe to state as one statement."""
    if declared["type"] == "__raw__" or declared["nullable"] is None:
        return []

    operations: list[Operation] = []

    if declared["nullable"] and not live["is_nullable"]:
        operations.append(
            Operation(
                kind="drop_not_null",
                table=table,
                key=f"{table}.{name}:null",
                forward_sql=(
                    f'ALTER TABLE "{table}" ALTER COLUMN "{name}" DROP NOT NULL'
                ),
                reverse_sql=f'ALTER TABLE "{table}" ALTER COLUMN "{name}" SET NOT NULL',
                safety=ADDITIVE,
                reason="model relaxed the column to nullable",
            )
        )
    elif not declared["nullable"] and live["is_nullable"]:
        operations.append(
            Operation(
                kind="set_not_null",
                table=table,
                key=f"{table}.{name}:not_null",
                forward_sql=f'ALTER TABLE "{table}" ALTER COLUMN "{name}" SET NOT NULL',
                reverse_sql=(
                    f'ALTER TABLE "{table}" ALTER COLUMN "{name}" DROP NOT NULL'
                ),
                safety=LOCKING,
                reason="model tightened the column to NOT NULL",
                preflight_sql=(f'SELECT 1 FROM "{table}" WHERE "{name}" IS NULL LIMIT 1'),
                preflight_failure=(
                    f"{table}.{name} still has NULL rows — SET NOT NULL will "
                    f"fail. Backfill them first, in its own deploy"
                ),
                notes=("takes an ACCESS EXCLUSIVE lock for a full scan",),
            )
        )

    model_category = MODEL_TYPE_CATEGORY.get(declared["type"])
    db_category = DB_TYPE_CATEGORY.get(live["data_type"])

    # WIDENING only. Every other type change is a rewrite whose correctness
    # depends on the data, and guessing a USING clause is exactly the kind of
    # help that corrupts a column.
    if model_category == db_category == "integer":
        model_rank = MODEL_INT_RANK.get(declared["type"])
        db_rank = DB_INT_RANK.get(live["data_type"])
        if model_rank and db_rank and model_rank > db_rank:
            type_sql = postgres_type(declared["type"], declared.get("params"))
            operations.append(
                Operation(
                    kind="widen_column",
                    table=table,
                    key=f"{table}.{name}:type",
                    forward_sql=(
                        f'ALTER TABLE "{table}" ALTER COLUMN "{name}" TYPE {type_sql}'
                    ),
                    # Narrowing back can fail on values that now exceed the
                    # old range, so the reverse is offered but not promised.
                    reverse_sql=(
                        f'ALTER TABLE "{table}" ALTER COLUMN "{name}" TYPE '
                        f"{live['data_type'].upper()}"
                    ),
                    safety=LOCKING,
                    reason=f"model widened {live['data_type']} → {declared['type']}",
                    restores_data=False,
                    notes=("rewrites the table; the reverse fails on out-of-range rows",),
                )
            )
    elif model_category == db_category == "text":
        # The bounded→bounded widening (varchar(n) → varchar(m>n)) is derived
        # by the ``:length`` block below. This branch covers the case that
        # block cannot: the model dropping the bound entirely (``text`` over
        # a live varchar(n)) — the shape a ``__renamed_from__`` rename leaves
        # behind when the split column widened its type. Narrowing stays the
        # operator's decision, mirroring the integer rule: ``schema:check``
        # reports it, and deriving a lossy statement would be worse than
        # deriving nothing.
        widens = (
            declared["type"] in _UNBOUNDED_TEXT_TYPES
            and live.get("max_length") is not None
        )
        if widens:
            type_sql = postgres_type(declared["type"], declared.get("params"))
            operations.append(
                Operation(
                    kind="widen_column",
                    table=table,
                    key=f"{table}.{name}:type",
                    forward_sql=(
                        f'ALTER TABLE "{table}" ALTER COLUMN "{name}" TYPE {type_sql}'
                    ),
                    # Narrowing back can fail on values that now exceed the
                    # old bound, so the reverse is offered but not promised.
                    reverse_sql=(
                        f'ALTER TABLE "{table}" ALTER COLUMN "{name}" TYPE '
                        f"{_live_type_sql(live)}"
                    ),
                    safety=LOCKING,
                    reason=(
                        f"model widened {_live_type_sql(live)} → "
                        f"{type_sql} — the live bound rejects writes the "
                        "model allows"
                    ),
                    restores_data=False,
                    notes=(
                        "varchar widening is metadata-only in Postgres — a "
                        "brief ACCESS EXCLUSIVE lock, no rewrite; the reverse "
                        "fails on rows longer than the old bound",
                    ),
                )
            )
    elif model_category == db_category == "numeric":
        # A model that widened a decimal (more scale for a proration factor,
        # more precision for money) must reach the deployed column, or every
        # write is silently rounded — and where a CHECK re-derives a value
        # from the stored column, the row is rejected outright. Widening only,
        # mirroring the integer rule: narrowing loses digits and stays the
        # operator's decision.
        params = declared.get("params") or {}
        declared_precision = params.get("precision")
        declared_scale = params.get("scale")
        live_precision = live.get("numeric_precision")
        live_scale = live.get("numeric_scale")
        widens = (
            declared_precision is not None
            and declared_scale is not None
            and live_precision is not None
            and live_scale is not None
            and (
                int(declared_precision) > int(live_precision)
                or int(declared_scale) > int(live_scale)
            )
            and int(declared_precision) - int(declared_scale)
            >= int(live_precision) - int(live_scale)
        )
        if widens:
            type_sql = postgres_type(declared["type"], params)
            operations.append(
                Operation(
                    kind="widen_column",
                    table=table,
                    key=f"{table}.{name}:type",
                    forward_sql=(
                        f'ALTER TABLE "{table}" ALTER COLUMN "{name}" TYPE {type_sql}'
                    ),
                    reverse_sql=(
                        f'ALTER TABLE "{table}" ALTER COLUMN "{name}" TYPE '
                        f"NUMERIC({live_precision},{live_scale})"
                    ),
                    safety=LOCKING,
                    reason=(
                        f"model widened numeric({live_precision},{live_scale}) → "
                        f"{type_sql} — the live column silently rounds every write"
                    ),
                    restores_data=False,
                    notes=(
                        "rewrites the table; the reverse re-rounds every value "
                        "and cannot restore the digits it drops",
                    ),
                )
            )
    elif model_category and db_category and model_category != db_category:
        raise SchemaPlanRefused(
            f"type change {live['data_type']} → {declared['type']} is a rewrite "
            "whose USING clause depends on the data; write it as an explicit "
            "expand/contract instead"
        )

    if model_category == db_category == "text":
        live_max = live.get("max_length")
        declared_len = declared.get("length")
        if (
            live_max is not None
            and declared_len is not None
            and int(declared_len) > int(live_max)
        ):
            operations.append(
                Operation(
                    kind="widen_column",
                    table=table,
                    key=f"{table}.{name}:length",
                    forward_sql=(
                        f'ALTER TABLE "{table}" ALTER COLUMN "{name}" TYPE '
                        f"VARCHAR({declared_len})"
                    ),
                    reverse_sql=(
                        f'ALTER TABLE "{table}" ALTER COLUMN "{name}" TYPE '
                        f"VARCHAR({live_max})"
                    ),
                    safety=LOCKING,
                    reason=(
                        f"model widened varchar({live_max}) → varchar({declared_len})"
                    ),
                    restores_data=False,
                    notes=("the reverse truncates and will fail on longer values",),
                )
            )

    return operations


# ── helpers ─────────────────────────────────────────────────────────────────


def _declared_renames(model: dict) -> dict[str, str]:
    renames = model.get("renamed_from") or {}
    return {str(new): str(old) for new, old in renames.items()}


def _live_type_sql(live: dict) -> str:
    base = (live.get("data_type") or "text").upper()
    max_length = live.get("max_length")
    if max_length and base in {"CHARACTER VARYING", "VARCHAR", "CHARACTER", "CHAR"}:
        return f"VARCHAR({max_length})"
    return base


def _sql_default(value) -> str:
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


__all__ = ["plan"]
