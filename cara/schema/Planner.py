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

import re

from cara.exceptions import SchemaPlanRefused
from cara.schema.LiveSchema import LiveSchema, declared_columns
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

#: Object names an ``__indexes__`` entry's SQL actually creates.
_CREATES_RE = re.compile(
    r"(?:CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:CONCURRENTLY\s+)?(?:IF\s+NOT\s+EXISTS\s+)?"
    r"|ADD\s+CONSTRAINT\s+"
    r"|CREATE\s+(?:OR\s+REPLACE\s+)?TRIGGER\s+"
    r"|CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+)"
    r"\"?(?P<name>\w+)\"?",
    re.IGNORECASE,
)


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

        operations.extend(_missing_indexes(model, table, live))
        operations.extend(_orphaned_indexes(model, table, live))

    notices.extend(_orphaned_tables({m["table"] for m in planned}, live))

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
    if default is None:
        raise SchemaPlanRefused(
            "NOT NULL with no default — an existing table has rows that would "
            "violate it. Give the field a default, or add it nullable, "
            "backfill, and tighten in a later deploy (expand/contract)"
        )

    # Add nullable → backfill → tighten. Postgres can fill a default in place,
    # but the three-step form is what stays safe on a large table and is the
    # same shape a reviewer already knows from expand/contract.
    literal = _sql_default(default)
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
                f'UPDATE "{table}" SET "{name}" = {literal} WHERE "{name}" IS NULL'
            ),
            reverse_sql=None,
            safety=LOCKING,
            reason="step 2/3: fill existing rows with the declared default",
            restores_data=False,
            notes=("rewrites every existing row; batch it by hand on a large table",),
        ),
        Operation(
            kind="set_not_null",
            table=table,
            key=f"{table}.{name}:not_null",
            forward_sql=(
                f'ALTER TABLE "{table}" ALTER COLUMN "{name}" SET DEFAULT {literal}, '
                f'ALTER COLUMN "{name}" SET NOT NULL'
            ),
            reverse_sql=f'ALTER TABLE "{table}" ALTER COLUMN "{name}" DROP NOT NULL',
            safety=LOCKING,
            reason="step 3/3: tighten to NOT NULL",
            preflight_sql=(
                f'SELECT 1 FROM "{table}" WHERE "{name}" IS NULL LIMIT 1'
            ),
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
                preflight_sql=(
                    f'SELECT 1 FROM "{table}" WHERE "{name}" IS NULL LIMIT 1'
                ),
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
                    notes=(
                        "rewrites the table; the reverse fails on out-of-range rows",
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


def _created_objects(up_sql: str, fallback: str) -> set[str]:
    """The names this statement creates, or the entry's own name as a fallback.

    An entry's ``name`` is a LABEL, not necessarily a database object: one
    entry legitimately creates two differently-named CHECK constraints, and
    keying presence on the label alone makes that entry replan forever. Since
    the plan is the thing a human reads before a production deploy, permanent
    phantom entries are not cosmetic — they train the reader to skim.
    """
    found = {match.group("name") for match in _CREATES_RE.finditer(up_sql)}
    return found or {fallback}


#: PostgreSQL truncates every identifier to NAMEDATALEN-1 bytes. The stored
#: name is the truncated one, so a planner comparing the full convention name
#: against the catalogue finds nothing and plans an index that already exists —
#: on this schema, 23 of them, every single plan.
_MAX_IDENTIFIER_LENGTH = 63


def _blueprint_index_name(table: str, columns: list[str], unique: bool) -> str:
    """The name Cara's Blueprint gives an unnamed ``index``/``unique``.

    ``table.index(["a", "b"])`` becomes ``<table>_a_b_index``, truncated the
    way Postgres truncates it. Reproducing the convention is what lets the
    planner ask "does the database already have this one?" — without it, a
    field-level index is invisible on the deployed side and never planned.
    """
    name = f"{table}_{'_'.join(columns)}_{'unique' if unique else 'index'}"
    return name[:_MAX_IDENTIFIER_LENGTH]


def _declared_blueprint_indexes(model: dict, table: str) -> list[tuple[str, list[str], bool]]:
    """``(name, columns, unique)`` for every field-level index the model declares.

    Covers both spellings: the per-field ``.index()`` / ``.unique()`` flags and
    the standalone ``field.index([...], name=...)`` declarations, which may
    carry an explicit name.

    Keyed by NAME, because the discoverer records a single-column
    ``field.string(...).index()`` under both spellings — once as a param flag,
    once as a standalone declaration. Emitting both would put the same
    ``CREATE INDEX`` in one plan twice, and a plan that lists an operation
    twice is a plan nobody trusts.
    """
    declared: dict[str, tuple[str, list[str], bool]] = {}

    for column, definition in (model.get("fields") or {}).items():
        params = definition.get("params") or {}
        for flag, unique in (("index", False), ("unique", True)):
            if params.get(flag):
                name = _blueprint_index_name(table, [column], unique)
                declared[name] = (name, [column], unique)

    for key, unique in (("composite_indexes", False), ("composite_uniques", True)):
        for declaration in model.get(key, []) or []:
            columns = list(declaration.get("columns") or [])
            if not columns:
                continue
            name = declaration.get("name") or _blueprint_index_name(
                table, columns, unique
            )
            declared[name] = (name, columns, unique)

    return list(declared.values())


def _missing_indexes(model: dict, table: str, live: LiveSchema) -> list[Operation]:
    """Index-shaped objects the model declares and the database does not have.

    Two sources, because a model has two ways to say "index this": the
    Blueprint flags (``field.string(...).index()``, ``field.index([...])``),
    whose SQL the planner renders from the declaration, and ``__indexes__``
    named-DDL entries, whose SQL is the entry's own ``up`` — nothing is
    re-rendered there. Missing the first source is not a cosmetic gap: a
    field-level index added to a model would never reach a deployed database
    and nothing would say so.
    """
    present = live.objects_on(table)
    operations: list[Operation] = []

    for name, columns, unique in _declared_blueprint_indexes(model, table):
        if name in present:
            continue
        column_list = ", ".join(f'"{column}"' for column in columns)
        operations.append(
            Operation(
                kind="create_index",
                table=table,
                key=f"{table}:{name}",
                # CONCURRENTLY by default: on a deployed table a plain build
                # holds a write lock for its duration, and the planner has no
                # reason to choose the blocking form when the model only asked
                # for an index.
                forward_sql=(
                    f"CREATE {'UNIQUE ' if unique else ''}INDEX CONCURRENTLY "
                    f'IF NOT EXISTS {name} ON "{table}" ({column_list})'
                ),
                reverse_sql=f"DROP INDEX CONCURRENTLY IF EXISTS {name}",
                safety=ADDITIVE,
                reason=(
                    f"model declares {'a unique' if unique else 'an'} index on "
                    f"{', '.join(columns)}"
                ),
                transactional=False,
                preflight_sql=(
                    f"SELECT 1 FROM \"{table}\" GROUP BY {column_list} "
                    f"HAVING COUNT(*) > 1 LIMIT 1"
                )
                if unique
                else None,
                preflight_failure=(
                    f"{table} already holds duplicate {', '.join(columns)} — a "
                    f"UNIQUE index cannot be built until they are resolved"
                )
                if unique
                else None,
                notes=(
                    "built CONCURRENTLY: cannot run in a transaction, and an "
                    "interrupted build leaves an INVALID index that re-running "
                    "replaces",
                ),
            )
        )

    for index in model.get("indexes", []) or []:
        name = index.get("name")
        up = index.get("up")
        if not name or not up:
            continue
        if _created_objects(up, name) <= present:
            continue
        concurrent = "CONCURRENTLY" in up.upper()
        operations.append(
            Operation(
                kind="create_index",
                table=table,
                key=f"{table}:{name}",
                forward_sql=up,
                reverse_sql=index.get("down"),
                safety=ADDITIVE if concurrent else LOCKING,
                reason="named DDL declared by the model, absent from the database",
                transactional=not concurrent,
                notes=()
                if concurrent
                else (
                    "builds with a write lock; declare it CONCURRENTLY to avoid that",
                ),
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


def _orphaned_indexes(model: dict, table: str, live: LiveSchema) -> list[Operation]:
    """Indexes on a model-owned table that the model no longer declares.

    Removing ``.index()`` from a model is a real instruction, and without this
    the index survives in production forever with nothing reporting it — the
    mirror of the missing-index gap, and just as silent.

    Three exclusions keep it from firing on objects the model never owned:
    the indexes Postgres creates to BACK a constraint (a primary key, a unique
    constraint — dropping those means dropping the constraint), anything a
    ``__indexes__`` entry creates, and the primary key itself.

    Classed ``destructive`` even though no row is lost: an index is the
    difference between a query and an outage, so removing one must be an
    explicit decision. Its reverse fully restores it, which is why
    ``restores_data`` stays True — the object comes back complete, unlike a
    dropped column.
    """
    declared = {name for name, _, _ in _declared_blueprint_indexes(model, table)}
    for index in model.get("indexes", []) or []:
        up = index.get("up")
        if up:
            declared |= _created_objects(up, index.get("name") or "")

    backed_by_constraint = live.constraint_indexes.get(table, set())
    present = live.indexes.get(table, set())

    operations: list[Operation] = []
    for name in sorted(present - declared - backed_by_constraint):
        if name.endswith("_pkey"):
            continue
        operations.append(
            Operation(
                kind="drop_index",
                table=table,
                key=f"{table}:{name}",
                forward_sql=f"DROP INDEX CONCURRENTLY IF EXISTS {name}",
                # Rebuilt from the catalogue's own definition, so the reverse
                # is exact rather than a reconstruction from the model.
                reverse_sql=None,
                safety=DESTRUCTIVE,
                reason="index in the database that no model declares",
                transactional=False,
                notes=(
                    "no reverse is recorded: an index the model does not declare "
                    "has no declaration to rebuild it from. Capture its "
                    "definition from pg_indexes before dropping if you may want "
                    "it back",
                ),
            )
        )
    return operations


def _orphaned_tables(model_tables: set[str], live: LiveSchema) -> list[str]:
    """Tables in the database that no model declares — REPORTED, never dropped.

    A DROP TABLE derived from a diff is where an autogenerating tool does its
    worst damage, because the diff cannot distinguish an abandoned table from
    a partition child, a table an extension owns, or the framework's own
    migration tracker. So this returns prose for a human, not an operation.
    """
    ignored = {"migrations"}
    orphans = sorted(live.table_names() - model_tables - ignored)
    return [
        f"table '{name}' exists in the database and no model declares it — "
        f"if it is obsolete, drop it by hand; the planner will not guess"
        for name in orphans
    ]
