"""Constraint drift the name-only diffs cannot see.

:mod:`cara.schema.Objects` asks whether a named object EXISTS. Two shapes of
drift survive that question, and both stayed invisible to ``schema:check``
and ``schema:plan`` on a real database (Synkronus dev, 2026-10-28):

* a CHECK whose body changed under an unchanged name
  (:func:`changed_checks`);
* a UNIQUE constraint no model declares any more
  (:func:`orphaned_unique_constraints`) — the orphaned-index rule exempts the
  index behind every constraint, so a unique rule a rename left behind
  enforced on every write while both gates read clean.
"""

from __future__ import annotations

from cara.schema.LiveSchema import LiveSchema, constraint_body
from cara.schema.Objects import _declared_checks, declared_index_names
from cara.schema.Operation import DESTRUCTIVE, LOCKING, Operation


def declared_checks(model: dict, table: str) -> list[tuple[str, str]]:
    """``(name, expression)`` for every CHECK the model declares — the input
    :func:`cara.schema.LiveSchema.normalize_declared_checks` spells."""
    return _declared_checks(model, table)


def declared_checks_by_table(models: list[dict]) -> dict[str, list[tuple[str, str]]]:
    """Every model-owned table's declared CHECKs, keyed by table."""
    return {
        model["table"]: _declared_checks(model, model["table"])
        for model in models
        if model.get("table")
    }


def changed_checks(model: dict, table: str, live: LiveSchema) -> list[Operation]:
    """CHECKs the database has BY NAME but asserting something else.

    ``missing_checks`` asks only whether the name exists, so a model that
    narrowed a CHECK — ``catalog_proposal.kind`` losing ``'quantity_review'``
    — left the old body enforcing on a database both gates called clean, and
    ``dev:seed-check`` reading its vocabulary from that body kept asking for
    the value the model had dropped. The two bodies are compared as the
    SERVER spells them (:func:`cara.schema.LiveSchema.normalize_declared_checks`),
    so a declaration that merely reads differently is not a change.

    Each lands as one ``ALTER TABLE`` that drops the old body and attaches the
    declared one ``NOT VALID`` in the same statement — the table is never
    without the constraint — whose reverse puts the old body back, then the
    same ``VALIDATE`` with a preflight that ``missing_checks`` uses.
    """
    live_bodies = (live.constraint_definitions or {}).get(table, {})
    declared_bodies = (live.declared_check_definitions or {}).get(table, {})
    operations: list[Operation] = []
    for name, expression in _declared_checks(model, table):
        current, wanted = live_bodies.get(name), declared_bodies.get(name)
        if not current or not wanted:
            continue
        if constraint_body(current) == constraint_body(wanted):
            continue
        operations.append(
            Operation(
                kind="replace_check",
                table=table,
                key=f"{table}:{name}",
                forward_sql=(
                    f'ALTER TABLE "{table}" DROP CONSTRAINT {name}, '
                    f"ADD CONSTRAINT {name} CHECK ({expression}) NOT VALID"
                ),
                reverse_sql=(
                    f'ALTER TABLE "{table}" DROP CONSTRAINT IF EXISTS {name}, '
                    f"ADD CONSTRAINT {name} {constraint_body(current)} NOT VALID"
                ),
                safety=LOCKING,
                reason="CHECK constraint in the database asserts something the model no longer declares",
                notes=(
                    f"database: {constraint_body(current)}",
                    "one statement drops the old body and attaches the declared "
                    "one NOT VALID — the paired VALIDATE scans without blocking writes",
                ),
            )
        )
        operations.append(
            Operation(
                kind="validate_check",
                table=table,
                key=f"{table}:{name}:validate",
                forward_sql=f'ALTER TABLE "{table}" VALIDATE CONSTRAINT {name}',
                reverse_sql=None,
                safety=LOCKING,
                reason="prove the existing rows satisfy the replaced CHECK",
                preflight_sql=(
                    f'SELECT 1 FROM "{table}" WHERE NOT ({expression}) LIMIT 1'
                ),
                preflight_failure=(
                    f"{table} holds rows the declared {name} refuses — repair "
                    "the data before validating the constraint"
                ),
                notes=(
                    "no reverse recorded: validation has nothing to undo — the "
                    "paired replace's reverse restores the old body",
                ),
            )
        )
    return operations


def orphaned_unique_constraints(
    model: dict, table: str, live: LiveSchema
) -> list[Operation]:
    """UNIQUE constraints on a model-owned table that no declaration owns.

    ``orphaned_indexes`` skips every index that BACKS a constraint — dropping
    one means dropping the constraint — so a unique rule the model stopped
    declaring was invisible to both gates. A column rename left
    ``external_attribute_value_map_source_source_key_unique (source,
    source_value)`` behind its new identity key, and every import that
    reused a value under a second attribute died on it.

    Ownership is :func:`cara.schema.Objects.declared_index_names`, the one
    answer to "may this stay?", so the same over-claiming rule holds. Unlike
    an orphaned index, the reverse is recorded: the server's own definition
    rebuilds it.
    """
    declared = declared_index_names(model, table)
    definitions = (live.constraint_definitions or {}).get(table, {})
    operations: list[Operation] = []
    for name in sorted((live.unique_constraints or {}).get(table, set()) - declared):
        definition = definitions.get(name)
        operations.append(
            Operation(
                kind="drop_unique_constraint",
                table=table,
                key=f"{table}:{name}",
                forward_sql=f'ALTER TABLE "{table}" DROP CONSTRAINT {name}',
                reverse_sql=(
                    f'ALTER TABLE "{table}" ADD CONSTRAINT {name} {definition}'
                    if definition
                    else None
                ),
                safety=DESTRUCTIVE,
                reason="UNIQUE constraint in the database that no model declares",
                restores_data=False,
                notes=(f"database: {definition}",) if definition else (),
            )
        )
    return operations


__all__ = [
    "changed_checks",
    "declared_checks",
    "declared_checks_by_table",
    "orphaned_unique_constraints",
]
