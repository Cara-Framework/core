"""Constraint drift the name-only diffs cannot see.

:mod:`cara.schema.Objects` asks whether a named object EXISTS. Two shapes of
drift survive that question, and both stayed invisible to ``schema:check``
and ``schema:plan`` on a real database (Synkronus dev, 2026-10-28):

* a CHECK whose body changed under an unchanged name
  (:func:`changed_checks`);
* a UNIQUE constraint no model declares any more
  (:func:`orphaned_unique_constraints`) — the orphaned-index rule exempts the
  index behind every constraint, so a unique rule a rename left behind
  enforced on every write while both gates read clean;
* a FOREIGN KEY whose rule changed under an unchanged name
  (:func:`changed_foreign_key`).
"""

from __future__ import annotations

import re

from cara.schema.LiveSchema import LiveSchema, constraint_body
from cara.schema.Objects import (
    _declared_checks,
    _foreign_key_clause,
    _validate_foreign_key,
    declared_index_names,
)
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
    so a declaration that merely reads differently is not a change, and each
    at its fixpoint, so neither does a CHECK that went through ``pg_dump``.

    Each lands as one ``ALTER TABLE`` that drops the old body and attaches the
    declared one ``NOT VALID`` in the same statement — the table is never
    without the constraint — whose reverse puts the old body back, then the
    same ``VALIDATE`` with a preflight that ``missing_checks`` uses.
    """
    live_bodies = (live.constraint_definitions or {}).get(table, {})
    declared_bodies = (live.declared_check_definitions or {}).get(table, {})
    respelled_bodies = (live.respelled_check_definitions or {}).get(table, {})
    operations: list[Operation] = []
    for name, expression in _declared_checks(model, table):
        current, wanted = live_bodies.get(name), declared_bodies.get(name)
        if not current or not wanted:
            continue
        settled = respelled_bodies.get(name) or current
        if constraint_body(settled) == constraint_body(wanted):
            if left_not_valid(live, table, name):
                operations.append(
                    _validate_check(
                        table,
                        name,
                        expression,
                        "finish a CHECK that was attached and never validated",
                        f"{table} holds rows violating {name} — repair the data "
                        "before validating the constraint",
                        "the ADD CONSTRAINT's reverse removes the constraint",
                    )
                )
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
            _validate_check(
                table,
                name,
                expression,
                "prove the existing rows satisfy the replaced CHECK",
                f"{table} holds rows the declared {name} refuses — repair "
                "the data before validating the constraint",
                "the paired replace's reverse restores the old body",
            )
        )
    return operations


def left_not_valid(live: LiveSchema, table: str, name: str) -> bool:
    """Whether a constraint was attached and never validated.

    An apply stopped between a recipe's ADD and its VALIDATE leaves exactly
    this, and by name and body it reads as finished: no plan asked for the
    VALIDATE again, so the existing rows stayed unproven for good.
    """
    current = (live.constraint_definitions or {}).get(table, {}).get(name)
    return bool(current) and constraint_body(current) != str(current).strip()


def _validate_check(
    table: str, name: str, expression: str, reason: str, failure: str, undone_by: str
) -> Operation:
    return Operation(
        kind="validate_check",
        table=table,
        key=f"{table}:{name}:validate",
        forward_sql=f'ALTER TABLE "{table}" VALIDATE CONSTRAINT {name}',
        reverse_sql=None,
        safety=LOCKING,
        reason=reason,
        preflight_sql=f'SELECT 1 FROM "{table}" WHERE NOT ({expression}) LIMIT 1',
        preflight_failure=failure,
        notes=(f"no reverse recorded: validation has nothing to undo — {undone_by}",),
    )


#: A foreign key as text: ``pg_get_constraintdef``, or a model's named DDL.
_FOREIGN_KEY_RE = re.compile(
    r"FOREIGN\s+KEY\s*\((?P<columns>[^)]*)\)\s*REFERENCES\s+(?P<target>[^(]+)"
    r"\((?P<references>[^)]*)\)(?P<rules>.*)",
    re.DOTALL | re.IGNORECASE,
)
_NAMED_FOREIGN_KEY_RE = re.compile(
    r"ADD\s+CONSTRAINT\s+\"?(?P<name>\w+)\"?\s+(?=FOREIGN\s+KEY)", re.IGNORECASE
)
#: ``SET NULL (column)`` names the columns it clears, so the list is the rule too.
_REFERENTIAL_RULE = r"(NO ACTION|RESTRICT|CASCADE|SET NULL|SET DEFAULT)(\s*\([^)]*\))?"


def _foreign_key_identifiers(text: str) -> list[str]:
    return [part.strip().strip('"') for part in text.split(",")]


def _referential_rule(event: str, rules: str) -> str | None:
    match = re.search(rf"ON\s+{event}\s+{_REFERENTIAL_RULE}", rules, re.IGNORECASE)
    if match is None:
        return None
    return f"{match.group(1)}{' ' + match.group(2).strip() if match.group(2) else ''}"


def _parsed_foreign_key(text: str) -> dict | None:
    match = _FOREIGN_KEY_RE.search(text)
    if match is None:
        return None
    rules = match.group("rules")
    return {
        "columns": _foreign_key_identifiers(match.group("columns")),
        "on": match.group("target").strip().rsplit(".", 1)[-1].strip('"'),
        "references": _foreign_key_identifiers(match.group("references")),
        "on_delete": _referential_rule("DELETE", rules),
        "on_update": _referential_rule("UPDATE", rules),
    }


def _foreign_key_shape(key: dict) -> tuple:
    """What a key asserts: columns, target, target columns, both rules."""
    return (
        tuple(key["columns"]),
        key["on"],
        tuple(key["references"]),
        str(key.get("on_delete") or "no action").upper(),
        str(key.get("on_update") or "no action").upper(),
    )


def raw_foreign_keys(model: dict) -> list[dict]:
    """The FOREIGN KEYs a model declares in ``__indexes__`` named DDL, in the
    shape of :func:`cara.schema.Objects.declared_foreign_keys`."""
    keys: list[dict] = []
    for entry in model.get("indexes") or []:
        sql = entry.get("up") or ""
        named = _NAMED_FOREIGN_KEY_RE.search(sql)
        parsed = _parsed_foreign_key(sql[named.start() :]) if named else None
        if parsed is not None:
            keys.append({"name": named.group("name"), **parsed})
    return keys


def changed_foreign_key(table: str, key: dict, live: LiveSchema) -> list[Operation]:
    """A key present by name that no longer asserts what the model declares.

    A constraint keeps its generated name when a model changes ``on_delete``,
    so a deployed database went on enforcing the old rule — cascading deletes
    the model had stopped asking for — while every gate read clean.

    One ``ALTER TABLE`` drops the old key and attaches the declared one ``NOT
    VALID``, so the table is never without it. It stays an ``add_foreign_key``:
    the declared key, as declared, is what the database does not have.
    """
    name = key["name"]
    current = (live.constraint_definitions or {}).get(table, {}).get(name)
    if not current:
        return []
    parsed = _parsed_foreign_key(current)
    if parsed is None or _foreign_key_shape(parsed) == _foreign_key_shape(key):
        return []
    body = constraint_body(current)
    return [
        Operation(
            kind="add_foreign_key",
            table=table,
            key=f"{table}:{name}",
            forward_sql=(
                f'ALTER TABLE "{table}" DROP CONSTRAINT {name}, '
                f"{_foreign_key_clause(key)}"
            ),
            reverse_sql=(
                f'ALTER TABLE "{table}" DROP CONSTRAINT IF EXISTS {name}, '
                f"ADD CONSTRAINT {name} {body} NOT VALID"
            ),
            safety=LOCKING,
            reason="FOREIGN KEY in the database asserts something the model no longer declares",
            notes=(
                f"database: {body}",
                "one statement drops the old key and attaches the declared one "
                "NOT VALID — the paired VALIDATE scans without blocking writes",
            ),
        ),
        _validate_foreign_key(
            table, key, "the paired replace's reverse restores the old key"
        ),
    ]


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
    "changed_foreign_key",
    "declared_checks",
    "declared_checks_by_table",
    "orphaned_unique_constraints",
    "raw_foreign_keys",
]
