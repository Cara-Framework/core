"""Index drift the name-only diffs cannot see.

:mod:`cara.schema.Objects` asks whether a named index EXISTS. Two shapes of
drift survive that question:

* an index whose DEFINITION changed under an unchanged name — its columns or
  their order, UNIQUE, a partial predicate, the method, an opclass, INCLUDE
  (:func:`drifted_indexes`). ``message_thread_assignee_idx`` sat on
  ``(tenant_id, assigned_to_user_id)`` under a model declaring the reverse
  order while ``schema:check`` said "No drift" and ``schema:plan`` "Nothing
  to do" (Synkronus dev, 2026-11-03) — and ``schema:verify``, which IS
  ``schema:check`` against a from-zero database, inherited the blindness;
* an INVALID index, what an interrupted ``CREATE INDEX CONCURRENTLY`` leaves:
  present by name, maintained on every write, read by no query.

Both sides are compared as the SERVER spells them. The live side is
``pg_get_indexdef``; the declared side is the model's own statement built on
a session TEMP clone of the table and read back the same way
(:func:`cara.schema.LiveSchema.normalize_declared_indexes`). A blueprint
``field.index([...])`` and a raw ``__indexes__`` statement therefore compare
by one exact rule, and a declaration that merely READS differently
(``LOWER(x)`` against the stored ``lower((x)::text)``) is not a change.

A changed index is planned as a RESHAPE that never leaves the table without
it: the declared definition is built CONCURRENTLY under a temporary name next
to the live index, then one short transaction drops the old one and gives the
new one its name (``ADD CONSTRAINT ... USING INDEX`` does both for a UNIQUE
constraint). The temporary name carries a digest of the declared shape, so an
apply interrupted after the build resumes from the built index, and a build
left over from a superseded declaration is never the one swapped in.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from cara.exceptions import SchemaPlanRefused
from cara.schema.DeclaredIndex import (
    _MAX_IDENTIFIER_LENGTH,
    DeclaredIndex,
    _unique_index_preflight,
    declared_indexes,
    index_shape,
)
from cara.schema.LiveSchema import LiveSchema
from cara.schema.Operation import ADDITIVE, LOCKING, Operation


def reshape_index_name(name: str, shape: str) -> str:
    """The temporary name a reshape builds the declared ``shape`` under.

    Derived from the shape, not fixed: a build left behind for a declaration
    that has since changed again has a DIFFERENT name, so it can never be the
    index swapped in. Truncated so Postgres stores the name it is given.
    """
    digest = hashlib.sha256(shape.encode()).hexdigest()[:8]
    return f"{name[: _MAX_IDENTIFIER_LENGTH - 12]}_rs_{digest}"


@dataclass(frozen=True)
class IndexDrift:
    """A declared index the database has by name and not as declared."""

    index: DeclaredIndex
    live_definition: str
    #: The server's spelling of the declaration; None when the probe could
    #: not build it — then the definition is never called changed.
    wanted_definition: str | None
    invalid: bool
    #: The live definition re-spelled by the probe — the fixpoint the
    #: declaration is compared at (``normalize_declared_indexes``). The
    #: stored text stays ``live_definition``: it is what a reverse rebuilds.
    live_respelled: str | None = None

    @property
    def live_shape(self) -> str | None:
        return index_shape(self.live_respelled or self.live_definition)

    @property
    def wanted_shape(self) -> str | None:
        return index_shape(self.wanted_definition)

    @property
    def changed(self) -> bool:
        live, wanted = self.live_shape, self.wanted_shape
        return live is not None and wanted is not None and live != wanted

    @property
    def scaffold(self) -> str | None:
        """The reshape's temporary index name, when there is a reshape."""
        wanted = self.wanted_shape
        return (
            reshape_index_name(self.index.name, wanted)
            if self.changed and wanted
            else None
        )


def drifted_indexes(model: dict, table: str, live: LiveSchema) -> list[IndexDrift]:
    """Declared indexes present by name whose definition differs, or INVALID.

    The one answer both gates read: ``schema:check`` reports these and
    ``schema:plan`` reshapes them (:func:`changed_indexes`), so the two can
    never disagree about whether an index is what the model says.
    """
    live_definitions = (live.index_definitions or {}).get(table, {})
    wanted = (live.declared_index_definitions or {}).get(table, {})
    respelled = (live.respelled_index_definitions or {}).get(table, {})
    invalid = (live.invalid_indexes or {}).get(table, set())
    drifts: list[IndexDrift] = []
    for index in declared_indexes(model, table):
        current = live_definitions.get(index.name)
        if current is None:
            continue  # missing, not drifted — ``missing_indexes`` plans it
        drift = IndexDrift(
            index,
            current,
            wanted.get(index.name),
            index.name in invalid,
            respelled.get(index.name),
        )
        if drift.changed or drift.invalid:
            drifts.append(drift)
    return drifts


def reshape_scaffolding(model: dict, table: str, live: LiveSchema) -> set[str]:
    """Temporary reshape indexes that belong to a reshape still to finish.

    The orphaned-index rule must not plan dropping these: an apply stopped
    between the build and the swap left one, and the re-derived plan swaps it
    in rather than building it again.
    """
    return {
        drift.scaffold
        for drift in drifted_indexes(model, table, live)
        if drift.scaffold is not None
    }


def _foreign_key_preflight(name: str) -> tuple[str, str]:
    """A foreign key leaning on a unique index blocks dropping it."""
    return (
        "SELECT 1 FROM pg_constraint WHERE contype = 'f' "
        f"AND conindid = to_regclass('{name}') LIMIT 1",
        f"foreign keys reference {name} — dropping it would need them dropped "
        "too; re-point them by hand before the reshape",
    )


def _swap(
    table: str,
    drift: IndexDrift,
    temp: str,
    *,
    live_constraint: bool,
    constraint_definition: str | None,
) -> Operation:
    """Drop the old index and give the built one its name, in one transaction."""
    name, old = drift.index.name, drift.live_definition
    if drift.index.constraint and live_constraint:
        forward = (
            f'ALTER TABLE "{table}" DROP CONSTRAINT {name}, '
            f"ADD CONSTRAINT {name} UNIQUE USING INDEX {temp}"
        )
        reverse = (
            f'ALTER TABLE "{table}" DROP CONSTRAINT {name}, '
            f"ADD CONSTRAINT {name} {constraint_definition}"
            if constraint_definition
            else None
        )
    elif drift.index.constraint:
        forward = (
            f"DO $$ BEGIN DROP INDEX {name}; "
            f'ALTER TABLE "{table}" ADD CONSTRAINT {name} UNIQUE USING INDEX {temp}; END $$'
        )
        reverse = (
            f'DO $$ BEGIN ALTER TABLE "{table}" DROP CONSTRAINT {name}; {old}; END $$'
        )
    elif live_constraint:
        forward = (
            f'DO $$ BEGIN ALTER TABLE "{table}" DROP CONSTRAINT {name}; '
            f"ALTER INDEX {temp} RENAME TO {name}; END $$"
        )
        reverse = (
            f"DO $$ BEGIN ALTER INDEX {name} RENAME TO {temp}; "
            f'ALTER TABLE "{table}" ADD CONSTRAINT {name} {constraint_definition}; END $$'
            if constraint_definition
            else None
        )
    else:
        forward = (
            f"DO $$ BEGIN DROP INDEX {name}; ALTER INDEX {temp} RENAME TO {name}; END $$"
        )
        reverse = f"DO $$ BEGIN ALTER INDEX {name} RENAME TO {temp}; {old}; END $$"

    preflight_sql, preflight_failure = (
        _foreign_key_preflight(name)
        if (drift.live_shape or "").startswith("UNIQUE ")
        else (None, None)
    )
    return Operation(
        kind="swap_index",
        table=table,
        key=f"{table}:{name}:swap",
        forward_sql=forward,
        reverse_sql=reverse,
        safety=LOCKING,
        reason=f"swap the CONCURRENTLY-built {temp} in for {name}",
        preflight_sql=preflight_sql,
        preflight_failure=preflight_failure,
        notes=(
            "one transaction: the old index goes and the built one takes its "
            "name — the table is never without it; brief ACCESS EXCLUSIVE "
            "lock, no scan",
            "the reverse rebuilds the old definition with a plain, "
            "write-blocking CREATE INDEX",
        ),
    )


def _rebuild(
    table: str, name: str, preflight: tuple[str | None, str | None]
) -> Operation:
    """``REINDEX ... CONCURRENTLY`` for an INVALID index already declared right."""
    return Operation(
        kind="rebuild_index",
        table=table,
        key=f"{table}:{name}:rebuild",
        forward_sql=f"REINDEX INDEX CONCURRENTLY {name}",
        reverse_sql=None,
        safety=ADDITIVE,
        reason=(
            "index is INVALID — an interrupted CONCURRENTLY build: maintained "
            "on every write, read by no query"
        ),
        transactional=False,
        preflight_sql=preflight[0],
        preflight_failure=preflight[1],
        notes=(
            "rebuilt CONCURRENTLY in place, with the definition it already has",
            "no reverse recorded: an invalid index has nothing a rollback could restore",
        ),
    )


def changed_indexes(model: dict, table: str, live: LiveSchema) -> list[Operation]:
    """Reshape every drifted index; rebuild every INVALID one in place.

    A changed definition becomes two operations, both keyed on the index:

    * ``reshape_index`` — the declared definition built ``CONCURRENTLY``
      under :func:`reshape_index_name`, next to the live index, which keeps
      serving queries. A UNIQUE build carries the same duplicate-key
      preflight :func:`cara.schema.Objects.missing_indexes` gives a new one.
      Already built by an interrupted apply, it is skipped; left INVALID by
      one, it is finished with ``REINDEX ... CONCURRENTLY``.
    * ``swap_index`` — one transaction that drops the old index and gives the
      built one its name. Its reverse is the database's own definition of the
      old index, so a rollback restores it exactly.

    An INVALID index whose definition is the declared one needs no reshape:
    ``REINDEX INDEX CONCURRENTLY`` rebuilds it under its own name.
    """
    live_definitions = (live.index_definitions or {}).get(table, {})
    invalid = (live.invalid_indexes or {}).get(table, set())
    unique_constraints = (live.unique_constraints or {}).get(table, set())
    backing = (live.constraint_indexes or {}).get(table, set())
    constraint_definitions = (live.constraint_definitions or {}).get(table, {})

    operations: list[Operation] = []
    for drift in drifted_indexes(model, table, live):
        index = drift.index
        name = index.name
        preflight = (
            _unique_index_preflight(index.statement(table), table, name)
            if index.unique
            else (None, None)
        )
        if not drift.changed:
            operations.append(_rebuild(table, name, preflight))
            continue

        if name in backing and name not in unique_constraints:
            raise SchemaPlanRefused(
                f"{table}.{name} backs a constraint that is not a UNIQUE one "
                f"(a primary key, an EXCLUDE) and its definition differs from "
                f"the model's — reshape it by hand"
            )

        temp = drift.scaffold
        build = None
        if temp in invalid:
            build = f"REINDEX INDEX CONCURRENTLY {temp}"
        elif temp not in live_definitions:
            build = index.statement(table, temp, concurrently=True)
        if build is not None:
            operations.append(
                Operation(
                    kind="reshape_index",
                    table=table,
                    key=f"{table}:{name}:reshape",
                    forward_sql=build,
                    reverse_sql=f"DROP INDEX CONCURRENTLY IF EXISTS {temp}",
                    safety=ADDITIVE,
                    reason=(
                        f"index {name} in the database is not what the model "
                        f"declares — build the declared one beside it"
                    ),
                    transactional=False,
                    preflight_sql=preflight[0],
                    preflight_failure=preflight[1],
                    notes=(
                        f"database: {drift.live_shape}",
                        f"model:    {drift.wanted_shape}",
                        "built CONCURRENTLY next to the live index, which keeps "
                        "serving queries until the swap",
                    ),
                )
            )
        operations.append(
            _swap(
                table,
                drift,
                temp,
                live_constraint=name in unique_constraints,
                constraint_definition=constraint_definitions.get(name),
            )
        )
    return operations


__all__ = [
    "IndexDrift",
    "changed_indexes",
    "drifted_indexes",
    "reshape_index_name",
    "reshape_scaffolding",
]
