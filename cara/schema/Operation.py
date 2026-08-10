"""A schema change as a typed, classified, reversible operation.

Evolve mode's whole claim is that a production schema change can be DERIVED
from the models instead of hand-written, and still be safe to run. That claim
rests on three properties per operation, which is why they are fields here
rather than prose in a runbook:

* **Safety class.** ``additive`` touches nothing that exists. ``locking``
  takes a lock that can stall a live table and must ship as its safe recipe.
  ``destructive`` removes data. A planner that emitted one undifferentiated
  list of SQL would be no safer than a hand-written migration; the class is
  what lets apply refuse by default and a reviewer read the risk at a glance.

* **Reverse SQL.** Recorded at PLAN time, not derived later from a schema that
  has already moved. This is what makes a rollback possible at all — and its
  absence (``reverse_sql is None``) is how an operation states honestly that
  it cannot be undone.

* **Reversibility is not the same as losslessness.** Dropping a column added
  yesterday is structurally reversible — ``ADD COLUMN`` brings it back — and
  the values written into it are gone forever. Operations therefore carry
  ``restores_data``: False means "the shape returns, the contents do not". A
  rollback that pretended otherwise would be worse than no rollback.

And one property the safety class CANNOT supply: **will this statement
actually succeed against the rows that are in there right now?** Classifying
``SET NOT NULL`` as locking says what it costs, not whether it works — on a
column with one NULL row it fails outright, halfway through a deploy, after
the operations before it already applied. Same for a UNIQUE index over a
column that has duplicates. So an operation may carry ``preflight_sql``: a
read-only query that must return NO rows for the statement to be safe. It is
answered against production itself, because the question is about production's
data, and a structure-only rehearsal — no rows — would answer it wrong.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field

#: Applies to a table without touching existing rows or blocking readers and
#: writers: a new table, a NULLable column, an index built CONCURRENTLY.
ADDITIVE = "additive"

#: Correct, but takes a lock that can stall a busy table for the duration of a
#: rewrite or a full scan. Must be shipped as its safe recipe (``NOT VALID``
#: then ``VALIDATE``, batched backfills) rather than as the naive statement.
LOCKING = "locking"

#: Removes a column, a table or capacity. Never applied without an explicit
#: flag naming the object, because no amount of review makes it recoverable.
DESTRUCTIVE = "destructive"

SAFETY_ORDER = {ADDITIVE: 0, LOCKING: 1, DESTRUCTIVE: 2}

#: The applied-operations ledger. Named here rather than imported from
#: ``cara.models`` because this module must stay importable without the ``db``
#: extra; :func:`sort_operations` needs the name, not the model. Kept honest by
#: a test asserting it equals ``SchemaOperation.__table__``.
LEDGER_TABLE = "schema_operation"

#: ``forward_sql`` prefix meaning "this is not SQL — run the named generated
#: migration". A new table is created by executing its own generated creator,
#: never by a CREATE TABLE re-rendered from the model: one renderer, one truth.
#: The prefix keeps that instruction inside the same field the ledger already
#: records, so what ran is auditable without a second column.
RUN_MIGRATION_PREFIX = "run-migration:"


def migration_to_run(forward_sql: str) -> str | None:
    """The migration slug ``forward_sql`` asks for, or None if it is SQL."""
    if forward_sql.startswith(RUN_MIGRATION_PREFIX):
        return forward_sql[len(RUN_MIGRATION_PREFIX) :]
    return None


@dataclass(frozen=True)
class Operation:
    """One statement, with everything a reviewer and a rollback need.

    ``key`` is the stable identity of the change (``table.column`` or
    ``table:index``) — the ledger stores it so a re-planned, re-applied
    operation is recognised as the one already recorded rather than run twice.
    """

    kind: str
    table: str
    key: str
    forward_sql: str
    reverse_sql: str | None
    safety: str
    reason: str
    restores_data: bool = True
    #: Statements that must NOT run inside a transaction (``CREATE INDEX
    #: CONCURRENTLY``). Apply runs these outside the surrounding transaction
    #: and can therefore leave an INVALID index behind if interrupted — which
    #: is recoverable by re-running, and is the trade Postgres offers for not
    #: locking the table.
    transactional: bool = True
    #: A read-only query that must return NO rows for this operation to
    #: succeed. ``None`` means the statement cannot fail on data.
    preflight_sql: str | None = None
    #: What a returned row MEANS, in the words the operator needs to act on.
    preflight_failure: str | None = None
    notes: tuple[str, ...] = field(default_factory=tuple)

    @property
    def reversible(self) -> bool:
        return self.reverse_sql is not None

    def describe(self) -> str:
        marks = []
        if not self.transactional:
            marks.append("non-transactional")
        if not self.reversible:
            marks.append("irreversible")
        elif not self.restores_data:
            marks.append("shape-only rollback")
        suffix = f"  [{', '.join(marks)}]" if marks else ""
        return f"{self.safety:11} {self.kind:16} {self.key}{suffix}"


def as_dict(operation: Operation) -> dict:
    """An operation as plain JSON data, for a plan artifact.

    Every field is carried, including the reverse statement and the preflight
    query: a reviewer reading the artifact in a pull request must see exactly
    what a reviewer reading the terminal saw, or the artifact is a summary
    pretending to be a plan.
    """
    return {
        "kind": operation.kind,
        "table": operation.table,
        "key": operation.key,
        "forward_sql": operation.forward_sql,
        "reverse_sql": operation.reverse_sql,
        "safety": operation.safety,
        "reason": operation.reason,
        "restores_data": operation.restores_data,
        "transactional": operation.transactional,
        "preflight_sql": operation.preflight_sql,
        "preflight_failure": operation.preflight_failure,
        "notes": list(operation.notes),
    }


def from_dict(data: dict) -> Operation:
    """The inverse of :func:`as_dict`, for reading a plan artifact back."""
    return Operation(
        kind=data["kind"],
        table=data["table"],
        key=data["key"],
        forward_sql=data["forward_sql"],
        reverse_sql=data.get("reverse_sql"),
        safety=data["safety"],
        reason=data["reason"],
        restores_data=data.get("restores_data", True),
        transactional=data.get("transactional", True),
        preflight_sql=data.get("preflight_sql"),
        preflight_failure=data.get("preflight_failure"),
        notes=tuple(data.get("notes") or ()),
    )


def plan_id(operations: list[Operation]) -> str:
    """A stable id for this exact set of operations.

    Content-derived on purpose. It is the ledger's grouping key, so re-running
    the same plan resumes the same rows instead of opening a second,
    half-empty record of the same work — and it is what lets apply tell "this
    artifact still describes the database" from "the database moved".
    """
    digest = hashlib.sha256()
    for operation in operations:
        digest.update(operation.key.encode())
        digest.update(operation.forward_sql.encode())
    return digest.hexdigest()[:16]


def sort_operations(operations: list[Operation]) -> list[Operation]:
    """Ledger first, then safest first, then stable by table and key.

    Ordering by safety is not cosmetic: a plan that a reviewer stops reading
    halfway through should have shown them the harmless work first and left
    the risky work adjacent to the flag that gates it.

    :data:`LEDGER_TABLE` jumping the queue is a real dependency, not a
    preference. On the FIRST evolve run against a database that predates the
    ledger — a production database on cutover day, the one case that matters —
    the ledger is itself a table the plan creates, and every operation is
    recorded the moment it succeeds. Any operation ordered ahead of it would
    run and then fail to record, which is the worst possible failure for this
    command: a schema that moved with no entry saying so. Sorted first, the
    ledger's own creation becomes its first row.
    """
    return sorted(
        operations,
        key=lambda op: (
            op.table != LEDGER_TABLE,
            SAFETY_ORDER.get(op.safety, 9),
            op.kind != "create_table",
            op.table,
            op.key,
        ),
    )


__all__ = [
    "ADDITIVE",
    "LEDGER_TABLE",
    "as_dict",
    "from_dict",
    "plan_id",
    "RUN_MIGRATION_PREFIX",
    "DESTRUCTIVE",
    "LOCKING",
    "Operation",
    "SAFETY_ORDER",
    "migration_to_run",
    "sort_operations",
]
