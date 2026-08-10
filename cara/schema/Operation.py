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
"""

from __future__ import annotations

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


def sort_operations(operations: list[Operation]) -> list[Operation]:
    """Safest first, then stable by table and key.

    Ordering by safety is not cosmetic: a plan that a reviewer stops reading
    halfway through should have shown them the harmless work first and left
    the risky work adjacent to the flag that gates it.
    """
    return sorted(
        operations,
        key=lambda op: (
            SAFETY_ORDER.get(op.safety, 9),
            op.kind != "create_table",
            op.table,
            op.key,
        ),
    )


__all__ = [
    "ADDITIVE",
    "DESTRUCTIVE",
    "LOCKING",
    "Operation",
    "SAFETY_ORDER",
    "sort_operations",
]
