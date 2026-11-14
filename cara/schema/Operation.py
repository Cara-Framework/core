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
import re
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

#: The ledger's status vocabulary, next to the table it belongs to. A row is
#: written ``STATUS_APPLIED`` when its statement succeeded and ``STATUS_FAILED``
#: when it did not; ``schema:rollback`` moves a reversed row to
#: ``STATUS_REVERSED``. ``SchemaOperation`` reads these rather than restating
#: them, and so do the two commands, which talk to the ledger in raw SQL.
STATUS_APPLIED = "applied"
STATUS_FAILED = "failed"
STATUS_REVERSED = "reversed"

#: Session advisory lock that serializes ``schema:apply`` and
#: ``schema:rollback`` on one database. Two runs of one plan queued on a table
#: lock: the loser died on "column already exists" and recorded the winner's
#: work as failed. A plain number because advisory locks are per database.
SCHEMA_CHANGE_LOCK = 7236418205

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
    #: The integer key a backfill walks. Apply runs ``forward_sql`` in short
    #: transactions over slices of this column instead of one UPDATE, so a
    #: writer waits for one slice, never for the table. ``None`` runs it whole.
    batch_column: str | None = None

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
        "batch_column": operation.batch_column,
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
        batch_column=data.get("batch_column"),
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


_CREATES_TRIGGER = re.compile(
    r"\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:CONSTRAINT\s+)?TRIGGER\b", re.IGNORECASE
)
_CREATES_FUNCTION = re.compile(
    r"\bCREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\b", re.IGNORECASE
)
_CREATES_EXTENSION = re.compile(r"\bCREATE\s+EXTENSION\b", re.IGNORECASE)

#: Kinds that only PROVE existing rows satisfy a constraint already attached.
#: They have no reverse because there is nothing to undo: the paired ADD (or
#: replace) carries the reverse that removes the constraint.
_VALIDATES = frozenset({"validate_check", "validate_foreign_key"})


def is_validation(kind: str | None, key: str | None = None) -> bool:
    """Whether a planned or recorded operation is a VALIDATE step."""
    return kind in _VALIDATES or bool(key and key.endswith(":validate"))


_SET_NOT_NULL_RE = re.compile(
    r'^ALTER TABLE "(?P<table>\w+)" ALTER COLUMN "(?P<column>\w+)" SET NOT NULL$'
)
_NARROW_VARCHAR_RE = re.compile(
    r'^ALTER TABLE "(?P<table>\w+)" ALTER COLUMN "(?P<column>\w+)" '
    r"TYPE (?P<kind>VARCHAR|CHAR)\((?P<length>\d+)\)$"
)
_NARROW_TYPE_RE = re.compile(
    r'^ALTER TABLE "(?P<table>\w+)" ALTER COLUMN "(?P<column>\w+)" '
    r"TYPE (?P<type>INTEGER|SMALLINT|NUMERIC\((?P<precision>\d+),(?P<scale>\d+)\))$"
)
#: The range a narrowed integer column must still hold.
_INTEGER_BOUNDS = {"INTEGER": 2**31, "SMALLINT": 2**15}


def reverse_preflight(reverse_sql: str | None) -> tuple[str, str] | None:
    """``(query, failure)`` that must return no rows for a reverse to succeed.

    Rollback replays each reverse in its own transaction, so a reverse that
    fails on data (a ``SET NOT NULL`` over a NULL written since, a varchar
    narrowed under a longer value) stops it half done. Asked of every reverse
    before the first one runs, the rollback either completes or never starts.

    Every narrowing a widening's reverse can be is covered, not only varchar:
    ``bigint → integer`` over an order id past 2^31, and ``numeric(14,2) →
    numeric(12,2)`` over an amount past 10^10, fail the same way; with only
    varchar asked, ``--force`` sent such a rollback in and it stopped halfway.
    """
    statement = (reverse_sql or "").strip()
    match = _NARROW_TYPE_RE.match(statement)
    if match:
        table, column, kind = match["table"], match["column"], match["type"]
        if kind in _INTEGER_BOUNDS:
            bound = _INTEGER_BOUNDS[kind]
            condition = f'"{column}" NOT BETWEEN {-bound} AND {bound - 1}'
            limit = f"the {kind.lower()} range"
        else:
            digits = int(match["precision"]) - int(match["scale"])
            condition = (
                f'abs(round("{column}"::numeric, {int(match["scale"])})) '
                f">= 10::numeric ^ {digits}"
            )
            limit = f"{kind.lower()} ({digits} integer digits)"
        return (
            f'SELECT 1 FROM "{table}" WHERE {condition} LIMIT 1',
            f"{table}.{column} holds values outside {limit} — the reverse "
            "narrowing would fail",
        )
    match = _SET_NOT_NULL_RE.match(statement)
    if match:
        table, column = match["table"], match["column"]
        return (
            f'SELECT 1 FROM "{table}" WHERE "{column}" IS NULL LIMIT 1',
            f"{table}.{column} has NULL rows written since the deploy — the "
            "reverse SET NOT NULL would fail",
        )
    match = _NARROW_VARCHAR_RE.match(statement)
    if match:
        table, column, length = match["table"], match["column"], match["length"]
        # Postgres drops only trailing SPACES past the bound, so they never fail.
        return (
            f'SELECT 1 FROM "{table}" WHERE length(rtrim("{column}"::text, \' \')) '
            f"> {length} LIMIT 1",
            f"{table}.{column} holds values longer than {length} — the reverse "
            "narrowing would fail",
        )
    return None


def _ddl_precedence(operation: Operation) -> int:
    """Dependency rank for named-DDL creates inside one safety class.

    A trigger EXECUTEs a function, and the function may be declared by a
    DIFFERENT model (one referee function, several guarded tables), so
    alphabetical table order can put ``CREATE TRIGGER`` ahead of the
    ``CREATE FUNCTION`` it needs and fail mid-apply. Extensions come first
    (anything may use them), then functions, then everything else, with
    triggers last. An entry whose SQL creates both a function and a trigger
    ranks as a trigger: it carries its own function and only needs the
    stand-alone functions to exist before it.

    A new column precedes a stand-alone function in the additive class, where
    a function now sorts: a ``LANGUAGE sql`` body is validated at creation and
    may read that column. A batched backfill comes after the triggers, so a
    trigger that guards the column sees every row the backfill writes.

    A VALIDATE comes after the batched backfill. A model that adds a column
    and a CHECK over it (``ai_credit_entry.period_start`` with
    ``ai_credit_entry_spend_period_check``) fills the existing rows first;
    validated before the fill, the CHECK refused every one of them.
    """
    if operation.kind == "add_column":
        return 0
    if operation.kind in _VALIDATES:
        return 5
    if operation.batch_column:
        return 4
    sql = operation.forward_sql
    if _CREATES_TRIGGER.search(sql):
        return 3
    if _CREATES_FUNCTION.search(sql):
        return 1
    if _CREATES_EXTENSION.search(sql):
        return 0
    return 2


def sort_operations(
    operations: list[Operation],
    references: dict[str, set[str]] | None = None,
) -> list[Operation]:
    """Ledger first, then safest first, then stable by table and key.

    ``references`` maps a table to the tables its FOREIGN KEYs point at
    (single-column and composite). A ``create_table`` runs the table's
    generated migration, which declares those keys inline, so a new child
    must be created after a new parent. Sorting creates by name alone put
    ``auth_notice_delivery`` ahead of ``users`` and the apply died on
    ``relation "users" does not exist`` — on every re-run, because the order
    is derived. The creates keep their slots; only their order among
    themselves follows the references.

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

    Within one safety class, :func:`_ddl_precedence` orders named-DDL creates
    by dependency (extension → function → other → trigger) before the stable
    table/key tiebreak.

    On one table a ``drop_column`` runs after every other drop: ``DROP
    COLUMN`` takes the CHECKs and indexes involving that column with it, so a
    ``DROP CONSTRAINT`` ordered after it would fail on an object already gone
    and stop the apply partway. The key tiebreak alone put it first
    (``table.col`` sorts before ``table:name``).

    An extension create jumps every safety class, right after the ledger. A
    ``CREATE INDEX CONCURRENTLY ... gin_trgm_ops`` is additive and sorted
    ahead of anything locking, so with the extension ranked only inside its
    own class the index ran first and the apply died on ``operator class
    gin_trgm_ops does not exist``.
    """
    ordered = sorted(
        operations,
        key=lambda op: (
            op.table != LEDGER_TABLE,
            not _CREATES_EXTENSION.search(op.forward_sql),
            SAFETY_ORDER.get(op.safety, 9),
            op.kind != "create_table",
            _ddl_precedence(op),
            op.table,
            op.kind == "drop_column",
            op.key,
        ),
    )
    slots = [index for index, op in enumerate(ordered) if op.kind == "create_table"]
    for slot, create in zip(
        slots,
        _referenced_first([ordered[i] for i in slots], references or {}),
        strict=True,
    ):
        ordered[slot] = create
    return ordered


def _referenced_first(
    creates: list[Operation], references: dict[str, set[str]]
) -> list[Operation]:
    """``creates`` with every referenced new table before its referrers.

    Kahn's algorithm, always taking the earliest ready create so the result
    stays the name order wherever the references allow it. A cycle among new
    tables cannot be created by inline keys in ANY order; its members keep
    their name order and the migration's own error names the cycle.
    """
    pending = list(creates)
    created = {op.table for op in creates}
    result: list[Operation] = []
    while pending:
        placed = {op.table for op in result}
        ready = next(
            (
                op
                for op in pending
                if not ((references.get(op.table, set()) & created) - placed - {op.table})
            ),
            pending[0],
        )
        pending.remove(ready)
        result.append(ready)
    return result


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
    "SCHEMA_CHANGE_LOCK",
    "STATUS_APPLIED",
    "STATUS_FAILED",
    "STATUS_REVERSED",
    "is_validation",
    "migration_to_run",
    "reverse_preflight",
    "sort_operations",
]
