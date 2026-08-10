"""The ``schema_operation`` ledger: what evolve mode applied, and how to undo it.

Regenerate mode needs no ledger — the migrations table already records which
generated files ran. Evolve mode applies statements DERIVED at deploy time, so
without a record there is nothing to resume from after a crash, nothing to roll
back, and no way to tell an operator what the last deploy did to the database.

Each row is one operation from one plan:

* ``plan_id`` groups the operations of a single ``schema:plan`` run, so a
  rollback can name "the last plan" without guessing at timestamps.
* ``operation_key`` is the operation's stable identity (``table.column``).
  Together with ``status`` it makes apply idempotent: a re-run skips what is
  already applied instead of re-issuing DDL that would now fail.
* ``forward_sql`` / ``reverse_sql`` are stored as EXECUTED, not re-derived.
  Re-deriving a reverse from the current schema would compute it against a
  database the operation has already changed — the one moment the answer is
  guaranteed wrong.
* ``restores_data`` records whether the reverse returns the column's CONTENTS
  or only its shape. Rollback refuses a False row unless the operator forces
  it, because "reversible" and "lossless" are different claims and conflating
  them is how a rollback quietly destroys a day of writes.

Written only by ``schema:apply`` and ``schema:rollback``; application code
never touches it.
"""

from __future__ import annotations

from cara.eloquent.models import Model
from cara.eloquent.schema import Schema


class SchemaOperation(Model):
    """One applied schema operation, with the statement that reverses it."""

    __table__ = "schema_operation"
    __primary_key__ = "id"

    STATUS_APPLIED = "applied"
    STATUS_FAILED = "failed"
    STATUS_REVERSED = "reversed"

    __fillable__ = [
        "plan_id",
        "operation_key",
        "kind",
        "table_name",
        "safety",
        "forward_sql",
        "reverse_sql",
        "restores_data",
        "status",
        "error",
        "applied_at",
        "reversed_at",
    ]

    __casts__ = {
        "restores_data": "bool",
        "created_at": "datetime",
        "updated_at": "datetime",
        "applied_at": "datetime",
        "reversed_at": "datetime",
    }

    __indexes__ = [
        {
            "name": "schema_operation_plan_key_unique",
            "up": (
                "CREATE UNIQUE INDEX IF NOT EXISTS schema_operation_plan_key_unique "
                "ON schema_operation (plan_id, operation_key)"
            ),
            "down": "DROP INDEX IF EXISTS schema_operation_plan_key_unique",
        },
    ]

    @property
    def fields(self):
        """Define table fields for migration auto-generation."""
        return Schema.build(
            lambda field: (
                field.big_increments("id"),
                # No separate index on plan_id: the unique below already
                # leads with it, so a standalone one is a redundant prefix
                # Postgres would never choose — it only costs writes.
                field.string("plan_id", 64),
                field.string("operation_key", 255),
                field.string("kind", 40),
                field.string("table_name", 255),
                field.string("safety", 20),
                field.text("forward_sql"),
                # NULL means the operation declared itself irreversible — a
                # backfill has no undo, and saying so is the point.
                field.text("reverse_sql").nullable(),
                field.boolean("restores_data").default(True),
                field.string("status", 20).default("applied"),
                field.text("error").nullable(),
                field.datetime("applied_at").nullable(),
                field.datetime("reversed_at").nullable(),
                field.timestamps(),
            )
        )
