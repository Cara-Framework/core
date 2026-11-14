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
from cara.schema import STATUS_APPLIED as _APPLIED
from cara.schema import STATUS_FAILED as _FAILED
from cara.schema import STATUS_REVERSED as _REVERSED


class SchemaOperation(Model):
    """One applied schema operation, with the statement that reverses it."""

    __table__ = "schema_operation"
    __primary_key__ = "id"

    # The vocabulary is owned by ``cara.schema.Operation`` beside
    # ``LEDGER_TABLE``; the two schema commands write the ledger in raw SQL
    # and read it from there too.
    STATUS_APPLIED = _APPLIED
    STATUS_FAILED = _FAILED
    STATUS_REVERSED = _REVERSED

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
        # OWNER-ONLY (cycle 3, 2026-11-14). ``schema:rollback`` EXECUTES each
        # row's ``reverse_sql`` as the migration owner and ``schema:apply``
        # trusts its ``applied`` rows, so a role that may write here (the
        # runtime role, through the owner's default privileges) could plant
        # SQL the owner runs. Its write privileges are taken from every role
        # but the table's owner when the guard is created, and the trigger
        # refuses a write by anyone else even if a later GRANT gives one back.
        {
            "name": "schema_operation_owner_only",
            # The function is a TOP-LEVEL statement, not an ``EXECUTE '...'``
            # inside the DO block: ``cara.schema.NamedDdlBodies`` compares the
            # declared dollar body with ``pg_proc.prosrc`` verbatim, and a body
            # nested in a string literal carries doubled quotes (``''MEMBER''``)
            # the server never stores — a from-zero scratch read as drift.
            "up": (
                "CREATE OR REPLACE FUNCTION schema_operation_owner_only() "
                "RETURNS trigger AS $fn$ BEGIN "
                "IF NOT pg_has_role(current_user, (SELECT relowner FROM pg_class "
                "WHERE oid = TG_RELID), 'MEMBER') THEN "
                "RAISE EXCEPTION 'schema_operation is written only by its owner "
                "(schema:apply, schema:rollback)'; END IF; "
                "RETURN COALESCE(NEW, OLD); END; $fn$ LANGUAGE plpgsql; "
                "DO $do$ DECLARE grantee_name text; BEGIN "
                "FOR grantee_name IN SELECT DISTINCT g.grantee "
                "FROM information_schema.role_table_grants g "
                "WHERE g.table_schema = current_schema() "
                "AND g.table_name = 'schema_operation' "
                "AND g.privilege_type IN ('INSERT', 'UPDATE', 'DELETE', 'TRUNCATE') "
                "AND g.grantee <> (SELECT pg_get_userbyid(c.relowner) FROM pg_class c "
                "WHERE c.oid = 'schema_operation'::regclass) LOOP "
                "EXECUTE format('REVOKE INSERT, UPDATE, DELETE, TRUNCATE "
                "ON schema_operation FROM %I', grantee_name); "
                "END LOOP; "
                "END $do$"
            ),
            "down": "DROP FUNCTION IF EXISTS schema_operation_owner_only() CASCADE",
        },
        {
            "name": "schema_operation_owner_writes",
            "up": (
                "CREATE OR REPLACE TRIGGER schema_operation_owner_writes "
                "BEFORE INSERT OR UPDATE OR DELETE ON schema_operation "
                "FOR EACH ROW EXECUTE FUNCTION schema_operation_owner_only()"
            ),
            "down": (
                "DROP TRIGGER IF EXISTS schema_operation_owner_writes ON schema_operation"
            ),
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
