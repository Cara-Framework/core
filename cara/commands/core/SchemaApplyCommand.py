"""``schema:apply`` — run the derived plan, and record what ran.

The plan is re-derived here rather than read from a file. Between the review
and the deploy the database can move (another deploy, a hotfix), and the only
plan that is safe to execute is the one computed against the schema actually
being changed. A printed plan is for humans; this is what runs.

Three properties make it safe to run against production:

* **Idempotent by ledger.** Every operation is recorded with its plan id and
  its stable key before the next one starts. A re-run after a crash skips
  what is already ``applied`` and continues — so the answer to "did it get
  halfway?" is a query, not an inspection of the schema.

* **Lock timeout.** Every statement runs under ``lock_timeout``, so an
  operation that would queue behind a long transaction fails fast instead of
  blocking every writer behind it. The default is deliberately short: a
  deploy that cannot get the lock now should retry, not hold the table.

* **Stops at the first failure.** A half-applied plan is recorded as such
  (the failing operation carries its error) and the command exits non-zero.
  It does not roll back what already succeeded — those operations were
  individually safe by classification, and unwinding them automatically would
  turn a transient lock timeout into a schema-wide reversal.

``CREATE INDEX CONCURRENTLY`` cannot run inside a transaction; those
operations are marked non-transactional by the planner and executed outside
one. If interrupted, Postgres leaves an INVALID index behind — re-running the
plan drops and rebuilds it, which is the trade for not locking the table.
"""

from __future__ import annotations

import hashlib

from cara.commands.CommandBase import CommandBase
from cara.commands.core.SchemaPlanCommand import SchemaPlanCommand
from cara.decorators import command
from cara.facades import DB
from cara.schema import DESTRUCTIVE

#: Short on purpose — see the module docstring.
DEFAULT_LOCK_TIMEOUT_MS = 5000


@command(
    name="schema:apply",
    help=(
        "Execute the derived schema plan against the deployed database, "
        "recording every operation (with the statement that reverses it) in "
        "the schema_operation ledger. Re-derives the plan at run time, skips "
        "operations already applied, stops at the first failure. Destructive "
        "operations require --allow-destructive."
    ),
    options={
        "--c|connection=default": "The connection to apply to",
        "--schema=?": "The Postgres schema to inspect (defaults to the connection's)",
        "--allow_destructive": "Permit destructive operations (drops)",
        "--lock_timeout=5000": "Per-statement lock timeout in milliseconds",
        "--dry_run": "Show what would run without executing or recording anything",
    },
)
class SchemaApplyCommand(CommandBase):
    def handle(self):
        """Re-derive, then execute and record operation by operation."""
        planner = SchemaPlanCommand(self.application)
        planner.set_parsed_options(
            {
                "connection": self.option("connection"),
                "schema": self.option("schema"),
            }
        )

        try:
            operations, refusals = planner.derive()
        except RuntimeError as exc:
            self.error(str(exc))
            return 2

        if refusals:
            self.error(
                f"Refusing to apply an INCOMPLETE plan — {len(refusals)} "
                f"difference(s) have no derivable statement. Run 'schema:plan' "
                f"to read them."
            )
            return 1

        if not operations:
            self.success("Nothing to apply — the deployed schema matches the models.")
            return 0

        destructive = [op for op in operations if op.safety == DESTRUCTIVE]
        if destructive and not self.option("allow_destructive"):
            self.error(
                f"{len(destructive)} destructive operation(s) in the plan; "
                f"re-run with --allow-destructive to permit them."
            )
            return 1

        plan_id = self._plan_id(operations)
        self.info(f"Plan {plan_id}: {len(operations)} operation(s)")

        if self.option("dry_run"):
            for operation in operations:
                self.info(f"   would run: {operation.forward_sql}")
            return 0

        applied = self._already_applied(plan_id)
        if applied:
            self.warning(
                f"Resuming: {len(applied)} operation(s) of this plan are already applied."
            )

        lock_timeout = int(self.option("lock_timeout") or DEFAULT_LOCK_TIMEOUT_MS)
        done = 0
        for operation in operations:
            if operation.key in applied:
                continue
            self.info(f"   {operation.describe()}")
            try:
                self._execute(operation, lock_timeout)
            except Exception as exc:
                self._record(plan_id, operation, status="failed", error=str(exc))
                self.error(
                    f"FAILED on {operation.key}: {exc}\n"
                    f"   statement: {operation.forward_sql}\n"
                    f"   {done} operation(s) of this plan were applied and are "
                    f"recorded; re-run to continue from here once the cause is fixed."
                )
                return 1
            self._record(plan_id, operation, status="applied")
            done += 1

        self.success(f"Applied {done} operation(s). Plan {plan_id} recorded.")
        return 0

    # ── seams ───────────────────────────────────────────────────────────────

    @staticmethod
    def _plan_id(operations) -> str:
        """A stable id for this exact set of operations.

        Content-derived, so re-running the same plan resumes the same ledger
        rows instead of opening a second, half-empty record of the same work.
        """
        digest = hashlib.sha256()
        for operation in operations:
            digest.update(operation.key.encode())
            digest.update(operation.forward_sql.encode())
        return digest.hexdigest()[:16]

    def _already_applied(self, plan_id: str) -> set[str]:
        rows = (
            DB.select(
                "SELECT operation_key FROM schema_operation "
                "WHERE plan_id = %s AND status = 'applied'",
                [plan_id],
            )
            or []
        )
        return {row["operation_key"] for row in rows}

    def _execute(self, operation, lock_timeout: int) -> None:
        if operation.transactional:
            DB.statement(f"SET LOCAL lock_timeout = '{lock_timeout}ms'")
            DB.statement(operation.forward_sql)
            return

        # Non-transactional (CONCURRENTLY): no SET LOCAL, no surrounding
        # transaction — the statement manages its own locking.
        DB.statement(operation.forward_sql)

    def _record(
        self, plan_id: str, operation, status: str, error: str | None = None
    ) -> None:
        DB.statement(
            "INSERT INTO schema_operation "
            "(plan_id, operation_key, kind, table_name, safety, forward_sql, "
            " reverse_sql, restores_data, status, error, applied_at, "
            " created_at, updated_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW(), NOW()) "
            "ON CONFLICT (plan_id, operation_key) DO UPDATE SET "
            "status = EXCLUDED.status, error = EXCLUDED.error, "
            "applied_at = EXCLUDED.applied_at, updated_at = NOW()",
            [
                plan_id,
                operation.key,
                operation.kind,
                operation.table,
                operation.safety,
                operation.forward_sql,
                operation.reverse_sql,
                operation.restores_data,
                status,
                error,
            ],
        )
