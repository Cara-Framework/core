"""``schema:apply`` — run the derived plan, and record what ran.

The plan is always re-derived here. Between the review and the deploy the
database can move — another deploy, a hotfix applied by hand — and the only
plan that is safe to execute is the one computed against the schema actually
being changed.

``--plan`` does not weaken that: it names a reviewed artifact and apply then
DERIVES the plan afresh and compares. Identical, it runs and you have both
properties at once — a change reviewed in a pull request AND a statement
computed against the current database. Different, it refuses and says so,
which is also the drift gate: an unadopted hotfix cannot be silently planned
around, because the plan a human approved is no longer the plan that would
run.

Three properties make it safe to run against production:

* **Idempotent by ledger.** Every operation is recorded with its plan id and
  its stable key before the next one starts — for a transactional statement
  in the same transaction as the statement itself. An applied operation stops
  deriving, so a plan stopped partway derives a smaller plan with a different
  id. Its artifact resumes when the ledger explains that difference exactly:
  the reviewed operations minus those applied under the reviewed id are the
  operations that derive now. The work continues under the reviewed id, so one
  rollback still reverses all of it. The answer to "did it get halfway?" is a
  query, not an inspection of the schema.

* **One at a time.** A session advisory lock serializes this command and
  ``schema:rollback`` per database. A second run is refused at once rather
  than queued, and the lock is taken before the plan is derived, so no run can
  act on a schema another one is still changing.

* **Lock timeout.** Every transactional statement runs in its own
  transaction under ``SET LOCAL lock_timeout``, so an operation that would
  queue behind a long transaction fails fast instead of blocking every writer
  behind it. The default is deliberately short: a deploy that cannot get the
  lock now should retry, not hold the table. A ``CONCURRENTLY`` statement
  cannot run in a transaction and keeps the connection's configured session
  ``lock_timeout``.

* **Its own statement budget.** The connection is the APPLICATION's, and its
  ``statement_timeout`` (30 s in ``config/database.py``) is a user-facing
  request budget: under it a fill of a real table, a ``VALIDATE`` scan or a
  ``CREATE INDEX CONCURRENTLY`` was cancelled on every re-run — a concurrent
  build left INVALID — while deploy.sh kept traffic offline. Every step sets
  ``--statement_timeout`` instead (``SET LOCAL`` in its transaction; around
  a ``CONCURRENTLY`` statement a session ``SET`` that is ``RESET`` after it,
  success or not). The default, 0, is no cap: the lock timeout is what keeps
  a step from holding writers, and a step stopped by a budget only repeats.

* **Preflight before each statement.** An operation that can fail on DATA
  carries a read-only query that must return no rows — a NULL in a column
  about to become NOT NULL, a duplicate under a unique index about to be
  built. Checking immediately before running is what makes it worth anything:
  the answer is about production's rows at this moment, and a plan reviewed an
  hour ago cannot speak for them. A failed preflight stops the plan BEFORE
  the statement, so the failure costs nothing.

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

import json
from pathlib import Path

from cara.commands.CommandBase import CommandBase
from cara.commands.core.SchemaPlanCommand import SchemaPlanCommand
from cara.decorators import command
from cara.facades import DB
from cara.schema import (
    DESTRUCTIVE,
    LEDGER_TABLE,
    SCHEMA_CHANGE_LOCK,
    STATUS_APPLIED,
    STATUS_FAILED,
    from_dict,
    migration_to_run,
    plan_id,
)
from cara.support import paths

#: Short on purpose — see the module docstring.
DEFAULT_LOCK_TIMEOUT_MS = 5000

#: No cap per step — see the module docstring ("Its own statement budget").
DEFAULT_STATEMENT_TIMEOUT_MS = 0

#: Key range of one backfill transaction. About 0.2 s of a writer's wait at the
#: measured 39 microseconds per row, on a table where every key needs a fill.
_BACKFILL_BATCH_KEYS = 5000

_FILL_KIND = "backfill_column"


@command(
    name="schema:apply",
    help=(
        "Execute the derived schema plan against the deployed database, "
        "recording every operation (with the statement that reverses it) in "
        "the schema_operation ledger. Re-derives the plan at run time, skips "
        "operations already applied, stops at the first failure. Destructive "
        "operations require --allow_destructive."
    ),
    options=[
        {
            "name": "-c|--connection",
            "help": "The connection to apply to",
            "type": str,
            "default": "default",
            "is_flag": False,
        },
        {
            "name": "--schema",
            "help": "The Postgres schema to inspect (defaults to the connection's)",
            "type": str,
            "default": None,
            "is_flag": False,
        },
        {
            "name": "--allow_destructive",
            "help": "Permit destructive operations (drops)",
            "type": bool,
            "default": False,
            "is_flag": True,
        },
        {
            "name": "--lock_timeout",
            "help": (
                "Lock timeout in milliseconds for each transactional statement "
                "(CONCURRENTLY statements keep the connection's own)"
            ),
            "type": int,
            "default": 5000,
            "is_flag": False,
        },
        {
            "name": "--statement_timeout",
            "help": (
                "statement_timeout in milliseconds for each step, replacing the "
                "connection's application budget (0 = no cap)"
            ),
            "type": int,
            "default": DEFAULT_STATEMENT_TIMEOUT_MS,
            "is_flag": False,
        },
        {
            "name": "--dry_run",
            "help": "Show what would run without executing or recording anything",
            "type": bool,
            "default": False,
            "is_flag": True,
        },
        {
            "name": "--plan",
            "help": "Apply a reviewed plan artifact; refuses if the database has moved",
            "type": str,
            "default": None,
            "is_flag": False,
        },
    ],
)
class SchemaApplyCommand(CommandBase):
    def handle(self):
        """Run the plan, one schema change at a time on this database."""
        if self.option("dry_run"):
            return self._apply()
        with DB.advisory_session(SCHEMA_CHANGE_LOCK) as held:
            if not held:
                self.error(
                    "Another schema:apply or schema:rollback is running on this "
                    "database. Nothing was applied; run it again when that one "
                    "has finished."
                )
                return 1
            return self._apply()

    def _apply(self):
        """Re-derive, then execute and record operation by operation."""
        planner = SchemaPlanCommand(self.application)
        planner.set_parsed_options(
            {
                "connection": self.option("connection"),
                "schema": self.option("schema"),
            }
        )

        try:
            # Notices are findings for a human, not work — they never
            # block an apply, so they are read here and not acted on.
            operations, refusals, _notices = planner.derive()
        except RuntimeError as exc:
            self.error(str(exc))
            return 2

        identifier = plan_id(operations)

        # FIRST, before any other outcome. "The plan you approved is not what
        # I would do now" is true whether the new plan is different, incomplete
        # or empty — and empty is the case that matters most: a hand-applied
        # hotfix makes the derived plan match the models exactly, so a check
        # placed after the empty-plan return would report a cheerful "nothing
        # to apply" for a database somebody changed behind the deploy's back.
        artifact = self.option("plan")
        ledger_explained = False
        if artifact:
            reviewed = self._reviewed_plan(artifact)
            if reviewed is None:
                return 2
            approved, reviewed_operations = reviewed
            ledger_explained = approved != identifier and self._ledger_explains(
                approved, reviewed_operations, operations
            )
            if ledger_explained:
                identifier = approved
                self.info(
                    f"Artifact {artifact}: what the ledger has not applied still "
                    f"matches the database."
                )
            elif approved != identifier:
                self.error(
                    f"The database has MOVED since {artifact} was reviewed.\n"
                    f"   approved plan: {approved}\n"
                    f"   derived now:   {identifier}"
                    f"{' (nothing left to do)' if not operations else ''}\n"
                    f"   Something changed this schema outside these commands, "
                    f"or the models changed after the plan was written. Re-run "
                    f"'schema:plan --out {artifact}', review it again, redeploy."
                )
                return 1
            else:
                self.info(f"Artifact {artifact} still matches the database.")

        if refusals:
            self.error(
                f"Refusing to apply an INCOMPLETE plan — {len(refusals)} "
                f"difference(s) have no derivable statement. Run 'schema:plan' "
                f"to read them."
            )
            return 1

        if not operations:
            self.success(
                f"Already applied: every operation of plan {identifier} is in the ledger."
                if ledger_explained
                else "Nothing to apply — the deployed schema matches the models."
            )
            return 0

        destructive = [op for op in operations if op.safety == DESTRUCTIVE]
        if destructive and not self.option("allow_destructive"):
            self.error(
                f"{len(destructive)} destructive operation(s) in the plan; "
                f"re-run with --allow_destructive to permit them."
            )
            return 1

        self.info(f"Plan {identifier}: {len(operations)} operation(s)")

        if self.option("dry_run"):
            for operation in operations:
                self.info(f"   would run: {operation.forward_sql}")
            return 0

        # Only a run the ledger EXPLAINS is a resume. A plan id is a hash of
        # its operations, so the same drift arising twice (a trigger disabled
        # again, the same index dropped again) derives the id of a plan that
        # already finished; reading that plan's rows as "done" skipped every
        # operation the database still needs and printed "Applied 0". What
        # derives now is work the schema still owes: run all of it.
        applied = self._already_applied(identifier) if ledger_explained else set()
        if applied:
            self.warning(
                f"Resuming: {len(applied)} operation(s) of this plan are already applied."
            )
        elif self._already_applied(identifier):
            self.warning(
                f"Plan {identifier} is recorded as applied, yet the database "
                f"derives it again — the same drift recurred. Applying it again."
            )

        lock_timeout = int(self.option("lock_timeout") or DEFAULT_LOCK_TIMEOUT_MS)
        self._statement_budget = int(
            self.option("statement_timeout") or DEFAULT_STATEMENT_TIMEOUT_MS
        )
        done = 0
        for operation in operations:
            # A fill that still derives runs again: its own IS NULL makes that
            # safe, and a row written NULL since would stop the SET NOT NULL
            # behind it on every re-run.
            if operation.key in applied and operation.kind != _FILL_KIND:
                continue
            self.info(f"   {operation.describe()}")
            blocker = self._preflight(operation)
            if blocker:
                self.error(
                    f"PREFLIGHT FAILED on {operation.key}: {blocker}\n"
                    f"   query: {operation.preflight_sql}\n"
                    f"   {done} operation(s) applied before this; nothing was "
                    f"attempted for this one."
                )
                return 1
            try:
                self._execute(identifier, operation, lock_timeout)
            except Exception as exc:
                self._record(identifier, operation, status=STATUS_FAILED, error=str(exc))
                self.error(
                    f"FAILED on {operation.key}: {exc}\n"
                    f"   statement: {operation.forward_sql}\n"
                    f"   {done} operation(s) of this plan were applied and are "
                    f"recorded; re-run to continue from here once the cause is fixed."
                )
                return 1
            done += 1

        self.success(f"Applied {done} operation(s). Plan {identifier} recorded.")
        return 0

    # ── seams ───────────────────────────────────────────────────────────────

    def _reviewed_plan(self, path: str) -> tuple[str, list] | None:
        """The plan id and operations of a reviewed artifact, or None if unreadable."""
        try:
            document = json.loads(Path(path).read_text(encoding="utf-8"))
            operations = [from_dict(data) for data in document.get("operations") or []]
        except Exception as exc:
            self.error(f"Could not read the plan artifact {path}: {exc}")
            return None
        approved = document.get("plan_id")
        if not approved:
            self.error(f"{path} carries no plan_id — it is not a plan artifact.")
            return None
        return approved, operations

    def _ledger_explains(self, approved: str, reviewed: list, derived: list) -> bool:
        """Whether the ledger accounts for all of the artifact's difference.

        The derived plan must be exactly the reviewed one minus the operations
        recorded under its id. Anything else, including an empty derivation the
        ledger does not account for, is a schema that moved some other way: an
        empty plan is what a hand-applied hotfix looks like. A fill the reviewed
        plan named is the one exception: it still derives after it ran, because
        a column that is still nullable cannot show it.
        """
        applied = self._already_applied(approved)
        if not applied:
            return False
        named = {(op.key, op.forward_sql) for op in reviewed}
        ran_fill = {
            (op.key, op.forward_sql)
            for op in derived
            if op.kind == _FILL_KIND and op.key in applied
        }
        if not ran_fill <= named:
            return False
        remaining = {pair for pair in named if pair[0] not in applied}
        return remaining == {(op.key, op.forward_sql) for op in derived} - ran_fill

    def _already_applied(self, plan_id: str) -> set[str]:
        """Operation keys of this plan already recorded as applied.

        The ledger's own absence is a legitimate answer, not an error: on the
        FIRST evolve run against a database that predates it — cutover day —
        the ledger is a table this very plan creates, so "nothing has been
        applied" is true precisely because there is nowhere it could have been
        recorded. Asked as an explicit existence question rather than caught as
        an exception, so a connection failure or a permission error still
        surfaces instead of being read as an empty ledger and re-running a plan
        that already ran.
        """
        exists = DB.select(f"SELECT to_regclass('{LEDGER_TABLE}') AS oid")
        if not exists or exists[0]["oid"] is None:
            return set()

        rows = (
            DB.select(
                f"SELECT operation_key FROM {LEDGER_TABLE} "
                f"WHERE plan_id = %s AND status = '{STATUS_APPLIED}'",
                [plan_id],
            )
            or []
        )
        return {row["operation_key"] for row in rows}

    def _preflight(self, operation) -> str | None:
        """The reason this operation would fail, or None if it is clear.

        Read-only by construction: the operation supplies a SELECT that must
        return no rows. It is asked right before its step, after every
        operation ordered ahead of it ran, so a query that still cannot run is
        not "no rows": nothing proves the step is safe, and the plan stops
        before it rather than learning the answer from the statement itself.
        """
        if not operation.preflight_sql:
            return None
        # Under the step's own budget: a duplicate or NULL probe scans the
        # whole table, and cut off by the API's 30 s budget it is unanswerable
        # — which stops the plan on every re-run.
        try:
            with DB.transaction():
                DB.statement(f"SET LOCAL statement_timeout = {self._budget_ms()}")
                rows = DB.select(operation.preflight_sql)
        except Exception as exc:
            return f"the preflight could not be answered: {exc}".strip()
        return operation.preflight_failure if rows else None

    def _execute(self, plan_id: str, operation, lock_timeout: int) -> None:
        """Run one operation and record it ``applied``.

        ``SET LOCAL`` binds only inside a transaction block: sent on its own
        through an autocommit connection it is a warning and nothing else, and
        the DDL after it ran with no timeout at all. So a transactional
        statement, its timeout and its ledger row share one transaction — the
        schema change and the record of it commit together or not at all.
        """
        slug = migration_to_run(operation.forward_sql)
        if slug is not None:
            self._run_generated_migration(slug)
            self._record(plan_id, operation, status=STATUS_APPLIED)
            return

        if operation.batch_column:
            self._run_in_batches(operation, lock_timeout)
            self._record(plan_id, operation, status=STATUS_APPLIED)
            return

        if operation.transactional:
            with DB.transaction():
                self._set_local_budget(lock_timeout)
                DB.statement(operation.forward_sql)
                self._record(plan_id, operation, status=STATUS_APPLIED)
            return

        # Non-transactional (CONCURRENTLY): Postgres refuses it inside a
        # transaction, so it keeps the connection's configured session
        # lock_timeout, and its statement budget is a session SET that is
        # RESET before the connection can serve anything else. This command
        # runs one statement at a time, so the pool hands the SET's
        # connection straight back to the statement after it.
        DB.statement(f"SET statement_timeout = {self._budget_ms()}")
        try:
            DB.statement(operation.forward_sql)
        finally:
            DB.statement("RESET statement_timeout")
        self._record(plan_id, operation, status=STATUS_APPLIED)

    def _budget_ms(self) -> int:
        return int(getattr(self, "_statement_budget", DEFAULT_STATEMENT_TIMEOUT_MS))

    def _set_local_budget(self, lock_timeout: int) -> None:
        """Both timeouts of one step, bound to its transaction."""
        DB.statement(f"SET LOCAL lock_timeout = '{lock_timeout}ms'")
        DB.statement(f"SET LOCAL statement_timeout = {self._budget_ms()}")

    def _run_in_batches(self, operation, lock_timeout: int) -> None:
        """Run a backfill over its key in short transactions.

        The range is walked rather than "the next rows still NULL", because a
        row a trigger keeps NULL would come back forever. Rows inserted after
        the upper bound is read are not walked: they are born under the column.
        """
        column = operation.batch_column
        bounds = DB.select(
            f'SELECT MIN("{column}") AS low, MAX("{column}") AS high '
            f'FROM "{operation.table}"'
        )
        if not bounds or bounds[0]["low"] is None:
            return
        for start in range(bounds[0]["low"] - 1, bounds[0]["high"], _BACKFILL_BATCH_KEYS):
            with DB.transaction():
                self._set_local_budget(lock_timeout)
                DB.statement(
                    f'{operation.forward_sql} AND "{column}" > {start} '
                    f'AND "{column}" <= {start + _BACKFILL_BATCH_KEYS}'
                )

    def _run_generated_migration(self, slug: str) -> None:
        """Execute the generated creator for a table the database lacks.

        A new table is the second most common schema change after a new
        column, and evolve mode could not perform it at all: the operation
        carried a comment where its SQL should be, so apply died on "can't
        execute an empty query" and no table could ever be added to a deployed
        database.

        Running the generated file rather than re-rendering its DDL keeps ONE
        renderer for a table's shape. The same file is what a fresh install
        runs, so an evolved database and a fresh one get byte-identical DDL —
        which is the property `schema:verify` proves for the directory as a
        whole.
        """
        from cara.eloquent.migrations import (
            MigrationFileManager,  # local: heavy optional dep
        )

        directory = Path(paths("migrations"))
        matches = sorted(directory.glob(f"*_{slug}.py"))
        if not matches:
            raise RuntimeError(
                f"No generated migration matching '{slug}' in {directory}. "
                f"Regenerate the directory in development and redeploy — evolve "
                f"mode runs the generated creator, it does not render DDL."
            )
        if len(matches) > 1:
            raise RuntimeError(
                f"{len(matches)} migrations match '{slug}': "
                f"{', '.join(path.name for path in matches)}. "
                f"One table, one generated file — fix the directory first."
            )

        migration_class = MigrationFileManager(str(directory)).load_migration_class(
            matches[0]
        )
        migration_class().up()

    def _record(
        self, plan_id: str, operation, status: str, error: str | None = None
    ) -> None:
        """Write one ledger row; a failure never overwrites an ``applied`` one.

        A re-applied row sheds the ``reversed_at`` of the undo it no longer is.
        """
        DB.statement(
            "INSERT INTO schema_operation "
            "(plan_id, operation_key, kind, table_name, safety, forward_sql, "
            " reverse_sql, restores_data, status, error, applied_at, "
            " created_at, updated_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW(), NOW()) "
            "ON CONFLICT (plan_id, operation_key) DO UPDATE SET "
            "status = EXCLUDED.status, error = EXCLUDED.error, "
            "applied_at = EXCLUDED.applied_at, "
            f"reversed_at = CASE WHEN EXCLUDED.status = '{STATUS_APPLIED}' THEN NULL "
            "ELSE schema_operation.reversed_at END, updated_at = NOW() "
            f"WHERE EXCLUDED.status = '{STATUS_APPLIED}' "
            f"OR schema_operation.status <> '{STATUS_APPLIED}'",
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
