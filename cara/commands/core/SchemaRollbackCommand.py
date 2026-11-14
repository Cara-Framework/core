"""``schema:rollback`` — undo the last applied plan, and refuse to lie about it.

Rollback exists, and it is deliberately narrow, because "reversible" and
"lossless" are different claims and every tool that blurs them eventually
destroys data on someone's behalf.

What it does: replays the stored ``reverse_sql`` of the last applied plan, in
reverse order, marking each row ``reversed``. The statements come from the
LEDGER, recorded at apply time — never re-derived from the current schema,
which the operations themselves have already changed.

What it refuses, unless ``--force`` names the intent:

* **Operations that restore shape but not contents.** Reversing a column or a
  table added yesterday drops it, and every value written into it since goes
  with it; reversing a dropped column brings back an empty one. The ledger
  records this per row (``restores_data``), and refusing by default is the
  difference between a rollback and a second data loss.
* **Operations with no reverse at all.** A backfill has no undo — the previous
  values are not recorded anywhere, and inventing an UPDATE that sets them
  back to NULL would be a guess about which rows were already filled.

The honest position, which the doctrine states and this command enforces:
**production rolls FORWARD.** A bad schema change is followed by another
change that fixes it, planned and reviewed like any other. Rollback is for
the minutes right after a deploy, when nothing has written to the new shape
yet — that is the window where it is genuinely safe, and it is the only
window this command is comfortable in.
"""

from __future__ import annotations

import re

from cara.commands.CommandBase import CommandBase
from cara.decorators import command
from cara.facades import DB
from cara.schema import (
    LEDGER_TABLE,
    SCHEMA_CHANGE_LOCK,
    STATUS_APPLIED,
    STATUS_REVERSED,
    is_validation,
    reverse_preflight,
)

_APPLY_LOCK_TIMEOUT_MS = 5000

#: Per-reverse statement budget, as ``schema:apply --statement_timeout``: the
#: connection's own is the API's 30 s request budget, and an index rebuild or
#: a type reverse on a real table outlives it on every re-run.
_DEFAULT_STATEMENT_TIMEOUT_MS = 0

#: The index a CONCURRENTLY reverse builds (``Objects._rebuild_index_sql``).
_BUILT_INDEX = re.compile(
    r"CREATE (?:UNIQUE )?INDEX CONCURRENTLY (?:IF NOT EXISTS )?(\S+) ON ", re.I
)


@command(
    name="schema:rollback",
    help=(
        "Reverse the last applied schema plan using the reverse statements "
        "recorded in the ledger, newest operation first. Refuses operations "
        "whose reverse restores shape but not data (and those with no reverse "
        "at all) unless --force. Production normally rolls FORWARD; this is "
        "for the window right after a deploy."
    ),
    options=[
        {
            "name": "--plan",
            "help": "Plan id to reverse (defaults to the most recent applied plan)",
            "type": str,
            "default": None,
            "is_flag": False,
        },
        {
            "name": "--force",
            "help": "Reverse even operations that cannot restore the data they removed",
            "type": bool,
            "default": False,
            "is_flag": True,
        },
        {
            "name": "--statement_timeout",
            "help": "statement_timeout in milliseconds for each reverse (0 = no cap)",
            "type": int,
            "default": _DEFAULT_STATEMENT_TIMEOUT_MS,
            "is_flag": False,
        },
        {
            "name": "--dry_run",
            "help": "Show what would be reversed without executing anything",
            "type": bool,
            "default": False,
            "is_flag": True,
        },
    ],
)
class SchemaRollbackCommand(CommandBase):
    def handle(self):
        """Replay the ledger's reverse statements, one change at a time."""
        if self.option("dry_run"):
            return self._rollback()
        with DB.advisory_session(SCHEMA_CHANGE_LOCK) as held:
            if not held:
                self.error(
                    "Another schema:apply or schema:rollback is running on this "
                    "database. Nothing was reversed; run it again when that "
                    "one has finished."
                )
                return 1
            return self._rollback()

    def _rollback(self):
        """Replay the ledger's reverse statements for one plan."""
        plan_id = self.option("plan") or self._latest_plan()
        if not plan_id:
            self.warning("No applied plan found in the ledger — nothing to reverse.")
            return 0

        rows = self._applied_rows(plan_id)
        if not rows:
            self.warning(f"Plan {plan_id} has no applied operations to reverse.")
            return 0

        self.info(f"Plan {plan_id}: {len(rows)} applied operation(s)")

        # A VALIDATE has nothing to undo: the ADD it proves carries the
        # reverse that removes the constraint. Read as "irreversible", it made
        # every plan that adds a CHECK or a FOREIGN KEY un-rollbackable without
        # --force, which also waves through the reverses that lose data.
        rows = [
            row
            for row in rows
            if not is_validation(row.get("kind"), row["operation_key"])
        ]
        blocked = [
            row for row in rows if row["reverse_sql"] is None or not row["restores_data"]
        ]
        if blocked and not self.option("force"):
            for row in blocked:
                why = (
                    "no reverse statement — this operation cannot be undone"
                    if row["reverse_sql"] is None
                    else "the reverse restores the object's SHAPE, not its contents"
                )
                self.warning(f"   {row['operation_key']}: {why}")
            self.error(
                f"{len(blocked)} operation(s) cannot be honestly reversed. Roll "
                f"FORWARD with a new plan, or re-run with --force if you have "
                f"confirmed nothing has written to the new shape."
            )
            return 1

        # Every reverse that can fail on DATA is asked first. Each reverse is
        # its own transaction, so one failing midway leaves the schema half
        # reversed; asked up front, the rollback completes or never starts.
        refusals = self._reverse_blockers(rows)
        if refusals:
            for refusal in refusals:
                self.error(f"   {refusal}")
            self.error(
                f"{len(refusals)} reverse(s) would FAIL against the rows there now. "
                "Nothing was reversed; roll FORWARD with a new plan."
            )
            return 1

        reversed_count = 0
        for row in rows:  # newest first — see _applied_rows
            if row["reverse_sql"] is None:
                self.warning(f"   skipping {row['operation_key']} (no reverse)")
                continue
            self.info(f"   reversing {row['operation_key']}")
            if self.option("dry_run"):
                self.info(f"      → {row['reverse_sql']}")
                continue
            try:
                self._execute(plan_id, row)
            except Exception as exc:
                self.error(
                    f"FAILED reversing {row['operation_key']}: {exc}\n"
                    f"   statement: {row['reverse_sql']}\n"
                    f"   {reversed_count} operation(s) were reversed; the rest "
                    f"remain applied and recorded."
                )
                return 1
            reversed_count += 1

        if self.option("dry_run"):
            self.success(f"Would reverse {len(rows)} operation(s).")
            return 0

        self.success(f"Reversed {reversed_count} operation(s) of plan {plan_id}.")
        return 0

    def _reverse_blockers(self, rows: list[dict]) -> list[str]:
        """Why each data-dependent reverse would fail now; empty when none."""
        found: list[str] = []
        for row in rows:
            question = reverse_preflight(row["reverse_sql"])
            if question is None:
                continue
            query, failure = question
            try:
                hit = DB.select(query)
            except Exception as exc:
                found.append(
                    f"{row['operation_key']}: the reverse's preflight could not be "
                    f"answered ({exc})"
                )
                continue
            if hit:
                found.append(f"{row['operation_key']}: {failure}")
        return found

    # ── seams ───────────────────────────────────────────────────────────────

    def _latest_plan(self) -> str | None:
        rows = (
            DB.select(
                f"SELECT plan_id FROM {LEDGER_TABLE} "
                f"WHERE status = '{STATUS_APPLIED}' ORDER BY id DESC LIMIT 1"
            )
            or []
        )
        return rows[0]["plan_id"] if rows else None

    def _applied_rows(self, plan_id: str) -> list[dict]:
        """The plan's applied operations, NEWEST FIRST.

        Reverse order matters: a plan that added a column and then indexed it
        must drop the index before the column, or the drop fails.
        """
        return (
            DB.select(
                "SELECT operation_key, kind, reverse_sql, restores_data "
                f"FROM {LEDGER_TABLE} "
                f"WHERE plan_id = %s AND status = '{STATUS_APPLIED}' "
                "ORDER BY id DESC",
                [plan_id],
            )
            or []
        )

    def _execute(self, plan_id: str, row: dict) -> None:
        """Run one reverse statement and mark its ledger row.

        ``SET LOCAL`` binds only inside a transaction block: sent on its own
        through an autocommit connection it is a warning and nothing else, and
        the statement after it ran with no timeout at all. So the timeout, the
        reverse and the ledger update share one transaction. ``CONCURRENTLY``
        cannot run inside one; it runs under the connection's configured
        session ``lock_timeout`` and is marked once it has succeeded.
        """
        statement = row["reverse_sql"]
        budget = self._budget_ms()
        if "CONCURRENTLY" in statement.upper():
            self._run_concurrently(statement, budget)
            self._mark_reversed(plan_id, row["operation_key"])
            return
        with DB.transaction():
            DB.statement(f"SET LOCAL lock_timeout = '{_APPLY_LOCK_TIMEOUT_MS}ms'")
            DB.statement(f"SET LOCAL statement_timeout = {budget}")
            DB.statement(statement)
            self._mark_reversed(plan_id, row["operation_key"])

    def _budget_ms(self) -> int:
        option = getattr(self, "option", None)
        value = option("statement_timeout") if callable(option) else None
        return int(value or _DEFAULT_STATEMENT_TIMEOUT_MS)

    def _run_concurrently(self, statement: str, budget: int) -> None:
        """A CONCURRENTLY reverse, under its own budget, that must end VALID.

        A rebuild cut off (by the API's 30 s budget, a lock timeout, a
        cancel) leaves an INVALID index under the name, and the reverse's
        ``IF NOT EXISTS`` then made the re-run a silent no-op: the ledger said
        "reversed" over an index that serves no query. That leftover is
        dropped first, and the build is checked VALID before it is marked.
        The session SET is RESET whatever happens — this command runs one
        statement at a time, so the pool hands the same connection back.
        """
        built = _BUILT_INDEX.match(statement.strip())
        name = built[1] if built else None
        if name and self._index_valid(name) is False:
            DB.statement(f"DROP INDEX CONCURRENTLY IF EXISTS {name}")
        DB.statement(f"SET statement_timeout = {budget}")
        try:
            DB.statement(statement)
        finally:
            DB.statement("RESET statement_timeout")
        if name and self._index_valid(name) is not True:
            raise RuntimeError(
                f"index {name} is not VALID after its rebuild; nothing was "
                "marked reversed — re-run the rollback"
            )

    def _index_valid(self, name: str) -> bool | None:
        """``pg_index.indisvalid`` of an index by name, None when it is absent."""
        rows = DB.select(
            "SELECT indisvalid AS valid FROM pg_index WHERE indexrelid = to_regclass(%s)",
            [name],
        )
        return bool(rows[0]["valid"]) if rows else None

    def _mark_reversed(self, plan_id: str, operation_key: str) -> None:
        """Mark a row reversed, and with a column the fill written into it.

        A backfill has no reverse of its own, so it was skipped and stayed
        ``applied``: a re-apply of the plan re-added the column and then
        skipped the fill as already done.
        """
        DB.statement(
            f"UPDATE {LEDGER_TABLE} SET status = '{STATUS_REVERSED}', "
            "reversed_at = NOW(), updated_at = NOW() "
            "WHERE plan_id = %s AND operation_key IN (%s, %s, %s)",
            [
                plan_id,
                operation_key,
                f"{operation_key}:backfill",
                # The VALIDATE of a constraint this reverse removed: left
                # ``applied``, a re-apply re-attached the constraint NOT VALID
                # and skipped proving the rows.
                f"{operation_key}:validate",
            ],
        )
