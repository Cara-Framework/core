"""``schema:rollback`` — undo the last applied plan, and refuse to lie about it.

Rollback exists, and it is deliberately narrow, because "reversible" and
"lossless" are different claims and every tool that blurs them eventually
destroys data on someone's behalf.

What it does: replays the stored ``reverse_sql`` of the last applied plan, in
reverse order, marking each row ``reversed``. The statements come from the
LEDGER, recorded at apply time — never re-derived from the current schema,
which the operations themselves have already changed.

What it refuses, unless ``--force`` names the intent:

* **Operations that restore shape but not contents.** Dropping a column added
  yesterday brings back an empty column; the values written into it since are
  gone. The ledger records this per row (``restores_data``), and refusing by
  default is the difference between a rollback and a second data loss.
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

from cara.commands.CommandBase import CommandBase
from cara.decorators import command
from cara.facades import DB

_APPLY_LOCK_TIMEOUT_MS = 5000


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

        blocked = [
            row for row in rows if row["reverse_sql"] is None or not row["restores_data"]
        ]
        if blocked and not self.option("force"):
            for row in blocked:
                why = (
                    "no reverse statement — this operation cannot be undone"
                    if row["reverse_sql"] is None
                    else "the reverse restores the column's SHAPE, not its contents"
                )
                self.warning(f"   {row['operation_key']}: {why}")
            self.error(
                f"{len(blocked)} operation(s) cannot be honestly reversed. Roll "
                f"FORWARD with a new plan, or re-run with --force if you have "
                f"confirmed nothing has written to the new shape."
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
                self._execute(row["reverse_sql"])
            except Exception as exc:
                self.error(
                    f"FAILED reversing {row['operation_key']}: {exc}\n"
                    f"   statement: {row['reverse_sql']}\n"
                    f"   {reversed_count} operation(s) were reversed; the rest "
                    f"remain applied and recorded."
                )
                return 1
            self._mark_reversed(plan_id, row["operation_key"])
            reversed_count += 1

        if self.option("dry_run"):
            self.success(f"Would reverse {len(rows)} operation(s).")
            return 0

        self.success(f"Reversed {reversed_count} operation(s) of plan {plan_id}.")
        return 0

    # ── seams ───────────────────────────────────────────────────────────────

    def _latest_plan(self) -> str | None:
        rows = (
            DB.select(
                "SELECT plan_id FROM schema_operation WHERE status = 'applied' "
                "ORDER BY id DESC LIMIT 1"
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
                "SELECT operation_key, reverse_sql, restores_data "
                "FROM schema_operation "
                "WHERE plan_id = %s AND status = 'applied' "
                "ORDER BY id DESC",
                [plan_id],
            )
            or []
        )

    def _execute(self, statement: str) -> None:
        if "CONCURRENTLY" not in statement.upper():
            DB.statement(f"SET LOCAL lock_timeout = '{_APPLY_LOCK_TIMEOUT_MS}ms'")
        DB.statement(statement)

    def _mark_reversed(self, plan_id: str, operation_key: str) -> None:
        DB.statement(
            "UPDATE schema_operation SET status = 'reversed', reversed_at = NOW(), "
            "updated_at = NOW() WHERE plan_id = %s AND operation_key = %s",
            [plan_id, operation_key],
        )
