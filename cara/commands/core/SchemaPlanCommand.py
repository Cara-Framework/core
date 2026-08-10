"""``schema:plan`` — derive the production schema change, and show it first.

Evolve mode's premise: on a deployed database you cannot regenerate the
migrations directory, but you also should not hand-write the delta — a
hand-written ``add_x_to_y`` file is a second declaration of a column the model
already declares, and the two drift. The delta is DERIVED instead, from the
same comparison ``schema:check`` already performs: what the database has
versus what the models declare.

What this command adds over that comparison is judgement. Each difference
becomes an operation carrying its statement, its reverse, and a safety class:

* ``additive``    — nothing existing is touched (new nullable column, a
                    CONCURRENTLY index). Safe on a live table.
* ``locking``     — correct, but takes a lock that can stall a busy table.
* ``destructive`` — removes data. Never planned as ordinary work: the command
                    exits non-zero unless ``--allow-destructive`` names the
                    intent, so a drop can never ride along unnoticed inside an
                    otherwise routine plan.

REFUSALS are the other half of the honesty. A type change whose USING clause
depends on the data, a NOT NULL column with no default, a raw ``__indexes__``
column — the planner states that it will not write SQL for these, and the
plan is reported as incomplete. A tool that guessed here would be exactly the
autogenerator this replaces.

The command only reads. ``schema:apply`` executes, and re-derives the plan at
that moment so a stale printout can never be what runs.
"""

from __future__ import annotations

from cara.commands.CommandBase import CommandBase
from cara.decorators import command
from cara.schema import ADDITIVE, DESTRUCTIVE, LOCKING, introspect, plan


@command(
    name="schema:plan",
    help=(
        "Derive the operations that move the DEPLOYED database to what the "
        "models declare, classified additive / locking / destructive, each "
        "with the statement that reverses it. Reads only. Exits 1 when the "
        "plan contains destructive operations without --allow-destructive, or "
        "when the planner refused to derive part of it."
    ),
    options={
        "--c|connection=default": "The connection to plan against",
        "--schema=?": "The Postgres schema to inspect (defaults to the connection's)",
        "--allow_destructive": "Permit destructive operations (drops) in the plan",
        "--sql": "Print the executable SQL for each operation",
    },
)
class SchemaPlanCommand(CommandBase):
    def handle(self):
        """Print the derived, classified plan; never touch the database."""
        try:
            operations, refusals, notices = self.derive()
        except RuntimeError as exc:
            self.error(str(exc))
            return 2

        if not operations and not refusals and not notices:
            self.success(
                "Nothing to do — the deployed schema already matches the models."
            )
            return 0

        by_safety: dict[str, list] = {ADDITIVE: [], LOCKING: [], DESTRUCTIVE: []}
        for operation in operations:
            by_safety.setdefault(operation.safety, []).append(operation)

        for safety in (ADDITIVE, LOCKING, DESTRUCTIVE):
            group = by_safety.get(safety) or []
            if not group:
                continue
            self.info("")
            self.info(f"── {safety} ({len(group)})")
            for operation in group:
                self.info(f"   {operation.describe()}")
                self.info(f"      {operation.reason}")
                for note in operation.notes:
                    self.warning(f"      ! {note}")
                if self.option("sql"):
                    self.info(f"      → {operation.forward_sql}")
                    if operation.reverse_sql:
                        self.info(f"      ← {operation.reverse_sql}")

        if refusals:
            self.info("")
            self.warning(f"── refused to derive ({len(refusals)})")
            for refusal in refusals:
                self.warning(f"   {refusal}")

        if notices:
            self.info("")
            self.warning(f"── found, not planned ({len(notices)})")
            for notice in notices:
                self.warning(f"   {notice}")

        self.info("")
        destructive = by_safety.get(DESTRUCTIVE) or []
        if refusals:
            self.error(
                "This plan is INCOMPLETE — the differences above have no "
                "derivable statement. Resolve them in the models (or by an "
                "explicit expand/contract) before applying."
            )
            return 1
        if destructive and not self.option("allow_destructive"):
            self.error(
                f"{len(destructive)} destructive operation(s) in the plan. "
                "Re-run with --allow-destructive once you have read them; "
                "they remove data and no rollback restores it."
            )
            return 1

        self.success(
            f"{len(operations)} operation(s) planned. Apply with 'craft schema:apply'."
        )
        return 0

    def derive(self):
        """The (operations, refusals, notices) triple for this connection.

        Shared with ``schema:apply``, which re-derives rather than trusting a
        printed plan: between a review and a deploy the database can move, and
        the only safe plan is the one computed against the schema being
        changed.
        """
        try:
            from cara.eloquent.migrations import (  # local: heavy optional dep
                ModelDiscoverer,
            )
            from cara.eloquent.schema import Schema  # local: heavy optional dep
        except ImportError as exc:
            raise RuntimeError(
                f"The 'db' extra is required for schema:plan: {exc}"
            ) from exc

        connection = self.option("connection") or "default"
        schema_name = self.option("schema")

        try:
            live_schema = Schema(connection=None, schema=schema_name).on(connection)
            live = introspect(live_schema, schema_name)
        except Exception as exc:
            raise RuntimeError(
                f"Could not introspect the database ('{connection}'): {exc}"
            ) from exc

        models = ModelDiscoverer().discover_models()
        return plan(models, live)
