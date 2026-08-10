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
                    exits non-zero unless ``--allow_destructive`` names the
                    intent, so a drop can never ride along unnoticed inside an
                    otherwise routine plan.

REFUSALS are the other half of the honesty. A type change whose USING clause
depends on the data, a NOT NULL column with no default, a raw ``__indexes__``
column — the planner states that it will not write SQL for these, and the
plan is reported as incomplete. A tool that guessed here would be exactly the
autogenerator this replaces.

``--out`` writes the plan as a JSON ARTIFACT. That is what restores the one
real advantage a hand-written migration has: the change becomes a file a
reviewer reads in a pull request, before the deploy, rather than terminal
output someone watches during it. The artifact is DERIVED, so it is still not
a second source of truth — and ``schema:apply --plan`` re-derives anyway and
refuses if the database has moved since, so a stale artifact can never be
what runs.

``--rehearse`` answers the last question review cannot: *does this SQL run?*
Classification says what an operation costs and preflight says whether the
rows allow it, but neither executes anything, and derived SQL can still be
rejected by the server — an index expression that is not IMMUTABLE, a default
the type will not take, an operation ordered before the one it depends on. So
the plan is run for real against a scratch database holding the deployed
SHAPE (``pg_dump --schema-only``, no rows), by spawning ``schema:apply``
itself rather than a rehearsal-flavoured copy of it. One executor, exercised
exactly as it will be on the day. A rehearsal that used its own code path
would prove that path works, which is not the question.

The scratch is filled through the plan ARTIFACT, so ``schema:apply``'s own
staleness gate does double duty: if the structure clone does not derive the
same plan as production, the gate says so, and that is a finding about the
clone rather than a rehearsal quietly testing something else.

The command itself only reads. ``--rehearse`` writes only to a scratch
database it creates and drops.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

from cara.commands.CommandBase import CommandBase
from cara.decorators import command
from cara.exceptions import ScratchDatabaseException
from cara.schema import (
    ADDITIVE,
    DESTRUCTIVE,
    LOCKING,
    Scratch,
    as_dict,
    introspect,
    plan,
    plan_id,
)


@command(
    name="schema:plan",
    help=(
        "Derive the operations that move the DEPLOYED database to what the "
        "models declare, classified additive / locking / destructive, each "
        "with the statement that reverses it. Reads only. Exits 1 when the "
        "plan contains destructive operations without --allow_destructive, or "
        "when the planner refused to derive part of it."
    ),
    options={
        "--c|connection=default": "The connection to plan against",
        "--schema=?": "The Postgres schema to inspect (defaults to the connection's)",
        "--allow_destructive": "Permit destructive operations (drops) in the plan",
        "--sql": "Print the executable SQL for each operation",
        "--out=?": "Write the plan as a JSON artifact to this path (for review)",
        "--rehearse": "Run the plan against a structure-clone scratch first",
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

        blocked: list[str] = []
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
                if operation.preflight_sql:
                    blocker = self._preflight(operation)
                    if blocker:
                        blocked.append(blocker)
                        self.error(f"      ✗ {blocker}")
                    else:
                        self.info("      ✓ preflight clear")
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
        if blocked:
            self.error(
                f"{len(blocked)} operation(s) would FAIL against the rows in "
                f"the database right now. Fix the data first — applying this "
                f"plan stops partway through."
            )
            return 1
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
                "Re-run with --allow_destructive once you have read them; "
                "they remove data and no rollback restores it."
            )
            return 1

        identifier = plan_id(operations)
        destination = self.option("out")
        if destination:
            self._write_artifact(destination, identifier, operations, notices)

        if self.option("rehearse"):
            code = self._rehearse(identifier, operations, notices)
            if code:
                return code

        if destination:
            self.success(
                f"{len(operations)} operation(s) written to {destination} "
                f"(plan {identifier}). Review it, then "
                f"'craft schema:apply --plan {destination}'."
            )
            return 0

        self.success(
            f"{len(operations)} operation(s) planned. Apply with 'craft schema:apply'."
        )
        return 0

    def _write_artifact(self, destination, identifier, operations, notices) -> None:
        Path(destination).write_text(
            json.dumps(
                {
                    "plan_id": identifier,
                    "operations": [as_dict(op) for op in operations],
                    "notices": notices,
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )

    def _rehearse(self, identifier, operations, notices) -> int:
        """Run this plan for real against a structure clone. 0 when it ran.

        The scratch is created, filled from ``pg_dump --schema-only``, handed
        to a child ``schema:apply``, and dropped in a ``finally`` — a scratch
        left behind after a failed rehearsal would be the next run's confusing
        leftover, and worse, a database named after production sitting on the
        production server.
        """
        from cara.configuration import config  # local: heavy optional dep
        from cara.support import base_path  # local: heavy optional dep

        try:
            params = Scratch.connection_params(config)
        except ValueError as exc:
            self.error(str(exc))
            return 2

        source = params["database"]
        name = Scratch.derive_name(source, "rehearsal")
        try:
            Scratch.validate_name(name, source)
        except ScratchDatabaseException as exc:
            self.error(str(exc))
            return 2

        self.info("")
        self.info(f"── rehearsal (scratch: {name})")

        artifact = Path(tempfile.gettempdir()) / f"cara-rehearsal-{identifier}.json"
        self._write_artifact(artifact, identifier, operations, notices)

        try:
            try:
                Scratch.recreate(params, name)
                self.info(f"   cloning the shape of '{source}' (no rows)...")
                Scratch.clone_structure(params, source, name)
            except Exception as exc:
                self.error(f"   could not prepare the rehearsal scratch: {exc}")
                return 2

            self.info(f"   running all {len(operations)} operation(s) for real...")
            arguments = ["schema:apply", "--plan", str(artifact)]
            if self.option("allow_destructive"):
                arguments.append("--allow_destructive")
            code = Scratch.run_craft(arguments, name, base_path())
        finally:
            artifact.unlink(missing_ok=True)
            try:
                Scratch.drop(params, name)
            except Exception as exc:
                self.warning(
                    f"   could not drop the rehearsal scratch '{name}': {exc} "
                    f"— drop it manually."
                )

        if code:
            self.error(
                "REHEARSAL FAILED — this plan does not execute against a copy "
                "of the deployed schema, so it would fail partway through the "
                "real one. The failing operation is in the output above; "
                "nothing was applied to the deployed database."
            )
            return code

        self.success("   rehearsal clean: every operation executed on the clone.")
        return 0

    def _preflight(self, operation) -> str | None:
        """Answer the operation's data question against the live database."""
        from cara.facades import DB  # local: heavy optional dep

        try:
            rows = DB.select(operation.preflight_sql)
        except Exception:
            return None
        return operation.preflight_failure if rows else None

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
