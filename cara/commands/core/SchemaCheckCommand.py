"""SchemaCheckCommand: detect drift between model declarations and the live DB.

The highest-value CI gate the migration tooling was missing. ``make:migration``
generates migrations from models, ``migrate`` applies them — but nothing verified
that the *live* Postgres schema actually matches what the models DECLARE. A
hand-edited migration, a half-applied ALTER, or a model field added without a
migration all leave the database silently out of sync with the code's
assumptions, surfacing only as a runtime ``column does not exist`` deep in a
request.

``schema:check`` introspects the live database and compares it against every
model's declared table + fields (via the existing ``ModelDiscoverer``),
reporting DRIFT:

  * columns a model declares but the DB is MISSING,
  * columns present in the DB but ABSENT from the model,
  * NULLABLE mismatches (model says NOT NULL, DB allows NULL, or vice-versa),
  * conservative TYPE mismatches (only flagged when the declared and live types
    normalise to clearly different categories — avoids false positives on the
    many type aliases Postgres reports differently than we declare),
  * TIMEZONE drift — a naive ``timestamp`` column where the model declares a
    tz-aware ``datetime`` (or vice versa), reported with the exact repair
    ``ALTER``; mixing the two in one expression needs a non-IMMUTABLE cast, so
    an index over e.g. ``COALESCE(last_seen_at, created_at)`` cannot build,
  * DEFAULT drift — a column whose deployed DEFAULT is not the declared one, so
    an INSERT that omits it stores something else than a from-zero database
    would (or fails there, NOT NULL with no default),
  * named objects a model declares — indexes, CHECK and UNIQUE constraints,
    FOREIGN KEYs (``field.foreign(...)`` and raw ``ADD CONSTRAINT``), and every
    trigger, function and extension an ``__indexes__`` entry creates — but
    MISSING from the database. A composite ``(child_id, tenant_id)`` key is
    where tenant isolation stops being a query habit and becomes a storage
    guarantee, and a role-scope trigger is where the database itself refuses a
    cross-tenant grant; without this comparison a database can be missing them
    and still be reported "in sync",
  * EXTRA indexes — a live index no model declares would vanish on the next
    regenerate-from-models. Indexes Postgres creates implicitly to back a
    PK/UNIQUE/EXCLUDE constraint are excluded — no model names those.

Introspection, the model's columns and the named-object comparison are all
``cara.schema`` — the same functions ``schema:plan`` derives operations from.
A checker with its own copy reported "No drift" over a database missing the
triggers the planner would have re-created, and the two tools must never
disagree about what is there.

It is strictly READ-ONLY: it never issues DDL. Exit code is non-zero when drift
is found, so CI fails loudly. If no database is configured (or it's
unreachable), it fails unless ``--allow_unavailable`` asks it to skip.
"""

from __future__ import annotations

from cara.decorators import command
from cara.exceptions import SchemaPlanRefused
from cara.schema import (
    created_objects,
    declared_columns,
    introspect,
    missing_checks,
    missing_foreign_keys,
    missing_indexes,
    orphaned_indexes,
)

from ..CommandBase import CommandBase
from ..OptionalDependencyError import missing_optional
from ._SchemaColumnDiff import _SCHEMA_COLUMN_DIFF

#: How the planner's "add a missing constraint" operations read as drift. Their
#: ``validate_*`` halves restate the same constraint and are not reported again.
_MISSING_CONSTRAINT_NOUNS = {
    "add_check": "CHECK constraint",
    "add_foreign_key": "FOREIGN KEY",
    "add_unique_constraint": "UNIQUE constraint",
}


@command(
    name="schema:check",
    help="Check for drift between model declarations and the live database schema.",
    options=[
        {
            "name": "-c|--connection",
            "help": "The connection to introspect",
            "type": str,
            "default": "default",
            "is_flag": False,
        },
        {
            "name": "--schema",
            "help": "The Postgres schema to introspect (defaults to the connection's)",
            "type": str,
            "default": None,
            "is_flag": False,
        },
        {
            "name": "--allow_unavailable",
            "help": "Explicitly skip when the target database is unavailable",
            "type": bool,
            "default": False,
            "is_flag": True,
        },
    ],
)
class SchemaCheckCommand(CommandBase):
    def handle(self):
        """Compare every model's declared schema against the live database."""
        # Lazy DB import: ``cara.eloquent`` pulls psycopg2/faker (the optional
        # 'db' extra). Defer it so a DB-less service still imports this module,
        # and fail LOUD + actionable here if the extra isn't installed.
        try:
            from cara.eloquent.migrations import (  # local: heavy optional dep
                ModelDiscoverer,
            )
            from cara.eloquent.schema import Schema  # local: heavy optional dep
        except ImportError as exc:
            raise missing_optional("db", exc) from exc

        connection = self.option("connection") or "default"
        schema_name = self.option("schema")

        self.info("Checking schema drift (models vs. live database)...")

        # Build a read-only Schema bound to the connection. If no database is
        # configured (or it is unreachable), fail by default. A green drift
        # gate that checked nothing is more dangerous than a failed pipeline.
        try:
            live_schema = Schema(connection=None, schema=schema_name).on(connection)
        except Exception as exc:  # any connection-resolution failure
            message = f"No usable database connection ('{connection}'): {exc}."
            if self.option("allow_unavailable"):
                self.warning(f"{message} Skipping by explicit request.")
                return 0
            self.error(message)
            return 2

        # Discover models (table + declared fields). Independent of the
        # comparator/generator by design.
        models = ModelDiscoverer().discover_models()
        checkable = [
            m
            for m in models
            if m.get("has_fields_method") and m.get("table") and m.get("fields")
        ]
        if not checkable:
            self.warning("No models with declared fields found; nothing to check.")
            return

        try:
            live = introspect(live_schema, schema_name)
        except Exception as exc:  # DB unreachable / introspection failed
            message = f"Could not introspect the live database: {exc}."
            if self.option("allow_unavailable"):
                self.warning(f"{message} Skipping by explicit request.")
                return 0
            self.error(message)
            return 2

        total_drift = 0
        tables_with_drift = 0
        checked_tables = 0

        for model in sorted(checkable, key=lambda m: m["table"]):
            table = model["table"]
            declared = declared_columns(model)
            if not declared:
                continue

            checked_tables += 1
            live_cols = live.tables.get(table)

            if live_cols is None:
                self.error(f"× {model['name']} ({table}): table MISSING from database")
                total_drift += 1
                tables_with_drift += 1
                continue

            drift = self._diff_table(table, declared, live_cols)
            drift.extend(self._diff_objects(model, table, live))
            if drift:
                tables_with_drift += 1
                total_drift += len(drift)
                self.warning(f"\nDrift in {model['name']} ({table}):")
                for issue in drift:
                    self.info(f"   • {issue}")

        self._summary(checked_tables, tables_with_drift, total_drift)

        if total_drift:
            # Non-zero exit so CI fails on drift. CommandRunner maps an int
            # return into ``typer.Exit(code=...)``.
            return 1

    # --- diff --------------------------------------------------------------

    def _diff_table(self, table: str, declared: dict, live: dict) -> list[str]:
        """Return human-readable column drift issues for one table."""
        issues: list[str] = []

        declared_names = set(declared)
        live_names = set(live)

        for col in sorted(declared_names - live_names):
            issues.append(f"column '{col}' declared in model but MISSING in database")

        for col in sorted(live_names - declared_names):
            issues.append(f"column '{col}' present in database but NOT declared in model")

        for col in sorted(declared_names & live_names):
            issues.extend(self._diff_column(table, col, declared[col], live[col]))

        return issues

    def _diff_column(
        self, table: str, name: str, declared: dict, live: dict
    ) -> list[str]:
        """Compare one shared column — see ``_SchemaColumnDiff``."""
        return _SCHEMA_COLUMN_DIFF.diff_column(table, name, declared, live)

    def _diff_objects(self, model: dict, table: str, live) -> list[str]:
        """Named-object drift for one table, asked the way the planner asks it.

        MISSING is exactly what ``schema:plan`` would create — the operations
        ``cara.schema.Objects`` derives, reported instead of run — so a trigger,
        a function or a raw foreign key the database lacks is drift here
        precisely when it is work there. An ``__indexes__`` entry whose SQL
        names nothing readable is reported with the planner's own refusal
        rather than skipped. EXTRA is the planner's orphaned-index rule; extra
        CHECKs and foreign keys stay unreported, because a model owns the
        invariants it asserts, not every one the database carries.
        """
        issues: list[str] = []
        try:
            wanted = missing_indexes(model, table, live)
        except SchemaPlanRefused as refusal:
            issues.append(str(refusal))
            wanted = []
        wanted += missing_checks(model, table, live)
        wanted += missing_foreign_keys(model, table, live)

        present = live.objects_on(table)
        reported: set[str] = set()
        for operation in wanted:
            noun = _MISSING_CONSTRAINT_NOUNS.get(operation.kind)
            if noun is not None:
                names = {operation.key.split(":")[1]}
            elif operation.kind == "create_index":
                noun = "object"
                # A missing raw column is the column diff's to report.
                names = {
                    name
                    for name in (created_objects(operation.forward_sql) or set())
                    - present
                    if not name.startswith("column:")
                }
            else:
                continue
            for name in sorted(names - reported):
                reported.add(name)
                issues.append(
                    f"{noun} '{name}' declared in model but MISSING in database"
                )

        for operation in orphaned_indexes(model, table, live):
            issues.append(
                f"index '{operation.key.split(':')[1]}' present in database but "
                f"NOT declared in model — add it to __indexes__ or drop it; a "
                f"regenerate-from-models sweep will not recreate it"
            )
        return issues

    def _summary(self, checked_tables: int, tables_with_drift: int, total_drift: int):
        self.info("\n" + "=" * 60)
        self.info(f"Checked {checked_tables} table(s) against the live database.")
        if total_drift:
            self.warning(
                f"⚠ Found {total_drift} drift issue(s) across "
                f"{tables_with_drift} table(s)."
            )
            self.warning(
                "Align the two sides: edit the model, or ALTER the development "
                "database to match it. Then regenerate the directory with "
                "'python craft make:migration --overwrite' and prove the loop "
                "with 'python craft schema:verify'. (Bare make:migration only "
                "REPORTS model↔directory drift; it never writes.)"
            )
        else:
            self.success("No drift — models and database are in sync!")
