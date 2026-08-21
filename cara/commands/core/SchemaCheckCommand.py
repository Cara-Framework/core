"""SchemaCheckCommand: detect drift between model declarations and the live DB.

The highest-value CI gate the migration tooling was missing. ``make:migration``
generates migrations from models, ``migrate`` applies them — but nothing verified
that the *live* Postgres schema actually matches what the models DECLARE. A
hand-edited migration, a half-applied ALTER, or a model field added without a
migration all leave the database silently out of sync with the code's
assumptions, surfacing only as a runtime ``column does not exist`` deep in a
request.

``schema:check`` introspects the live database (``information_schema.columns``,
``pg_indexes``) and compares it against every model's declared table + fields
(via the existing ``ModelDiscoverer``), reporting DRIFT:

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
  * CHECK constraints declared in a model's ``__indexes__`` but MISSING from
    live ``pg_constraint`` (a dropped CHECK otherwise passes silently),
  * INDEX drift in BOTH directions — every index a model declares (``__indexes__``
    raw SQL and ``field.index([...])``) diffed by name against live
    ``pg_indexes``. Missing catches a dropped ON-CONFLICT upsert target; EXTRA
    catches an index that lives only in a hand-written migration and would
    vanish on the next regenerate-from-models. Indexes Postgres creates
    implicitly to back a PK/UNIQUE/EXCLUDE constraint are excluded — no model
    names those.

It is strictly READ-ONLY: it never issues DDL. Exit code is non-zero when drift
is found, so CI fails loudly. If no database is configured (or it's
unreachable), it skips cleanly with a clear message and exit 0 — mirroring how
``make:migration`` treats the optional ``db`` extra as a soft, actionable
condition rather than a crash.
"""

from __future__ import annotations

import re

from cara.decorators import command

from ..CommandBase import CommandBase
from ..OptionalDependencyError import missing_optional
from ._LiveSchemaInspection import _LIVE_SCHEMA_INSPECTION
from ._SchemaColumnDiff import _SCHEMA_COLUMN_DIFF

# Harvest ``ADD COLUMN [IF NOT EXISTS] <name>`` from raw-SQL ``__indexes__``
# ``up`` clauses. Models declare GENERATED columns (e.g. a tsvector
# ``search_vector``, a partition-key ``recorded_at``) the Blueprint ``fields()``
# DSL can't express via the raw-SQL escape hatch, so those columns never appear
# in ``model["fields"]``. Without recognising them, the live DB column would be
# falsely flagged as "present in database but NOT declared in model".
_ADD_COLUMN_RE = re.compile(
    r"ADD\s+COLUMN\s+(?:IF\s+NOT\s+EXISTS\s+)?\"?(?P<col>\w+)\"?",
    re.IGNORECASE,
)

# Harvest declared CHECK constraints from ``__indexes__`` ``up`` SQL —
# ``ALTER TABLE <t> ADD CONSTRAINT <name> CHECK (...)``. The constraint NAME is
# what we diff against live ``pg_constraint`` (a dropped/renamed CHECK is the
# silent-pass we're closing). We don't compare the CHECK *expression* — Postgres
# rewrites it (parens, casts, COALESCE spelling) so an expression diff would cry
# wolf; presence-by-name is the high-signal, zero-false-positive gate.
_ADD_CHECK_RE = re.compile(
    r"ADD\s+CONSTRAINT\s+\"?(?P<name>\w+)\"?\s+CHECK\b",
    re.IGNORECASE,
)

# Harvest EVERY declared index name — unique and plain — from ``__indexes__``
# ``up`` SQL: ``CREATE [UNIQUE] INDEX [IF NOT EXISTS] <name> ON <t> (...)``. The
# NAME is what we diff against live ``pg_indexes``; the definition is not
# compared, because Postgres rewrites expressions (parens, casts, COALESCE
# spelling) and an expression diff would cry wolf.
#
# Plain indexes used to be excluded here on the grounds that a missing perf
# index is not a correctness bug. That reasoning was wrong in one direction: in
# a real product dozens of indexes existed ONLY inside hand-written migrations,
# so nothing reported them and a regenerate-from-models sweep would drop them
# silently.
_CREATE_ANY_INDEX_RE = re.compile(
    r"CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:CONCURRENTLY\s+)?"
    r"(?:IF\s+NOT\s+EXISTS\s+)?\"?(?P<name>\w+)\"?",
    re.IGNORECASE,
)

# Field "types" that are not real columns by themselves — they expand into one
# or more concrete columns at migration time.
_PSEUDO_FIELD_EXPANSIONS = {
    "timestamps": [("created_at", "datetime"), ("updated_at", "datetime")],
    "soft_deletes": [("deleted_at", "datetime")],
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
        except Exception as exc:  # noqa: BLE001 — any connection-resolution failure
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
            live_tables = self._introspect_live_tables(live_schema, schema_name)
            # CHECK constraints + unique indexes live in pg_constraint /
            # pg_indexes, NOT information_schema.columns — introspect them
            # separately so we can diff declared ``__indexes__`` against them.
            live_checks = self._introspect_live_checks(live_schema, schema_name)
            live_indexes = self._introspect_live_indexes(live_schema, schema_name)
            constraint_indexes = self._introspect_constraint_indexes(
                live_schema, schema_name
            )
        except Exception as exc:  # noqa: BLE001 — DB unreachable / introspection failed
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
            declared = self._declared_columns(model)
            if not declared:
                continue

            checked_tables += 1
            live_cols = live_tables.get(table)

            if live_cols is None:
                self.error(f"× {model['name']} ({table}): table MISSING from database")
                total_drift += 1
                tables_with_drift += 1
                continue

            drift = self._diff_table(table, declared, live_cols)
            # Constraint + unique-index drift: a model that DECLARES a CHECK or
            # a (partial-)unique index in ``__indexes__`` but whose live table
            # is MISSING it. A dropped ON-CONFLICT target or a dropped CHECK
            # otherwise passes silently — caught here.
            drift.extend(self._diff_checks(model, live_checks.get(table, set())))
            # Full index diff (both directions, constraint-owned excluded): an
            # index living only in a hand-written migration is invisible to the
            # column diff and silently disappears on regenerate-from-models.
            drift.extend(
                self._diff_indexes(
                    model,
                    live_indexes.get(table, set()),
                    constraint_indexes.get(table, set()),
                )
            )
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

    # --- introspection -----------------------------------------------------

    def _introspect_live_tables(self, live_schema, schema_name) -> dict[str, dict]:
        return _LIVE_SCHEMA_INSPECTION.tables(live_schema, schema_name)

    def _introspect_live_checks(self, live_schema, schema_name) -> dict[str, set[str]]:
        return _LIVE_SCHEMA_INSPECTION.checks(live_schema, schema_name)

    def _introspect_live_indexes(self, live_schema, schema_name) -> dict[str, set[str]]:
        return _LIVE_SCHEMA_INSPECTION.indexes(live_schema, schema_name)

    def _introspect_constraint_indexes(
        self, live_schema, schema_name
    ) -> dict[str, set[str]]:
        return _LIVE_SCHEMA_INSPECTION.constraint_indexes(live_schema, schema_name)

    # --- model side --------------------------------------------------------

    def _declared_columns(self, model: dict) -> dict[str, dict]:
        """Flatten a model's declared fields into concrete columns.

        Returns ``{column_name: {"type", "nullable"}}``, expanding the
        ``timestamps`` / ``soft_deletes`` pseudo-fields into their real columns.
        """
        columns: dict[str, dict] = {}
        for field_name, field_def in model["fields"].items():
            field_type = field_def.get("type", field_name)
            params = field_def.get("params", {}) or {}

            if field_type in _PSEUDO_FIELD_EXPANSIONS:
                # Pseudo-field (timestamps/soft_deletes) -> concrete columns,
                # all nullable timestamps.
                for col_name, col_type in _PSEUDO_FIELD_EXPANSIONS[field_type]:
                    columns[col_name] = {"type": col_type, "nullable": True}
                continue

            columns[field_name] = {
                "type": field_type,
                # Primary keys (the *increments family) and uniquely-keyed PKs
                # are NOT NULL; everything else honours the declared nullable.
                "nullable": bool(params.get("nullable", False)),
                # Declared capacity — None for unbounded types (text/jsonb) or
                # when the model omitted a length. Feeds the NARROWER-THAN-
                # DECLARED check in ``_diff_column``.
                "length": params.get("length"),
                # Decimal capacity is the same question asked of numbers, and
                # dropping it here is why a live numeric(6,4) under a declared
                # numeric(20,18) read as "no drift" while every write was
                # being silently rounded.
                "precision": params.get("precision"),
                "scale": params.get("scale"),
            }

        if model.get("uses_soft_deletes") and "deleted_at" not in columns:
            columns["deleted_at"] = {"type": "datetime", "nullable": True}

        # Columns added via the raw-SQL ``__indexes__`` escape hatch (GENERATED
        # columns the Blueprint can't express). These ARE declared by the model
        # — just not through ``fields()`` — so register them as known. Their
        # concrete type/nullable isn't introspectable cheaply from the raw SQL,
        # so mark the type unknown (skips the type check) and nullable=None
        # (skips the nullable check) — we only assert the column EXISTS.
        for raw_col in self._raw_sql_columns(model):
            columns.setdefault(raw_col, {"type": "__raw__", "nullable": None})

        return columns

    @staticmethod
    def _raw_sql_columns(model: dict) -> set[str]:
        """Column names introduced by ``__indexes__`` raw-SQL ``ADD COLUMN``."""
        found: set[str] = set()
        for index in model.get("indexes", []) or []:
            up_sql = index.get("up") or ""
            for match in _ADD_COLUMN_RE.finditer(up_sql):
                found.add(match.group("col"))
        return found

    @staticmethod
    def _declared_check_constraints(model: dict) -> set[str]:
        """CHECK constraint names declared in ``__indexes__`` ``up`` SQL.

        Prefers the regex-extracted ``ADD CONSTRAINT <name> CHECK`` name; falls
        back to the entry's own ``name`` field when the ``up`` SQL spells the
        CHECK in a form the regex doesn't catch (the Blueprint convention is
        that the entry ``name`` IS the constraint name).
        """
        table = model.get("table") or ""
        found: set[str] = set()
        for check in model.get("checks", []) or []:
            name = check.get("name")
            if name:
                found.add(name)
                continue
            expression = check.get("expression") or ""
            slug = re.sub(r"[^a-zA-Z0-9]+", "_", expression).strip("_").lower()
            found.add(f"{table}_{slug or 'check'}_check"[:63])
        for index in model.get("indexes", []) or []:
            up_sql = index.get("up") or ""
            matched = False
            for match in _ADD_CHECK_RE.finditer(up_sql):
                found.add(match.group("name"))
                matched = True
            # ``ADD CONSTRAINT <name> CHECK`` that the regex missed but the SQL
            # clearly is a CHECK: trust the declared entry name.
            if not matched and re.search(r"\bCHECK\b", up_sql, re.IGNORECASE):
                name = index.get("name")
                if name:
                    found.add(name)
        return found

    @staticmethod
    def _declared_indexes(model: dict) -> set[str]:
        """EVERY index name the model declares, by whichever route.

        Two routes exist and both are the model's own declaration:
          * ``__indexes__`` raw SQL — ``CREATE [UNIQUE] INDEX <name> ...``,
          * ``fields()`` ``field.index([...])`` — collected by the discoverer as
            ``composite_indexes``, whose live name is the entry's ``name`` or the
            ConstraintManager default ``<table>_<cols joined by _>_index``.

        ``composite_uniques`` are deliberately absent: those become table-level
        UNIQUE CONSTRAINTS, whose backing index is filtered out on the live side
        as constraint-owned.
        """
        found: set[str] = set()
        for index in model.get("indexes", []) or []:
            up_sql = index.get("up") or ""
            for match in _CREATE_ANY_INDEX_RE.finditer(up_sql):
                found.add(match.group("name"))

        table = model.get("table") or ""
        for declaration in model.get("composite_indexes", []) or []:
            columns = declaration.get("columns") or []
            name = declaration.get("name") or f"{table}_{'_'.join(columns)}_index"
            # Postgres truncates identifiers at 63 bytes, so the live name of a
            # long auto-derived index differs from the one we just built.
            found.add(name[:63])
        return found

    def _diff_indexes(
        self, model: dict, live_indexes: set[str], constraint_indexes: set[str]
    ) -> list[str]:
        """Report index drift in BOTH directions, by name.

        Indexes backing a PK/UNIQUE/EXCLUDE constraint are excluded: Postgres
        names those itself and no model declares them, so counting them would
        make every table report phantom extras.
        """
        declared = self._declared_indexes(model)
        standalone = live_indexes - constraint_indexes

        issues: list[str] = []
        for name in sorted(declared - standalone):
            issues.append(f"index '{name}' declared in model but MISSING in database")
        for name in sorted(standalone - declared):
            issues.append(
                f"index '{name}' present in database but NOT declared in model "
                f"— add it to __indexes__ or drop it; a regenerate-from-models "
                f"sweep will not recreate it"
            )
        return issues

    def _diff_checks(self, model: dict, live_checks: set[str]) -> list[str]:
        """Report declared CHECK constraints MISSING from the DB.

        One direction only — declared-but-absent — because a model is the source
        of truth for the invariants it asserts, while extra live CHECKs (system
        NOT-NULL constraints among them) are not the model's concern.
        Index drift is handled by ``_diff_indexes``, which reports BOTH
        directions.
        """
        return [
            f"CHECK constraint '{name}' declared in model but MISSING in database"
            for name in sorted(self._declared_check_constraints(model) - live_checks)
        ]

    # --- diff --------------------------------------------------------------

    def _diff_table(self, table: str, declared: dict, live: dict) -> list[str]:
        """Return human-readable drift issues for one table."""
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

    # --- helpers -----------------------------------------------------------

    @staticmethod
    def _sql_literal(value: str) -> str:
        """Escape a string for safe inlining into an SQL literal.

        The introspection query targets a schema name we control (the
        connection's configured schema or 'public'), never user input — but
        escape single quotes anyway so an unusual schema name can't break the
        query or smuggle SQL.
        """
        return str(value).replace("'", "''")
