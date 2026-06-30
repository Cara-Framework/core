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
    many type aliases Postgres reports differently than we declare).

It is strictly READ-ONLY: it never issues DDL. Exit code is non-zero when drift
is found, so CI fails loudly. If no database is configured (or it's
unreachable), it skips cleanly with a clear message and exit 0 — mirroring how
``make:migration`` treats the optional ``db`` extra as a soft, actionable
condition rather than a crash.
"""

from __future__ import annotations

import re

from cara.commands import CommandBase, missing_optional
from cara.decorators import command

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

# --- Type normalisation -----------------------------------------------------
#
# ``information_schema.columns.data_type`` reports Postgres's canonical type
# names, which differ from the names we declare on models. We map BOTH sides to
# a small set of coarse "categories" and only flag a mismatch when the two
# categories clearly disagree. This is deliberately conservative: a false
# "drift" on every ``string``↔``character varying`` pair would make the gate
# noise and get it ignored. We'd rather miss a subtle type change than cry wolf.

# Model-declared field type -> coarse category.
_MODEL_TYPE_CATEGORY = {
    "string": "text",
    "char": "text",
    "text": "text",
    "tiny_text": "text",
    "long_text": "text",
    "enum": "text",
    "uuid": "uuid",
    "boolean": "boolean",
    "integer": "integer",
    "tiny_integer": "integer",
    "small_integer": "integer",
    "medium_integer": "integer",
    "big_integer": "integer",
    "unsigned_integer": "integer",
    "unsigned_big_integer": "integer",
    "integer_unsigned": "integer",
    "big_integer_unsigned": "integer",
    "small_integer_unsigned": "integer",
    "medium_integer_unsigned": "integer",
    "tiny_integer_unsigned": "integer",
    "increments": "integer",
    "big_increments": "integer",
    "tiny_increments": "integer",
    "decimal": "numeric",
    "unsigned_decimal": "numeric",
    "double": "numeric",
    "float": "numeric",
    "json": "json",
    "jsonb": "json",
    "binary": "binary",
    "inet": "inet",
    "cidr": "cidr",
    "macaddr": "macaddr",
    "date": "date",
    "time": "time",
    "timestamp": "datetime",
    "datetime": "datetime",
    "point": "point",
    "geometry": "geometry",
}

# Live ``data_type`` (lower-cased) -> coarse category.
_DB_TYPE_CATEGORY = {
    "character varying": "text",
    "varchar": "text",
    "character": "text",
    "char": "text",
    "text": "text",
    "uuid": "uuid",
    "boolean": "boolean",
    "smallint": "integer",
    "integer": "integer",
    "bigint": "integer",
    "numeric": "numeric",
    "decimal": "numeric",
    "double precision": "numeric",
    "real": "numeric",
    "json": "json",
    "jsonb": "json",
    "bytea": "binary",
    "inet": "inet",
    "cidr": "cidr",
    "macaddr": "macaddr",
    "date": "date",
    "time without time zone": "time",
    "time with time zone": "time",
    "timestamp without time zone": "datetime",
    "timestamp with time zone": "datetime",
    "point": "point",
}

# Field "types" that are not real columns by themselves — they expand into one
# or more concrete columns at migration time.
_PSEUDO_FIELD_EXPANSIONS = {
    "timestamps": [("created_at", "datetime"), ("updated_at", "datetime")],
    "soft_deletes": [("deleted_at", "datetime")],
}


@command(
    name="schema:check",
    help="Check for drift between model declarations and the live database schema.",
    options={
        "--c|connection=default": "The connection to introspect",
        "--schema=?": "The Postgres schema to introspect (defaults to the connection's)",
    },
)
class SchemaCheckCommand(CommandBase):
    def handle(self):
        """Compare every model's declared schema against the live database."""
        # Lazy DB import: ``cara.eloquent`` pulls psycopg2/faker (the optional
        # 'db' extra). Defer it so a DB-less service still imports this module,
        # and fail LOUD + actionable here if the extra isn't installed.
        try:
            from cara.eloquent.migrations import ModelDiscoverer
            from cara.eloquent.schema import Schema
        except ImportError as exc:
            raise missing_optional("db", exc) from exc

        connection = self.option("connection") or "default"
        schema_name = self.option("schema")

        self.info("Checking schema drift (models vs. live database)...")

        # Build a read-only Schema bound to the connection. If no database is
        # configured (or it's unreachable), skip cleanly rather than crash —
        # CI on a DB-less context must stay green.
        try:
            live_schema = Schema(connection=None, schema=schema_name).on(connection)
        except Exception as exc:  # noqa: BLE001 — any connection-resolution failure
            self.warning(
                "No usable database connection "
                f"('{connection}'): {exc}. Skipping schema:check."
            )
            return

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
        except Exception as exc:  # noqa: BLE001 — DB unreachable / introspection failed
            self.warning(
                f"Could not introspect the live database: {exc}. "
                "Skipping schema:check."
            )
            return

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

            drift = self._diff_table(declared, live_cols)
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
        """Read every column of every table in the target schema (read-only).

        Returns ``{table_name: {column_name: {"data_type", "is_nullable"}}}``.
        """
        target_schema = schema_name or live_schema.get_schema() or "public"

        sql = (
            "SELECT table_name, column_name, data_type, is_nullable "
            "FROM information_schema.columns "
            f"WHERE table_schema = '{self._sql_literal(target_schema)}' "
            "ORDER BY table_name, ordinal_position"
        )

        rows = live_schema.query_executor.get_query_result(sql) or []

        tables: dict[str, dict] = {}
        for row in rows:
            table_name = row["table_name"]
            tables.setdefault(table_name, {})[row["column_name"]] = {
                "data_type": (row["data_type"] or "").lower(),
                "is_nullable": (row["is_nullable"] or "").upper() == "YES",
            }
        return tables

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
            columns.setdefault(
                raw_col, {"type": "__raw__", "nullable": None}
            )

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

    # --- diff --------------------------------------------------------------

    def _diff_table(self, declared: dict, live: dict) -> list[str]:
        """Return human-readable drift issues for one table."""
        issues: list[str] = []

        declared_names = set(declared)
        live_names = set(live)

        for col in sorted(declared_names - live_names):
            issues.append(f"column '{col}' declared in model but MISSING in database")

        for col in sorted(live_names - declared_names):
            issues.append(f"column '{col}' present in database but NOT declared in model")

        for col in sorted(declared_names & live_names):
            issues.extend(self._diff_column(col, declared[col], live[col]))

        return issues

    def _diff_column(self, name: str, declared: dict, live: dict) -> list[str]:
        """Compare a single shared column: nullability + conservative type."""
        issues: list[str] = []

        # Nullable mismatch — cheap and high-signal. ``nullable is None`` means
        # "declared via raw SQL, nullability not cheaply known" → skip.
        if declared["nullable"] is not None and declared["nullable"] != live["is_nullable"]:
            model_null = "NULL" if declared["nullable"] else "NOT NULL"
            db_null = "NULL" if live["is_nullable"] else "NOT NULL"
            issues.append(
                f"column '{name}' nullability differs: model={model_null}, db={db_null}"
            )

        # Type mismatch — only when both sides map to KNOWN, DIFFERENT
        # categories. Unknown types on either side are skipped (no false
        # positives on aliases we haven't catalogued).
        model_cat = _MODEL_TYPE_CATEGORY.get(declared["type"])
        db_cat = _DB_TYPE_CATEGORY.get(live["data_type"])
        if model_cat and db_cat and model_cat != db_cat:
            issues.append(
                f"column '{name}' type differs: model={declared['type']} "
                f"(~{model_cat}), db={live['data_type']} (~{db_cat})"
            )

        return issues

    # --- output ------------------------------------------------------------

    def _summary(self, checked_tables: int, tables_with_drift: int, total_drift: int):
        self.info("\n" + "=" * 60)
        self.info(f"Checked {checked_tables} table(s) against the live database.")
        if total_drift:
            self.warning(
                f"⚠ Found {total_drift} drift issue(s) across "
                f"{tables_with_drift} table(s)."
            )
            self.warning(
                "Run 'python craft make:migration' to generate the missing "
                "migration(s), then 'python craft migrate'."
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
