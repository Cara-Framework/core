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
# index is not a correctness bug. That reasoning was wrong in one direction: 37
# synk indexes existed ONLY inside hand-written migrations, so nothing reported
# them and a regenerate-from-models sweep would drop them silently.
_CREATE_ANY_INDEX_RE = re.compile(
    r"CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:CONCURRENTLY\s+)?"
    r"(?:IF\s+NOT\s+EXISTS\s+)?\"?(?P<name>\w+)\"?",
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
    # Naive and aware are DELIBERATELY different categories: mixing them in one
    # expression forces a session-timezone-dependent (non-IMMUTABLE) cast, so an
    # index over e.g. COALESCE(aware_col, naive_col) cannot be built at all.
    "timestamp": "datetime_naive",
    "datetime": "datetime_aware",
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
    "timestamp without time zone": "datetime_naive",
    "timestamp with time zone": "datetime_aware",
    "point": "point",
}

# The two datetime categories, so a naive↔aware mismatch can be reported with a
# repair statement instead of the generic "type differs" line.
_DATETIME_CATEGORIES = {"datetime_naive", "datetime_aware"}

# Integer CAPACITY rank — the coarse "integer" category above blurs
# smallint/integer/bigint into one bucket, so a column WIDENED in the model
# (e.g. integer → big_integer for an id that will cross 2.1B) passes the
# category check silently while the live column stays too narrow. These
# ranks restore the one signal that matters: is the live column big enough
# to hold what the model now declares? Same data-loss-only direction as the
# varchar narrower-than-declared check.
_MODEL_INT_RANK = {
    "tiny_integer": 1,
    "tiny_increments": 1,
    "tiny_integer_unsigned": 1,
    "small_integer": 1,
    "small_integer_unsigned": 1,
    "integer": 2,
    "medium_integer": 2,
    "increments": 2,
    "unsigned_integer": 2,
    "integer_unsigned": 2,
    "medium_integer_unsigned": 2,
    "big_integer": 3,
    "big_increments": 3,
    "unsigned_big_integer": 3,
    "big_integer_unsigned": 3,
}
_DB_INT_RANK = {"smallint": 1, "integer": 2, "bigint": 3}

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
        "--allow_unavailable": "Explicitly skip when the target database is unavailable",
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
        """Read every column of every table in the target schema (read-only).

        Returns ``{table_name: {column_name: {"data_type", "is_nullable"}}}``.
        """
        target_schema = schema_name or live_schema.get_schema() or "public"

        sql = (
            "SELECT table_name, column_name, data_type, is_nullable, "
            "character_maximum_length "
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
                # None for unbounded types (text, jsonb, …).
                "max_length": row.get("character_maximum_length"),
            }
        return tables

    def _introspect_live_checks(self, live_schema, schema_name) -> dict[str, set[str]]:
        """Read every CHECK constraint NAME per table (read-only).

        ``pg_constraint.contype = 'c'`` is a CHECK constraint. NOT-NULL columns
        also surface as system CHECKs with auto-generated names; we only keep
        constraints whose name doesn't look auto-generated, and in practice we
        only DIFF the names a model explicitly DECLARES, so a stray system
        constraint never produces a false drift. Returns
        ``{table_name: {constraint_name, ...}}``.
        """
        target_schema = schema_name or live_schema.get_schema() or "public"

        sql = (
            "SELECT c.relname AS table_name, con.conname AS constraint_name "
            "FROM pg_constraint con "
            "JOIN pg_class c ON c.oid = con.conrelid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            f"WHERE n.nspname = '{self._sql_literal(target_schema)}' "
            "AND con.contype = 'c' "
            "ORDER BY c.relname, con.conname"
        )

        rows = live_schema.query_executor.get_query_result(sql) or []

        checks: dict[str, set[str]] = {}
        for row in rows:
            checks.setdefault(row["table_name"], set()).add(row["constraint_name"])
        return checks

    def _introspect_live_indexes(self, live_schema, schema_name) -> dict[str, set[str]]:
        """Read every index NAME per table (read-only).

        ``pg_indexes`` lists all indexes (unique + non-unique) by name. We diff
        only the names a model DECLARES as unique in ``__indexes__``, so listing
        every index here is harmless — a declared unique index simply must
        appear in this set. Returns ``{table_name: {index_name, ...}}``.
        """
        target_schema = schema_name or live_schema.get_schema() or "public"

        sql = (
            "SELECT tablename AS table_name, indexname AS index_name "
            "FROM pg_indexes "
            f"WHERE schemaname = '{self._sql_literal(target_schema)}' "
            "ORDER BY tablename, indexname"
        )

        rows = live_schema.query_executor.get_query_result(sql) or []

        indexes: dict[str, set[str]] = {}
        for row in rows:
            indexes.setdefault(row["table_name"], set()).add(row["index_name"])
        return indexes

    def _introspect_constraint_indexes(
        self, live_schema, schema_name
    ) -> dict[str, set[str]]:
        """Index names Postgres created IMPLICITLY to back a constraint.

        A PRIMARY KEY / UNIQUE / EXCLUDE constraint owns an index
        (``pg_constraint.conindid``) that no model ever names. Excluding these
        from the full index diff is what keeps "extra index in database"
        reporting free of false positives. Returns ``{table_name: {name, ...}}``.
        """
        target_schema = schema_name or live_schema.get_schema() or "public"

        sql = (
            "SELECT c.relname AS table_name, i.relname AS index_name "
            "FROM pg_constraint con "
            "JOIN pg_class c ON c.oid = con.conrelid "
            "JOIN pg_class i ON i.oid = con.conindid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            f"WHERE n.nspname = '{self._sql_literal(target_schema)}' "
            "AND con.conindid <> 0 "
            "ORDER BY c.relname, i.relname"
        )

        rows = live_schema.query_executor.get_query_result(sql) or []

        owned: dict[str, set[str]] = {}
        for row in rows:
            owned.setdefault(row["table_name"], set()).add(row["index_name"])
        return owned

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
        found: set[str] = set()
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
        """Compare a single shared column: nullability + conservative type."""
        issues: list[str] = []

        # Nullable mismatch — cheap and high-signal. ``nullable is None`` means
        # "declared via raw SQL, nullability not cheaply known" → skip.
        if (
            declared["nullable"] is not None
            and declared["nullable"] != live["is_nullable"]
        ):
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
            if {model_cat, db_cat} <= _DATETIME_CATEGORIES:
                issues.append(self._timezone_drift_message(table, name, model_cat))
            else:
                issues.append(
                    f"column '{name}' type differs: model={declared['type']} "
                    f"(~{model_cat}), db={live['data_type']} (~{db_cat})"
                )

        # NARROWER INTEGER CAPACITY — both sides land in the coarse "integer"
        # bucket, so a model widened to big_integer while the live column is
        # still integer/smallint passed SILENTLY (schema:check green, yet
        # values past the live column's range overflow on write). One-way,
        # data-loss-only — a model narrower than live is fine (lenient).
        if model_cat == "integer" and db_cat == "integer":
            model_rank = _MODEL_INT_RANK.get(declared["type"])
            db_rank = _DB_INT_RANK.get(live["data_type"])
            if model_rank and db_rank and model_rank > db_rank:
                issues.append(
                    f"column '{name}' is NARROWER than declared: model="
                    f"{declared['type']}, db={live['data_type']} — the live "
                    "column can't hold the model's full integer range"
                )

        # NARROWER-THAN-DECLARED capacity — the one length comparison that is
        # pure signal. The coarse categories above deliberately treat
        # string/varchar/text as one bucket, which let an undersized live
        # varchar hide behind a widened model FOREVER: pipeline_product_trace
        # kept varchar(100) job_ids while real ids ran 100+ chars, Postgres
        # rejected every long INSERT, and the fail-open writer silently
        # dropped ~3.8k trace rows before anything noticed. Direction matters:
        #   * live BOUNDED  + model UNBOUNDED (text/…)      -> drift (data loss)
        #   * live max_len  <  declared length              -> drift (data loss)
        #   * live WIDER than declared                      -> fine (lenient)
        # so this can never cry wolf on the aliases the categories blur.
        live_max = live.get("max_length")
        if live_max is not None and model_cat == "text" and db_cat == "text":
            declared_len = declared.get("length")
            if declared_len is None and declared["type"] in (
                "text",
                "tiny_text",
                "long_text",
            ):
                issues.append(
                    f"column '{name}' is NARROWER than declared: model="
                    f"{declared['type']} (unbounded), db={live['data_type']}"
                    f"({live_max}) — oversized writes are being rejected"
                )
            elif declared_len is not None and int(live_max) < int(declared_len):
                issues.append(
                    f"column '{name}' is NARROWER than declared: model="
                    f"{declared['type']}({declared_len}), db="
                    f"{live['data_type']}({live_max}) — oversized writes are "
                    "being rejected"
                )

        return issues

    @staticmethod
    def _timezone_drift_message(table: str, column: str, model_category: str) -> str:
        """Naive↔aware drift, reported with the exact repair statement.

        Direction matters: the model is the source of truth, so a model that
        declares ``datetime`` (aware) against a naive live column is repaired by
        WIDENING the column to timestamptz, interpreting the stored wall-clock
        values as UTC (the house rule). The reverse direction is a genuine
        model/DB disagreement we can only report — narrowing to naive discards
        the offset, so we refuse to hand out that statement casually.
        """
        if model_category == "datetime_aware":
            return (
                f"column '{column}' is timezone-NAIVE but the model declares a "
                f"timezone-AWARE datetime — an index or expression mixing it "
                f"with an aware column needs a non-IMMUTABLE cast and will fail "
                f"to build. Fix: ALTER TABLE {table} ALTER COLUMN {column} TYPE "
                f"timestamptz USING {column} AT TIME ZONE 'UTC';"
            )
        return (
            f"column '{column}' is timezone-AWARE in the database but the model "
            f"declares a naive timestamp. Either declare it as datetime (the "
            f"framework default, UTC everywhere) or, if naive is truly intended, "
            f"ALTER TABLE {table} ALTER COLUMN {column} TYPE timestamp USING "
            f"{column} AT TIME ZONE 'UTC'; — note this DISCARDS the offset."
        )

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
