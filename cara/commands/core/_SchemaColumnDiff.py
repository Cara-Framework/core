"""Column-level drift comparison for ``schema:check``.

Split out of ``SchemaCheckCommand`` as a sibling (the same shape
``_LiveSchemaInspection`` already uses): the command owns the run and the
report, this owns the question "does this one column still match what the
model declares?".

Every rule here is ONE-WAY on purpose — a live column with MORE room than the
model asks for loses nothing, so only the narrowing direction is drift. Three
widths are compared, and each was added after the previous omission cost
something real: text (a varchar(100) silently dropping trace rows), integer
(a model widened to big_integer overflowing on write) and decimal (a live
numeric(6,4) under a declared numeric(20,18), which rounded every proration
factor and, once a CHECK re-derived a total from the stored value, rejected
prorated invoices outright).
"""

from __future__ import annotations

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


class _SchemaColumnDiff:
    """The per-column half of the drift comparison."""

    def diff_column(
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

        # NARROWER DECIMAL WIDTH — the same silent-loss class as the integer
        # and varchar cases below/above, and the one that stayed open longest.
        # `information_schema` reports numeric width in its OWN columns, which
        # this inspector did not even read, so a live numeric(6,4) under a
        # model declaring numeric(20,18) was invisible: every write was
        # silently coerced, and once `invoice_amounts_check` began re-deriving
        # the total from the STORED factor, a prorated cancel-now invoice was
        # rejected outright — a 500 that left the customer unable to cancel
        # while metering kept running. Direction matters, exactly as it does
        # for text: a live column WIDER than declared is lenient and fine.
        if model_cat == "numeric" and db_cat == "numeric":
            declared_precision = declared.get("precision")
            declared_scale = declared.get("scale")
            live_precision = live.get("numeric_precision")
            live_scale = live.get("numeric_scale")
            if (
                declared_scale is not None
                and live_scale is not None
                and int(live_scale) < int(declared_scale)
            ):
                issues.append(
                    f"column '{name}' is NARROWER than declared: model="
                    f"{declared['type']}({declared_precision},{declared_scale}), "
                    f"db={live['data_type']}({live_precision},{live_scale}) — "
                    "writes are silently rounded to the live scale"
                )
            elif (
                declared_precision is not None
                and live_precision is not None
                and int(live_precision) < int(declared_precision)
            ):
                issues.append(
                    f"column '{name}' is NARROWER than declared: model="
                    f"{declared['type']}({declared_precision},{declared_scale}), "
                    f"db={live['data_type']}({live_precision},{live_scale}) — "
                    "oversized values are being rejected"
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


_SCHEMA_COLUMN_DIFF = _SchemaColumnDiff()
