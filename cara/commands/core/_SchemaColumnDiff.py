"""Column-level drift comparison for ``schema:check``.

Split out of ``SchemaCheckCommand`` as a sibling: the command owns the run and
the report, this owns the question "does this one column still match what the
model declares?". The live column is ``cara.schema.LiveSchema``'s and the
declared one ``cara.schema.declared_columns``'s — the planner's inputs.

Every WIDTH rule here is ONE-WAY on purpose — a live column with MORE room than
the model asks for loses nothing, so only the narrowing direction is drift. Three
widths are compared, and each was added after the previous omission cost
something real: text (a varchar(100) silently dropping trace rows), integer
(a model widened to big_integer overflowing on write) and decimal (a live
numeric(6,4) under a declared numeric(20,18), which rounded every proration
factor and, once a CHECK re-derived a total from the stored value, rejected
prorated invoices outright).
"""

from __future__ import annotations

from cara.exceptions import SchemaPlanRefused
from cara.schema import (
    DATETIME_CATEGORIES,
    DB_INT_RANK,
    DB_TYPE_CATEGORY,
    MODEL_INT_RANK,
    MODEL_TYPE_CATEGORY,
    declared_default_sql,
    defaults_match,
    live_default_sql,
)

# Types are compared through ``cara.schema.Vocabulary``'s coarse categories —
# the same tables the planner reads — and a mismatch is only flagged when the
# two categories clearly disagree. This is deliberately conservative: a false
# "drift" on every ``string``↔``character varying`` pair would make the gate
# noise and get it ignored. We'd rather miss a subtle type change than cry wolf.


class _SchemaColumnDiff:
    """The per-column half of the drift comparison."""

    def diff_column(self, table: str, name: str, declared: dict, live: dict) -> list[str]:
        """Compare a single shared column: nullability, type, width, default."""
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
        model_cat = MODEL_TYPE_CATEGORY.get(declared["type"])
        db_cat = DB_TYPE_CATEGORY.get(live["data_type"])
        if model_cat and db_cat and model_cat != db_cat:
            if {model_cat, db_cat} <= DATETIME_CATEGORIES:
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
        params = declared.get("params") or {}
        if model_cat == "numeric" and db_cat == "numeric":
            declared_precision = params.get("precision")
            declared_scale = params.get("scale")
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
            model_rank = MODEL_INT_RANK.get(declared["type"])
            db_rank = DB_INT_RANK.get(live["data_type"])
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

        issues.extend(self._diff_default(name, declared, live))
        return issues

    @staticmethod
    def _diff_default(name: str, declared: dict, live: dict) -> list[str]:
        """DEFAULT drift, compared as values through ``cara.schema.Defaults``.

        A column created by a from-zero migration carries its declared
        default; one evolved by hand or by an interrupted plan may not, and an
        INSERT that omits the column then stores NULL (or fails, NOT NULL) on
        one database and the default on the other. A raw-SQL column's shape is
        not known, so it is skipped. A default the model keeps as a Python
        expression has no SQL spelling to compare, and is reported with the
        planner's own refusal: skipped here, it was a green gate over a column
        ``schema:plan`` refuses on every run.
        """
        if declared["nullable"] is None:
            return []
        try:
            expected = declared_default_sql(declared.get("params"))
        except SchemaPlanRefused as refusal:
            return [f"column '{name}': {refusal}"]
        actual = live_default_sql(live.get("default"))
        if defaults_match(expected, actual):
            return []
        return [
            f"column '{name}' default differs: model={expected or 'none'}, "
            f"db={actual or 'none'}"
        ]

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
