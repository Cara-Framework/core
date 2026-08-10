"""The shared model↔database vocabulary: type categories, ranks, expansions.

``schema:check`` reports drift, ``schema:plan`` turns drift into operations,
and both have to agree on what "the same type" means. Stating these tables
once is not tidiness: two copies of a type map drift in exactly the way this
codebase has been bitten by before — a category added on the reporting side
and forgotten on the planning side means the planner emits an ALTER for a
column the checker considers fine, forever.

Everything here is data, deliberately: no imports, no I/O, no configuration.
"""

from __future__ import annotations

import re

#: Column names introduced by the raw-SQL ``__indexes__`` escape hatch. Models
#: declare GENERATED columns (a tsvector ``search_vector``, a partition-key
#: ``recorded_at``) the Blueprint ``fields()`` DSL cannot express, so those
#: columns never appear in ``model["fields"]``. Without recognising them the
#: live column reads as "present in database but NOT declared in model".
ADD_COLUMN_RE = re.compile(
    r"ADD\s+COLUMN\s+(?:IF\s+NOT\s+EXISTS\s+)?\"?(?P<col>\w+)\"?",
    re.IGNORECASE,
)

#: Model-declared field type -> coarse category.
MODEL_TYPE_CATEGORY = {
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

#: Live ``data_type`` (lower-cased) -> coarse category.
DB_TYPE_CATEGORY = {
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

#: The two datetime categories, so a naive↔aware mismatch can be reported with
#: a repair statement instead of the generic "type differs" line.
DATETIME_CATEGORIES = {"datetime_naive", "datetime_aware"}

#: Integer CAPACITY rank — the coarse "integer" category blurs
#: smallint/integer/bigint into one bucket, so a column WIDENED in the model
#: passes the category check silently while the live column stays too narrow.
MODEL_INT_RANK = {
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
DB_INT_RANK = {"smallint": 1, "integer": 2, "bigint": 3}

#: Field "types" that are not real columns by themselves — they expand into
#: one or more concrete columns at migration time.
PSEUDO_FIELD_EXPANSIONS = {
    "timestamps": [("created_at", "datetime"), ("updated_at", "datetime")],
    "soft_deletes": [("deleted_at", "datetime")],
}

#: Model field type -> the PostgreSQL type an evolve-mode ``ADD COLUMN`` emits.
#: Only types a planner may safely materialise on a deployed table appear here;
#: anything absent makes the planner refuse rather than guess a DDL fragment.
POSTGRES_TYPE_SQL = {
    "string": "VARCHAR({length})",
    "char": "CHAR({length})",
    "text": "TEXT",
    "tiny_text": "TEXT",
    "long_text": "TEXT",
    "enum": "VARCHAR({length})",
    "uuid": "UUID",
    "boolean": "BOOLEAN",
    "tiny_integer": "SMALLINT",
    "small_integer": "SMALLINT",
    "integer": "INTEGER",
    "medium_integer": "INTEGER",
    "unsigned_integer": "INTEGER",
    "integer_unsigned": "INTEGER",
    "big_integer": "BIGINT",
    "unsigned_big_integer": "BIGINT",
    "big_integer_unsigned": "BIGINT",
    "decimal": "NUMERIC({precision},{scale})",
    "unsigned_decimal": "NUMERIC({precision},{scale})",
    "double": "DOUBLE PRECISION",
    "float": "REAL",
    "json": "JSON",
    "jsonb": "JSONB",
    "binary": "BYTEA",
    "inet": "INET",
    "cidr": "CIDR",
    "macaddr": "MACADDR",
    "date": "DATE",
    "time": "TIME",
    "timestamp": "TIMESTAMP",
    "datetime": "TIMESTAMPTZ",
}

#: Defaults when a model declares a bounded type without one.
DEFAULT_STRING_LENGTH = 255
DEFAULT_DECIMAL_PRECISION = 10
DEFAULT_DECIMAL_SCALE = 2


def postgres_type(field_type: str, params: dict | None = None) -> str | None:
    """The PostgreSQL type for a model field, or ``None`` when unmappable.

    ``None`` is the honest answer for a type no planner should invent DDL for
    — the caller refuses the operation and asks a human, which is the whole
    difference between this and a tool that guesses.
    """
    template = POSTGRES_TYPE_SQL.get(field_type)
    if template is None:
        return None
    params = params or {}
    scale = params.get("scale")
    return template.format(
        length=params.get("length") or DEFAULT_STRING_LENGTH,
        precision=params.get("precision") or DEFAULT_DECIMAL_PRECISION,
        scale=DEFAULT_DECIMAL_SCALE if scale is None else scale,
    )


__all__ = [
    "ADD_COLUMN_RE",
    "DATETIME_CATEGORIES",
    "DB_INT_RANK",
    "DB_TYPE_CATEGORY",
    "DEFAULT_DECIMAL_PRECISION",
    "DEFAULT_DECIMAL_SCALE",
    "DEFAULT_STRING_LENGTH",
    "MODEL_INT_RANK",
    "MODEL_TYPE_CATEGORY",
    "POSTGRES_TYPE_SQL",
    "PSEUDO_FIELD_EXPANSIONS",
    "postgres_type",
]
