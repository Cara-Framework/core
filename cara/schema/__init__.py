"""Schema evolution: read a deployed database, derive the change, record it.

``cara.eloquent.schema`` builds schema (the Blueprint DSL migrations use).
This package is the other direction — comparing what a database HAS with what
the models DECLARE, and turning the difference into classified, reversible
operations. Regenerate mode (development) never needs it; evolve mode
(production) is built entirely on it.

Four modules, in dependency order:

* ``Vocabulary`` — the type tables both ``schema:check`` and ``schema:plan``
  read, stated once so a category added on one side cannot go missing on the
  other.
* ``LiveSchema`` — read-only introspection of a deployed database, plus the
  flattening of a model's declared fields into concrete columns.
* ``Operation`` — one change as a typed, safety-classified, reversible record.
* ``Planner`` — the difference between the two, as ordered operations.
"""

from cara.schema.LiveSchema import (
    LiveSchema,
    declared_columns,
    introspect,
    raw_sql_columns,
    sql_literal,
)
from cara.schema.Operation import (
    ADDITIVE,
    RUN_MIGRATION_PREFIX,
    DESTRUCTIVE,
    LOCKING,
    SAFETY_ORDER,
    Operation,
    migration_to_run,
    sort_operations,
)
from cara.schema.Planner import plan
from cara.schema.Vocabulary import (
    ADD_COLUMN_RE,
    DATETIME_CATEGORIES,
    DB_INT_RANK,
    DB_TYPE_CATEGORY,
    DEFAULT_DECIMAL_PRECISION,
    DEFAULT_DECIMAL_SCALE,
    DEFAULT_STRING_LENGTH,
    MODEL_INT_RANK,
    MODEL_TYPE_CATEGORY,
    POSTGRES_TYPE_SQL,
    PSEUDO_FIELD_EXPANSIONS,
    postgres_type,
)

__all__ = [
    "ADDITIVE",
    "ADD_COLUMN_RE",
    "DATETIME_CATEGORIES",
    "DB_INT_RANK",
    "DB_TYPE_CATEGORY",
    "DEFAULT_DECIMAL_PRECISION",
    "DEFAULT_DECIMAL_SCALE",
    "DEFAULT_STRING_LENGTH",
    "DESTRUCTIVE",
    "LOCKING",
    "LiveSchema",
    "MODEL_INT_RANK",
    "MODEL_TYPE_CATEGORY",
    "Operation",
    "POSTGRES_TYPE_SQL",
    "PSEUDO_FIELD_EXPANSIONS",
    "RUN_MIGRATION_PREFIX",
    "SAFETY_ORDER",
    "declared_columns",
    "introspect",
    "migration_to_run",
    "plan",
    "postgres_type",
    "raw_sql_columns",
    "sort_operations",
    "sql_literal",
]
