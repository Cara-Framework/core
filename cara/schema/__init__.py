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
* ``Objects`` — which named objects a model owns, and whether the database
  already has them (asked of the right catalogue per kind).
* ``Planner`` — the difference between the two, as ordered operations.
* ``Scratch`` — a disposable database beside a real one, for proving both
  the acceptance invariant and a plan before it runs.
"""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "ADDITIVE": (".Operation", "ADDITIVE"),
    "ADD_COLUMN_RE": (".Vocabulary", "ADD_COLUMN_RE"),
    "DATETIME_CATEGORIES": (".Vocabulary", "DATETIME_CATEGORIES"),
    "DB_INT_RANK": (".Vocabulary", "DB_INT_RANK"),
    "DB_TYPE_CATEGORY": (".Vocabulary", "DB_TYPE_CATEGORY"),
    "DEFAULT_DECIMAL_PRECISION": (".Vocabulary", "DEFAULT_DECIMAL_PRECISION"),
    "DEFAULT_DECIMAL_SCALE": (".Vocabulary", "DEFAULT_DECIMAL_SCALE"),
    "DEFAULT_STRING_LENGTH": (".Vocabulary", "DEFAULT_STRING_LENGTH"),
    "DESTRUCTIVE": (".Operation", "DESTRUCTIVE"),
    "LEDGER_TABLE": (".Operation", "LEDGER_TABLE"),
    "LOCKING": (".Operation", "LOCKING"),
    "LiveSchema": (".LiveSchema", "LiveSchema"),
    "MODEL_INT_RANK": (".Vocabulary", "MODEL_INT_RANK"),
    "MODEL_TYPE_CATEGORY": (".Vocabulary", "MODEL_TYPE_CATEGORY"),
    "Operation": (".Operation", "Operation"),
    "POSTGRES_TYPE_SQL": (".Vocabulary", "POSTGRES_TYPE_SQL"),
    "PSEUDO_FIELD_EXPANSIONS": (".Vocabulary", "PSEUDO_FIELD_EXPANSIONS"),
    "RUN_MIGRATION_PREFIX": (".Operation", "RUN_MIGRATION_PREFIX"),
    "SAFETY_ORDER": (".Operation", "SAFETY_ORDER"),
    "SAFE_DB_NAME": (".Scratch", "SAFE_DB_NAME"),
    "admin_sql": (".Scratch", "admin_sql"),
    "as_dict": (".Operation", "as_dict"),
    "clone_structure": (".Scratch", "clone_structure"),
    "connection_params": (".Scratch", "connection_params"),
    "created_objects": (".Objects", "created_objects"),
    "declared_columns": (".LiveSchema", "declared_columns"),
    "derive_name": (".Scratch", "derive_name"),
    "drop": (".Scratch", "drop"),
    "from_dict": (".Operation", "from_dict"),
    "introspect": (".LiveSchema", "introspect"),
    "migration_to_run": (".Operation", "migration_to_run"),
    "missing_checks": (".Objects", "missing_checks"),
    "missing_indexes": (".Objects", "missing_indexes"),
    "orphaned_checks": (".Objects", "orphaned_checks"),
    "orphaned_indexes": (".Objects", "orphaned_indexes"),
    "orphaned_tables": (".Objects", "orphaned_tables"),
    "plan": (".Planner", "plan"),
    "plan_id": (".Operation", "plan_id"),
    "postgres_type": (".Vocabulary", "postgres_type"),
    "raw_sql_columns": (".LiveSchema", "raw_sql_columns"),
    "recreate": (".Scratch", "recreate"),
    "run_craft": (".Scratch", "run_craft"),
    "sort_operations": (".Operation", "sort_operations"),
    "sql_literal": (".LiveSchema", "sql_literal"),
    "validate_name": (".Scratch", "validate_name"),
}

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
    "LEDGER_TABLE",
    "LOCKING",
    "LiveSchema",
    "MODEL_INT_RANK",
    "MODEL_TYPE_CATEGORY",
    "Operation",
    "POSTGRES_TYPE_SQL",
    "PSEUDO_FIELD_EXPANSIONS",
    "RUN_MIGRATION_PREFIX",
    "SAFETY_ORDER",
    "SAFE_DB_NAME",
    "admin_sql",
    "as_dict",
    "clone_structure",
    "connection_params",
    "created_objects",
    "declared_columns",
    "derive_name",
    "drop",
    "from_dict",
    "introspect",
    "migration_to_run",
    "missing_checks",
    "missing_indexes",
    "orphaned_checks",
    "orphaned_indexes",
    "orphaned_tables",
    "plan",
    "plan_id",
    "postgres_type",
    "raw_sql_columns",
    "recreate",
    "run_craft",
    "sort_operations",
    "sql_literal",
    "validate_name",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
