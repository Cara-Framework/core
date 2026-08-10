"""Read-only introspection of a deployed PostgreSQL schema.

``schema:check`` (report) and ``schema:plan`` (act) must be looking at the
same database in the same way, so the queries live here once. Everything is
SELECT-only by construction — nothing in this module can change a schema.
"""

from __future__ import annotations

from dataclasses import dataclass

from cara.schema.Vocabulary import ADD_COLUMN_RE, PSEUDO_FIELD_EXPANSIONS


def sql_literal(value: str) -> str:
    """Escape a value for inlining into an introspection query."""
    return str(value).replace("'", "''")


@dataclass(frozen=True)
class LiveSchema:
    """A snapshot of the deployed schema, as plain dictionaries."""

    tables: dict[str, dict]
    checks: dict[str, set[str]]
    indexes: dict[str, set[str]]
    constraint_indexes: dict[str, set[str]]
    #: EVERY constraint name, whatever its type. ``checks`` is deliberately
    #: only ``contype='c'`` because drift reporting compares declared CHECKs;
    #: a planner asking "does the database already have the object this
    #: ``__indexes__`` entry creates?" needs the whole set. Without it a
    #: FOREIGN KEY entry (``contype='f'``, and Postgres creates no index for
    #: one) reads as missing on every plan, forever.
    constraints: dict[str, set[str]] | None = None
    #: Trigger names per table, and schema-wide function names. A model's
    #: ``__indexes__`` entry may create either (the append-only audit trigger
    #: and the function it calls are declared exactly that way), and an object
    #: the introspection cannot see is one the planner re-creates every time.
    triggers: dict[str, set[str]] | None = None
    functions: set[str] | None = None
    #: Installed extension names. Schema-wide rather than per-table, and read
    #: because a model may install one through ``__indexes__`` — an entry
    #: labelled ``ext_pg_trgm`` creates an extension called ``pg_trgm``, so
    #: without this catalogue it reads as missing on every plan, forever.
    extensions: set[str] | None = None
    #: Relation name -> ``information_schema.tables.table_type``. ``tables``
    #: is built from ``information_schema.columns``, which does not
    #: distinguish a table from a VIEW — so without this, three cheapa views
    #: were reported as orphaned TABLES with the advice "drop it by hand",
    #: about relations the storefront reads on every request.
    relation_kinds: dict[str, str] | None = None

    def __post_init__(self):
        for name, empty in (
            ("constraints", {}),
            ("triggers", {}),
            ("functions", set()),
            ("extensions", set()),
            ("relation_kinds", {}),
        ):
            if getattr(self, name) is None:
                object.__setattr__(self, name, empty)

    def table_names(self) -> set[str]:
        """Every relation carrying columns, VIEWS INCLUDED — see
        :meth:`base_table_names` for the ones a model could own."""
        return set(self.tables)

    def base_table_names(self) -> set[str]:
        """Relations a model can legitimately be backed by.

        A relation whose kind was never read counts as a base table: this must
        not start hiding real orphans the day a Postgres release renames a
        ``table_type``, and a missing kind means "unknown", not "view".
        """
        return {
            name
            for name in self.tables
            if self.relation_kinds.get(name, "BASE TABLE") == "BASE TABLE"
        }

    def objects_on(self, table: str) -> set[str]:
        """Every named schema object the database already carries for ``table``.

        Objects that do NOT live in the table's namespace are kind-qualified
        (``extension:pg_trgm``, ``column:search_vector``) so a planner asking
        "is it already there?" asks the right catalogue. Unqualified names
        would collide across namespaces and, worse, silently miss: an
        extension is not an index, and no amount of looking through the index
        list will ever find one.
        """
        return (
            self.indexes.get(table, set())
            | self.checks.get(table, set())
            | self.constraints.get(table, set())
            | self.triggers.get(table, set())
            | self.functions
            | {f"extension:{name}" for name in self.extensions}
            | {f"column:{name}" for name in self.tables.get(table, {})}
        )


def introspect(live_schema, schema_name: str | None = None) -> LiveSchema:
    """Read columns, constraints, indexes, triggers, functions, extensions."""
    target = schema_name or live_schema.get_schema() or "public"
    run = live_schema.query_executor.get_query_result

    tables: dict[str, dict] = {}
    for row in (
        run(
            "SELECT table_name, column_name, data_type, is_nullable, "
            "character_maximum_length "
            "FROM information_schema.columns "
            f"WHERE table_schema = '{sql_literal(target)}' "
            "ORDER BY table_name, ordinal_position"
        )
        or []
    ):
        tables.setdefault(row["table_name"], {})[row["column_name"]] = {
            "data_type": (row["data_type"] or "").lower(),
            "is_nullable": (row["is_nullable"] or "").upper() == "YES",
            # None for unbounded types (text, jsonb, …).
            "max_length": row.get("character_maximum_length"),
        }

    checks: dict[str, set[str]] = {}
    constraints: dict[str, set[str]] = {}
    for row in (
        run(
            "SELECT c.relname AS table_name, con.conname AS constraint_name, "
            "con.contype AS constraint_type "
            "FROM pg_constraint con "
            "JOIN pg_class c ON c.oid = con.conrelid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            f"WHERE n.nspname = '{sql_literal(target)}' "
            "ORDER BY c.relname, con.conname"
        )
        or []
    ):
        constraints.setdefault(row["table_name"], set()).add(row["constraint_name"])
        if str(row["constraint_type"]) == "c":
            checks.setdefault(row["table_name"], set()).add(row["constraint_name"])

    indexes: dict[str, set[str]] = {}
    for row in (
        run(
            "SELECT tablename AS table_name, indexname AS index_name "
            "FROM pg_indexes "
            f"WHERE schemaname = '{sql_literal(target)}' "
            "ORDER BY tablename, indexname"
        )
        or []
    ):
        indexes.setdefault(row["table_name"], set()).add(row["index_name"])

    constraint_indexes: dict[str, set[str]] = {}
    for row in (
        run(
            "SELECT c.relname AS table_name, i.relname AS index_name "
            "FROM pg_constraint con "
            "JOIN pg_class c ON c.oid = con.conrelid "
            "JOIN pg_class i ON i.oid = con.conindid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            f"WHERE n.nspname = '{sql_literal(target)}' "
            "AND con.conindid <> 0 "
            "ORDER BY c.relname, i.relname"
        )
        or []
    ):
        constraint_indexes.setdefault(row["table_name"], set()).add(row["index_name"])

    triggers: dict[str, set[str]] = {}
    for row in (
        run(
            "SELECT c.relname AS table_name, t.tgname AS trigger_name "
            "FROM pg_trigger t "
            "JOIN pg_class c ON c.oid = t.tgrelid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            f"WHERE n.nspname = '{sql_literal(target)}' "
            "AND NOT t.tgisinternal "
            "ORDER BY c.relname, t.tgname"
        )
        or []
    ):
        triggers.setdefault(row["table_name"], set()).add(row["trigger_name"])

    functions = {
        row["function_name"]
        for row in run(
            "SELECT p.proname AS function_name "
            "FROM pg_proc p "
            "JOIN pg_namespace n ON n.oid = p.pronamespace "
            f"WHERE n.nspname = '{sql_literal(target)}'"
        )
        or []
    }

    relation_kinds = {
        row["table_name"]: (row["table_type"] or "").upper()
        for row in run(
            "SELECT table_name, table_type FROM information_schema.tables "
            f"WHERE table_schema = '{sql_literal(target)}'"
        )
        or []
    }

    extensions = {
        row["extension_name"]
        for row in run("SELECT extname AS extension_name FROM pg_extension") or []
    }

    return LiveSchema(
        tables=tables,
        checks=checks,
        indexes=indexes,
        constraint_indexes=constraint_indexes,
        constraints=constraints,
        triggers=triggers,
        functions=functions,
        extensions=extensions,
        relation_kinds=relation_kinds,
    )


def declared_columns(model: dict) -> dict[str, dict]:
    """Flatten a model's declared fields into concrete columns.

    Returns ``{column: {"type", "nullable", "length", "params"}}``, expanding
    the ``timestamps`` / ``soft_deletes`` pseudo-fields and registering the
    columns a model introduces through raw ``__indexes__`` SQL (GENERATED
    columns the Blueprint cannot express). Those raw columns carry
    ``type="__raw__"`` and ``nullable=None`` — "this column exists, and its
    shape is not cheaply knowable from the SQL" — so every consumer knows to
    assert existence only.
    """
    columns: dict[str, dict] = {}
    for field_name, field_def in (model.get("fields") or {}).items():
        field_type = field_def.get("type", field_name)
        params = field_def.get("params", {}) or {}

        if field_type in PSEUDO_FIELD_EXPANSIONS:
            for col_name, col_type in PSEUDO_FIELD_EXPANSIONS[field_type]:
                columns[col_name] = {
                    "type": col_type,
                    "nullable": True,
                    "length": None,
                    "params": {},
                }
            continue

        columns[field_name] = {
            "type": field_type,
            "nullable": bool(params.get("nullable", False)),
            "length": params.get("length"),
            "params": params,
        }

    if model.get("uses_soft_deletes") and "deleted_at" not in columns:
        columns["deleted_at"] = {
            "type": "datetime",
            "nullable": True,
            "length": None,
            "params": {},
        }

    for raw_col in raw_sql_columns(model):
        columns.setdefault(
            raw_col, {"type": "__raw__", "nullable": None, "length": None, "params": {}}
        )

    return columns


def raw_sql_columns(model: dict) -> set[str]:
    """Column names introduced by ``__indexes__`` raw-SQL ``ADD COLUMN``."""
    found: set[str] = set()
    for index in model.get("indexes", []) or []:
        for match in ADD_COLUMN_RE.finditer(index.get("up") or ""):
            found.add(match.group("col"))
    return found


__all__ = [
    "LiveSchema",
    "declared_columns",
    "introspect",
    "raw_sql_columns",
    "sql_literal",
]
