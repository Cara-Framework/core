"""Read-only introspection of a deployed PostgreSQL schema.

``schema:check`` (report) and ``schema:plan`` (act) must be looking at the
same database in the same way, so the queries live here once. Everything is
SELECT-only by construction — nothing in this module can change a schema. The
one write introspection needs, the TEMP-table probe that has the server spell
the model's declarations, lives in :mod:`cara.schema.DeclaredSpelling`.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from cara.schema.LivePartitions import read_partitioning, without_partitions
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
    #: The server's own text of every CHECK and UNIQUE constraint
    #: (``pg_get_constraintdef``), per table and name. A name says a CHECK
    #: EXISTS; only its definition says whether it still asserts what the
    #: model declares — comparing names alone let a CHECK keep a value the
    #: model had dropped (``catalog_proposal.kind = 'quantity_review'``) on a
    #: database every gate called clean. It is also the reverse of a drop.
    #: FOREIGN KEYs are here for the same reason: an ``on_delete`` a model
    #: changed keeps the constraint's name.
    constraint_definitions: dict[str, dict[str, str]] | None = None
    #: UNIQUE constraint names per table (``contype='u'``). The orphaned-index
    #: rule skips the index BEHIND a constraint, so without this a unique rule
    #: no model declares — one left behind by a column rename — enforced on
    #: every write while both gates read clean.
    unique_constraints: dict[str, set[str]] | None = None
    #: The model's CHECKs as the SERVER spells them, per table and name, for
    #: the CHECKs the database also has (:func:`normalize_declared_checks`).
    #: A model writes ``kind IN ('a', 'b')``; Postgres stores
    #: ``((kind)::text = ANY ((ARRAY['a'::character varying, ...])::text[]))``,
    #: so the only faithful comparison is the server deparsing both. Empty
    #: when the probe could not run — then no CHECK is called changed.
    declared_check_definitions: dict[str, dict[str, str]] | None = None
    #: The LIVE CHECK definitions, attached once more on the probe and read
    #: back — the CHECK side of :attr:`respelled_index_definitions`. A database
    #: restored from a dump stores ``= ANY (ARRAY[('a'::varchar)::text])`` where
    #: a migrated one stores ``= ANY ((ARRAY['a'::varchar])::text[])``.
    respelled_check_definitions: dict[str, dict[str, str]] | None = None
    #: The server's own text of every index (``pg_indexes.indexdef``), per
    #: table and name. A name says an index EXISTS; only its definition says
    #: whether it still covers what the model declares — comparing names
    #: alone let ``message_thread_assignee_idx`` sit on ``(tenant_id,
    #: assigned_to_user_id)`` under a model declaring the reverse order, on a
    #: database every gate called clean. It is also the reverse of a reshape.
    index_definitions: dict[str, dict[str, str]] | None = None
    #: Indexes with ``indisvalid = false`` — what an interrupted ``CREATE
    #: INDEX CONCURRENTLY`` leaves behind: maintained on every write, read by
    #: no query, and present by name, so a name comparison calls it done.
    invalid_indexes: dict[str, set[str]] | None = None
    #: The model's indexes as the SERVER spells them, per table and name, for
    #: the indexes the database also has (:func:`normalize_declared_indexes`).
    #: Empty when the probe could not run — then no index is called changed.
    declared_index_definitions: dict[str, dict[str, str]] | None = None
    #: The LIVE index definitions, built once more on the probe and read back.
    #: A stored definition re-parsed is not always the same text: the cast in
    #: ``(ARRAY['a'::varchar])::text[]`` moves into the elements, which is
    #: exactly what an index rebuilt by ``ALTER COLUMN TYPE`` or restored from
    #: a dump stores. Both sides are compared at that fixpoint.
    respelled_index_definitions: dict[str, dict[str, str]] | None = None
    #: Explicit per-column statistics targets (``ALTER COLUMN ... SET
    #: STATISTICS n``), per table and column. A target of 0 counts only while
    #: no sample of the column remains: ANALYZE at target 0 skips the column
    #: but keeps the sample it took before, which still steers the planner.
    column_statistics: dict[str, dict[str, int]] | None = None
    #: Trigger and function BODIES (``cara.schema.NamedDdlBodies``): a name
    #: says the object exists, only its definition says what it still does.
    trigger_definitions: dict[str, dict[str, str]] | None = None
    declared_trigger_definitions: dict[str, dict[str, str]] | None = None
    function_definitions: dict[str, list[dict]] | None = None
    #: Triggers that exist but do not fire (``pg_trigger.tgenabled`` 'D' or
    #: 'R'), per table: name -> (state, FOREIGN KEY it enforces, the table
    #: that key constrains, ``pg_get_constraintdef``), the last three None
    #: for a model trigger.
    disabled_triggers: (
        dict[str, dict[str, tuple[str, str | None, str | None, str | None]]] | None
    ) = None

    partitioning: dict[str, dict] | None = None  # cara.schema.LivePartitions

    def __post_init__(self):
        for name, empty in (
            ("partitioning", {}),
            ("constraints", {}),
            ("triggers", {}),
            ("functions", set()),
            ("extensions", set()),
            ("relation_kinds", {}),
            ("constraint_definitions", {}),
            ("unique_constraints", {}),
            ("declared_check_definitions", {}),
            ("respelled_check_definitions", {}),
            ("index_definitions", {}),
            ("invalid_indexes", {}),
            ("declared_index_definitions", {}),
            ("respelled_index_definitions", {}),
            ("column_statistics", {}),
            ("trigger_definitions", {}),
            ("declared_trigger_definitions", {}),
            ("function_definitions", {}),
            ("disabled_triggers", {}),
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
            | {
                f"statistics:{column}={target}"
                for column, target in self.column_statistics.get(table, {}).items()
            }
        )


def introspect(live_schema, schema_name: str | None = None) -> LiveSchema:
    """Read columns, constraints, indexes, triggers, functions, extensions."""
    target = schema_name or live_schema.get_schema() or "public"
    run = live_schema.query_executor.get_query_result

    tables: dict[str, dict] = {}
    for row in (
        run(
            "SELECT table_name, column_name, data_type, is_nullable, "
            "character_maximum_length, numeric_precision, numeric_scale, "
            "column_default, udt_name, "
            # The full spelling, element modifier included: ``udt_name`` of a
            # ``varchar(20)[]`` is ``_varchar`` and information_schema has no
            # element length anywhere.
            "(SELECT pg_catalog.format_type(a.atttypid, a.atttypmod) "
            "FROM pg_catalog.pg_attribute a "
            "JOIN pg_catalog.pg_class c ON c.oid = a.attrelid "
            "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = columns.table_schema "
            "AND c.relname = columns.table_name "
            "AND a.attname = columns.column_name) AS formatted_type "
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
            # Numeric width lives in its own information_schema columns. Not
            # reading them let a live numeric(6,4) sit under a model declaring
            # numeric(20,18) with both gates calling it clean.
            "numeric_precision": row.get("numeric_precision"),
            "numeric_scale": row.get("numeric_scale"),
            # The server's own spelling (``'USD'::character varying``); read
            # it through ``cara.schema.Defaults``. Not reading it at all let
            # a column lose its DEFAULT with every gate calling it clean.
            "default": row.get("column_default"),
            # An array's element (``_int8``) or an enum's name: what a dropped
            # column's reverse needs to come back as the same type.
            "udt_name": row.get("udt_name"),
            "formatted_type": row.get("formatted_type"),
        }

    checks: dict[str, set[str]] = {}
    constraints: dict[str, set[str]] = {}
    constraint_definitions: dict[str, dict[str, str]] = {}
    unique_constraints: dict[str, set[str]] = {}
    for row in (
        run(
            "SELECT c.relname AS table_name, con.conname AS constraint_name, "
            "con.contype AS constraint_type, "
            "pg_get_constraintdef(con.oid) AS definition "
            "FROM pg_constraint con "
            "JOIN pg_class c ON c.oid = con.conrelid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            f"WHERE n.nspname = '{sql_literal(target)}' "
            "ORDER BY c.relname, con.conname"
        )
        or []
    ):
        table_name, constraint_name = row["table_name"], row["constraint_name"]
        constraint_type = str(row["constraint_type"])
        constraints.setdefault(table_name, set()).add(constraint_name)
        if constraint_type == "c":
            checks.setdefault(table_name, set()).add(constraint_name)
        if constraint_type == "u":
            unique_constraints.setdefault(table_name, set()).add(constraint_name)
        if constraint_type in ("c", "u", "f") and row.get("definition"):
            constraint_definitions.setdefault(table_name, {})[constraint_name] = str(
                row["definition"]
            )

    indexes: dict[str, set[str]] = {}
    index_definitions: dict[str, dict[str, str]] = {}
    for row in (
        run(
            "SELECT tablename AS table_name, indexname AS index_name, "
            "indexdef AS definition "
            "FROM pg_indexes "
            f"WHERE schemaname = '{sql_literal(target)}' "
            "ORDER BY tablename, indexname"
        )
        or []
    ):
        indexes.setdefault(row["table_name"], set()).add(row["index_name"])
        if row.get("definition"):
            index_definitions.setdefault(row["table_name"], {})[row["index_name"]] = str(
                row["definition"]
            )

    invalid_indexes: dict[str, set[str]] = {}
    for row in (
        run(
            "SELECT c.relname AS table_name, i.relname AS index_name "
            "FROM pg_index x "
            "JOIN pg_class i ON i.oid = x.indexrelid "
            "JOIN pg_class c ON c.oid = x.indrelid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            f"WHERE n.nspname = '{sql_literal(target)}' "
            # A partitioned parent's index (``I``) is legitimately invalid
            # until every partition's index is attached; only a plain index
            # being invalid means a build died.
            "AND i.relkind = 'i' AND NOT x.indisvalid "
            "ORDER BY c.relname, i.relname"
        )
        or []
    ):
        invalid_indexes.setdefault(row["table_name"], set()).add(row["index_name"])

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

    # NULL (17+) / -1 (older) is the default target; only an explicit one is
    # declared. A 0 target over a sample that is still there is not the
    # object a model declares (see ``LiveSchema.column_statistics``).
    column_statistics: dict[str, dict[str, int]] = {}
    for row in (
        run(
            "SELECT c.relname AS table_name, a.attname AS column_name, "
            "a.attstattarget::int AS target, EXISTS (SELECT 1 FROM pg_stats s "
            "WHERE s.schemaname = n.nspname AND s.tablename = c.relname "
            "AND s.attname = a.attname) AS sampled "
            "FROM pg_attribute a "
            "JOIN pg_class c ON c.oid = a.attrelid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            f"WHERE n.nspname = '{sql_literal(target)}' "
            "AND c.relkind IN ('r', 'p') AND a.attnum > 0 "
            "AND NOT a.attisdropped AND a.attstattarget >= 0 "
            "ORDER BY c.relname, a.attname"
        )
        or []
    ):
        if int(row["target"]) == 0 and row.get("sampled"):
            continue
        column_statistics.setdefault(row["table_name"], {})[row["column_name"]] = int(
            row["target"]
        )

    live = LiveSchema(
        tables=tables,
        checks=checks,
        indexes=indexes,
        constraint_indexes=constraint_indexes,
        constraints=constraints,
        triggers=triggers,
        functions=functions,
        extensions=extensions,
        relation_kinds=relation_kinds,
        constraint_definitions=constraint_definitions,
        unique_constraints=unique_constraints,
        index_definitions=index_definitions,
        invalid_indexes=invalid_indexes,
        column_statistics=column_statistics,
    )
    return without_partitions(live, read_partitioning(run, sql_literal(target)))


#: A constraint attached NOT VALID reads back with this suffix; it says how the
#: constraint was attached, not what it asserts.
_NOT_VALID = re.compile(r"\s+NOT VALID\s*$")


def constraint_body(definition: str | None) -> str:
    """A constraint definition without its NOT VALID suffix — what it asserts."""
    return _NOT_VALID.sub("", str(definition or "")).strip()


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
            for col_name, col_type, col_params in PSEUDO_FIELD_EXPANSIONS[field_type]:
                columns[col_name] = {
                    "type": col_type,
                    "nullable": True,
                    "length": None,
                    "params": dict(col_params),
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
