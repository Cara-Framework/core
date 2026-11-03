"""Read-only introspection of a deployed PostgreSQL schema.

``schema:check`` (report) and ``schema:plan`` (act) must be looking at the
same database in the same way, so the queries live here once. Everything is
SELECT-only by construction — nothing in this module can change a schema. The
one write is the probe behind :func:`normalize_declared_checks` and
:func:`normalize_declared_indexes`: a TEMP table in its own session, which no
other session can see and which is dropped before the connection goes back.
"""

from __future__ import annotations

import re
from contextlib import suppress
from dataclasses import dataclass, replace

from cara.schema.DeclaredIndex import index_parts
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

    def __post_init__(self):
        for name, empty in (
            ("constraints", {}),
            ("triggers", {}),
            ("functions", set()),
            ("extensions", set()),
            ("relation_kinds", {}),
            ("constraint_definitions", {}),
            ("unique_constraints", {}),
            ("declared_check_definitions", {}),
            ("index_definitions", {}),
            ("invalid_indexes", {}),
            ("declared_index_definitions", {}),
            ("respelled_index_definitions", {}),
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
            "character_maximum_length, numeric_precision, numeric_scale, "
            "column_default "
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
        if constraint_type in ("c", "u") and row.get("definition"):
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
        constraint_definitions=constraint_definitions,
        unique_constraints=unique_constraints,
        index_definitions=index_definitions,
        invalid_indexes=invalid_indexes,
    )


#: A constraint attached NOT VALID reads back with this suffix; it says how the
#: constraint was attached, not what it asserts.
_NOT_VALID = re.compile(r"\s+NOT VALID\s*$")


def constraint_body(definition: str | None) -> str:
    """A constraint definition without its NOT VALID suffix — what it asserts."""
    return _NOT_VALID.sub("", str(definition or "")).strip()


def normalize_declared_checks(
    live_schema,
    live: LiveSchema,
    declared: dict[str, list[tuple[str, str]]],
    schema_name: str | None = None,
) -> LiveSchema:
    """Have the server spell the model's CHECKs, so they compare to the live ones.

    ``declared`` is ``{table: [(name, expression), ...]}``. For each table the
    database has, a session TEMP table copies its columns (``LIKE``) and takes
    each declared CHECK — that the database also has — ``NOT VALID``, and
    ``pg_get_constraintdef`` reads it back in exactly the spelling the live
    constraint has. Nothing touches the real table: the probe is a temporary
    relation in this one session, dropped before the connection goes back.

    It runs on ONE connection, because a temp table lives in its session and
    the query executor hands out a fresh one per query. Best effort: a role
    without TEMP, or an expression the probe cannot attach, leaves that CHECK
    out — and a CHECK left out is never called changed.
    """
    target = schema_name or live_schema.get_schema() or "public"
    manager = live_schema.query_executor.connection_manager
    connection = manager.create_connection_instance()
    found: dict[str, dict[str, str]] = {}
    try:
        for table, checks in sorted(declared.items()):
            present = live.checks.get(table, set())
            wanted = [(name, expr) for name, expr in checks if name in present]
            if not wanted or table not in live.tables:
                continue
            probe = f"_cara_check_probe_{abs(hash(table)) % 10**8}"
            try:
                connection.query(
                    f'CREATE TEMP TABLE "{probe}" '
                    f'(LIKE "{sql_literal(target)}"."{table}")',
                    (),
                )
            except Exception:
                continue
            try:
                for name, expression in wanted:
                    try:
                        connection.query(
                            f'ALTER TABLE "{probe}" ADD CONSTRAINT "{name}" '
                            f"CHECK ({expression}) NOT VALID",
                            (),
                        )
                        rows = connection.query(
                            "SELECT pg_get_constraintdef(oid) AS definition "
                            "FROM pg_constraint WHERE conrelid = %s::regclass "
                            "AND conname = %s",
                            (f'pg_temp."{probe}"', name),
                        )
                    except Exception:
                        continue
                    row = rows[0] if isinstance(rows, list) and rows else rows
                    if isinstance(row, dict) and row.get("definition"):
                        found.setdefault(table, {})[name] = str(row["definition"])
            finally:
                with suppress(Exception):
                    connection.query(f'DROP TABLE IF EXISTS "{probe}"', ())
    finally:
        manager.release(connection)
    # The snapshot is frozen: the spelled CHECKs come back on a copy.
    return replace(live, declared_check_definitions=found)


def normalize_declared_indexes(
    live_schema,
    live: LiveSchema,
    declared: dict[str, list[tuple[str, bool, str]]],
    schema_name: str | None = None,
) -> LiveSchema:
    """Have the server spell the model's indexes, so they compare to the live ones.

    ``declared`` is ``{table: [(name, unique, rest), ...]}`` — ``rest`` being
    everything after the table in the model's ``CREATE INDEX`` (``("a", "b")``,
    ``USING gin (...) WHERE ...``). Same mechanism as
    :func:`normalize_declared_checks`: each declaration the database also has
    BY NAME is built on a session TEMP table copying the real one's columns,
    and ``pg_get_indexdef`` reads it back in exactly the spelling the live
    index has — casts, ``USING btree``, opclasses, ``INCLUDE``, the
    parenthesised predicate. Nothing touches the real table.

    The server deparsing BOTH sides is the point. Normalising the model's DDL
    by hand would be a second SQL parser (``lower((counterparty)::text)`` is
    what ``LOWER(counterparty)`` reads back as), and comparing structured
    ``pg_index`` columns cannot reach a raw ``__indexes__`` statement without
    parsing it first. Best effort, like the CHECK probe: a declaration the
    probe cannot build is left out, and one left out is never called changed.

    One server spelling is not yet a fixpoint. Re-parsing a STORED
    definition can move a cast — ``(ARRAY['a'::varchar])::text[]`` comes
    back as ``ARRAY[('a'::varchar)::text]`` — and an index rebuilt by
    ``ALTER COLUMN TYPE`` or restored from a dump stores the re-parsed form.
    So the declaration is spelled twice (built, read back, built from what
    was read), the live definition once more, and the two compare at that
    fixpoint: the same index never reads as changed because of its history.
    """
    target = schema_name or live_schema.get_schema() or "public"
    manager = live_schema.query_executor.connection_manager
    connection = manager.create_connection_instance()
    found: dict[str, dict[str, str]] = {}
    respelled: dict[str, dict[str, str]] = {}
    try:
        for table, indexes in sorted(declared.items()):
            present = live.index_definitions.get(table, {})
            wanted = [entry for entry in indexes if entry[0] in present]
            if not wanted or table not in live.tables:
                continue
            probe = f"_cara_index_probe_{abs(hash(table)) % 10**8}"
            try:
                connection.query(
                    f'CREATE TEMP TABLE "{probe}" '
                    f'(LIKE "{sql_literal(target)}"."{table}")',
                    (),
                )
            except Exception:
                continue
            try:
                for position, (name, unique, rest) in enumerate(wanted):
                    spelled = _spell_index(
                        connection, probe, f"{probe}_d{position}", (unique, rest)
                    )
                    spelled = (
                        _spell_index(
                            connection,
                            probe,
                            f"{probe}_e{position}",
                            index_parts(spelled),
                        )
                        or spelled
                    )
                    if spelled:
                        found.setdefault(table, {})[name] = spelled
                    again = _spell_index(
                        connection,
                        probe,
                        f"{probe}_l{position}",
                        index_parts(present[name]),
                    )
                    if again:
                        respelled.setdefault(table, {})[name] = again
            finally:
                with suppress(Exception):
                    connection.query(f'DROP TABLE IF EXISTS "{probe}"', ())
    finally:
        manager.release(connection)
    return replace(
        live,
        declared_index_definitions=found,
        respelled_index_definitions=respelled,
    )


def _spell_index(
    connection, probe: str, label: str, parts: tuple[bool, str] | None
) -> str | None:
    """Build ``(unique, rest)`` as ``label`` on the probe table; the server's
    ``pg_get_indexdef`` of it, or None when it would not build."""
    if parts is None:
        return None
    unique, rest = parts
    try:
        connection.query(
            f'CREATE {"UNIQUE " if unique else ""}INDEX "{label}" ON "{probe}"{rest}',
            (),
        )
        rows = connection.query(
            "SELECT pg_get_indexdef(%s::regclass) AS definition",
            (f'pg_temp."{label}"',),
        )
    except Exception:
        return None
    row = rows[0] if isinstance(rows, list) and rows else rows
    if isinstance(row, dict) and row.get("definition"):
        return str(row["definition"])
    return None


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
    "normalize_declared_checks",
    "normalize_declared_indexes",
    "raw_sql_columns",
    "sql_literal",
]
