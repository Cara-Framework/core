"""The model's declarations as the SERVER spells them.

A declared CHECK or index is compared to the live one only after Postgres has
deparsed both: each declaration the database also has by name is attached to
a session TEMP copy of its table and read back with ``pg_get_constraintdef``
or ``pg_get_indexdef``. Nothing touches the real table.

Split out of :mod:`cara.schema.LiveSchema`, which only reads the catalogue;
this is the one place introspection WRITES (a temp relation in its own
session, dropped before the connection goes back).

Best effort per declaration, never per database: a declaration the probe
cannot build is left unspelled and is never called changed, but only a
statement the server REFUSED is read that way. A lost database propagates,
because "could not ask" must not read as "nothing drifted".
"""

from __future__ import annotations

import logging
from contextlib import contextmanager, suppress
from dataclasses import replace

from cara.exceptions import QueryException
from cara.schema.DeclaredIndex import index_parts
from cara.schema.LiveSchema import LiveSchema, constraint_body, sql_literal

_logger = logging.getLogger("cara.schema")


@contextmanager
def _probe_table(connection, target: str, table: str, kind: str):
    """A session TEMP copy of ``table``'s columns, dropped on exit; None when
    the server refuses it (a role without TEMP) — logged, because every
    declaration of that table then goes uncompared.

    Only a refused STATEMENT is caught: a lost database is not "nothing
    drifted" and propagates, so the gate fails instead of reading clean.
    """
    probe = f"_cara_{kind}_probe_{abs(hash(table)) % 10**8}"
    try:
        connection.query(
            f'CREATE TEMP TABLE "{probe}" (LIKE "{sql_literal(target)}"."{table}")', ()
        )
    except QueryException:
        _logger.warning(
            "schema probe for %s refused; its declared %s definitions are not "
            "compared this run",
            table,
            kind,
            exc_info=True,
        )
        yield None
        return
    try:
        yield probe
    finally:
        # The temp table dies with the session anyway; a refused drop must
        # not mask what the probe itself raised.
        with suppress(QueryException):
            connection.query(f'DROP TABLE IF EXISTS "{probe}"', ())


def normalize_declared_checks(
    live_schema,
    live: LiveSchema,
    declared: dict[str, list[tuple[str, str]]],
    schema_name: str | None = None,
    reshapes: dict[str, list[str]] | None = None,
) -> LiveSchema:
    """Have the server spell the model's CHECKs, so they compare to the live ones.

    ``reshapes`` (``cara.schema.ConstraintDrift.probe_reshapes``) gives the
    probe the POST-plan columns first, so a CHECK naming a column the same
    plan renames or adds is spelled rather than skipped.

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

    The same fixpoint as :func:`normalize_declared_indexes`, for the same
    reason: the declaration is spelled twice and the live definition once
    more, so a CHECK that went through ``pg_dump`` reads the same as one that
    never did.
    """
    target = schema_name or live_schema.get_schema() or "public"
    manager = live_schema.query_executor.connection_manager
    connection = manager.create_connection_instance()
    found: dict[str, dict[str, str]] = {}
    respelled: dict[str, dict[str, str]] = {}
    try:
        for table, checks in sorted(declared.items()):
            present = live.checks.get(table, set())
            wanted = [(name, expr) for name, expr in checks if name in present]
            live_bodies = live.constraint_definitions.get(table, {})
            if not wanted or table not in live.tables:
                continue
            with _probe_table(connection, target, table, "check") as probe:
                if probe is None:
                    continue
                for clause in (reshapes or {}).get(table, []):
                    # A clause that does not apply leaves the CHECK naming
                    # its column unspelled — replaced, never called clean.
                    with suppress(QueryException):
                        connection.query(f'ALTER TABLE "{probe}" {clause}', ())
                for position, (name, expression) in enumerate(wanted):
                    spelled = _spell_check(
                        connection, probe, f"{probe}_d{position}", f"CHECK ({expression})"
                    )
                    spelled = (
                        _spell_check(
                            connection,
                            probe,
                            f"{probe}_e{position}",
                            constraint_body(spelled),
                        )
                        or spelled
                    )
                    if spelled:
                        found.setdefault(table, {})[name] = spelled
                    again = _spell_check(
                        connection,
                        probe,
                        f"{probe}_l{position}",
                        constraint_body(live_bodies.get(name)),
                    )
                    if again:
                        respelled.setdefault(table, {})[name] = again
    finally:
        manager.release(connection)
    # The snapshot is frozen: the spelled CHECKs come back on a copy.
    return replace(
        live,
        declared_check_definitions=found,
        respelled_check_definitions=respelled,
    )


def _spell_check(connection, probe: str, label: str, body: str) -> str | None:
    """Attach ``body`` as ``label`` on the probe table; the server's
    ``pg_get_constraintdef`` of it, or None when it would not attach."""
    if not body:
        return None
    try:
        connection.query(
            f'ALTER TABLE "{probe}" ADD CONSTRAINT "{label}" {body} NOT VALID',
            (),
        )
        rows = connection.query(
            "SELECT pg_get_constraintdef(oid) AS definition "
            "FROM pg_constraint WHERE conrelid = %s::regclass AND conname = %s",
            (f'pg_temp."{probe}"', label),
        )
    except QueryException:
        # An expression the probe cannot attach is left unspelled, which is
        # never called changed. An outage is not that, and propagates.
        _logger.debug("CHECK probe could not attach %s", label, exc_info=True)
        return None
    row = rows[0] if isinstance(rows, list) and rows else rows
    if isinstance(row, dict) and row.get("definition"):
        return str(row["definition"])
    return None


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
            with _probe_table(connection, target, table, "index") as probe:
                if probe is None:
                    continue
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
    except QueryException:
        # As for ``_spell_check``: refused declarations are left unspelled.
        _logger.debug("index probe could not build %s", label, exc_info=True)
        return None
    row = rows[0] if isinstance(rows, list) and rows else rows
    if isinstance(row, dict) and row.get("definition"):
        return str(row["definition"])
    return None


__all__ = [
    "normalize_declared_checks",
    "normalize_declared_indexes",
]
