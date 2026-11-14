"""Trigger and function BODIES, not just their names.

``__indexes__`` named-DDL entries were compared by NAME only
(``cara.schema.Objects.missing_indexes``: an object that exists is done). On a
scratch migrated from zero, the append-only guard
``reject_audit_log_mutation()`` was replaced with a body that returns NEW and
``schema:check`` still printed "No drift" (overnight schema-deploy c1,
2026-11-14). The deploy direction is worse: a FIX to a trigger body
(``sales_order_closed_at_stamp``, ``product_identifier_condition_resync``)
never reached a deployed database through EVOLVE, and the old body kept
running with every gate reading clean.

So the bodies are compared:

* **A function** by what ``pg_proc`` stores verbatim (``prosrc``) and its
  language, against the declared dollar-quoted body; whitespace is the only
  thing normalized, because PL/pgSQL keeps the source exactly as written.
* **A trigger** as the SERVER spells it: the declared statement is run on a
  session TEMP copy of the table and ``pg_get_triggerdef`` reads it back,
  the same probe :mod:`cara.schema.LiveSchema` uses for CHECKs, because the
  server reorders events and deparses ``WHEN`` clauses.

A difference becomes one ``replace_routine`` operation that runs the entry's
own ``up`` (every entry is ``CREATE OR REPLACE``) and whose reverse is the
server's own text of the previous definition.
"""

from __future__ import annotations

import re
from contextlib import suppress
from dataclasses import replace

from cara.schema.ConstraintDrift import _parsed_foreign_key
from cara.schema.LiveSchema import LiveSchema, sql_literal
from cara.schema.Objects import _foreign_key_preflight, created_objects
from cara.schema.Operation import ADDITIVE, LOCKING, Operation

_FUNCTION_RE = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+(?:\"?\w+\"?\.)?\"?(?P<name>\w+)\"?\s*\(",
    re.IGNORECASE,
)
_PLAIN_FUNCTION_RE = re.compile(r"\bCREATE\s+FUNCTION\b", re.IGNORECASE)
_TRIGGER_RE = re.compile(
    r"CREATE\s+(?P<replace>OR\s+REPLACE\s+)?(?:CONSTRAINT\s+)?TRIGGER\s+\"?(?P<name>\w+)\"?",
    re.IGNORECASE,
)
_BODY_RE = re.compile(
    r"\bAS\s+(?:(?P<tag>\$\w*\$)(?P<dollar>.*?)(?P=tag)|'(?P<quoted>(?:[^']|'')*)')",
    re.IGNORECASE | re.DOTALL,
)
_LANGUAGE_RE = re.compile(r"\bLANGUAGE\s+'?\"?(?P<language>\w+)", re.IGNORECASE)


def _flat(text: str) -> str:
    return " ".join(str(text or "").split())


def declared_routines(model: dict) -> list[dict]:
    """Each ``__indexes__`` entry that creates a function or a trigger.

    ``{"label", "up", "functions": [(name, body, language)],
    "triggers": [(name, statement)]}``.
    """
    found: list[dict] = []
    for entry in model.get("indexes") or []:
        up = entry.get("up") or ""
        functions = []
        for match in _FUNCTION_RE.finditer(up):
            body = _BODY_RE.search(up, match.end())
            language = _LANGUAGE_RE.search(up, match.end())
            if body is None:
                continue
            text = (
                body["dollar"]
                if body["tag"] is not None
                else body["quoted"].replace("''", "'")
            )
            functions.append(
                (
                    match["name"],
                    _flat(text),
                    (language["language"] if language else "").lower(),
                )
            )
        triggers = []
        for match in _TRIGGER_RE.finditer(up):
            end = up.find(";", match.end())
            triggers.append(
                (match["name"], up[match.start() : end if end >= 0 else None])
            )
        if functions or triggers:
            found.append(
                {
                    "label": entry.get("name") or "",
                    "up": up,
                    "functions": functions,
                    "triggers": triggers,
                }
            )
    return found


def _on_relation(table: str, probe: str | None = None) -> re.Pattern:
    names = "|".join(re.escape(name) for name in (table, probe) if name)
    return re.compile(rf'\bON\s+(?:[\w"]+\.)?"?(?:{names})"?(?=\s)', re.IGNORECASE)


def trigger_shape(definition: str, table: str, probe: str | None = None) -> str:
    """A ``pg_get_triggerdef`` with the relation it sits on taken out."""
    return _flat(_on_relation(table, probe).sub(f"ON {table}", definition or "", count=1))


def read_named_ddl(
    live_schema, live: LiveSchema, models: list[dict], schema_name: str | None = None
) -> LiveSchema:
    """Live trigger/function definitions, and the declared triggers as the
    server spells them on a TEMP probe of their table.

    Best effort on the probe, as for CHECKs: a declared trigger that would not
    build there is left out, and one left out is never called changed.
    """
    target = schema_name or live_schema.get_schema() or "public"
    run = live_schema.query_executor.get_query_result
    triggers: dict[str, dict[str, str]] = {}
    for row in (
        run(
            "SELECT c.relname AS table_name, t.tgname AS trigger_name, "
            "pg_get_triggerdef(t.oid) AS definition "
            "FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            f"WHERE n.nspname = '{sql_literal(target)}' AND NOT t.tgisinternal"
        )
        or []
    ):
        triggers.setdefault(row["table_name"], {})[row["trigger_name"]] = str(
            row["definition"]
        )
    # A guard that exists but does not fire (``DISABLE TRIGGER`` left behind
    # by a hand data fix, or ``DISABLE TRIGGER ALL`` switching a FOREIGN KEY's
    # internal triggers off) reads as present by name AND by body.
    disabled: dict[str, dict[str, tuple]] = {}
    for row in (
        run(
            "SELECT c.relname AS table_name, t.tgname AS trigger_name, "
            "t.tgenabled::text AS state, "
            "CASE WHEN t.tgisinternal THEN con.conname END AS constraint_name, "
            "CASE WHEN t.tgisinternal THEN child.relname END AS constrained_table, "
            "CASE WHEN t.tgisinternal THEN pg_get_constraintdef(con.oid) END "
            "AS constraint_definition "
            "FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "LEFT JOIN pg_constraint con ON con.oid = t.tgconstraint "
            "LEFT JOIN pg_class child ON child.oid = con.conrelid "
            f"WHERE n.nspname = '{sql_literal(target)}' "
            "AND t.tgenabled IN ('D', 'R') "
            "AND (NOT t.tgisinternal OR con.contype = 'f')"
        )
        or []
    ):
        disabled.setdefault(row["table_name"], {})[row["trigger_name"]] = (
            str(row["state"]),
            row.get("constraint_name"),
            row.get("constrained_table"),
            row.get("constraint_definition"),
        )
    functions: dict[str, list[dict]] = {}
    for row in (
        run(
            "SELECT p.proname AS name, p.prosrc AS source, l.lanname AS language, "
            "pg_get_functiondef(p.oid) AS definition "
            "FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
            "JOIN pg_language l ON l.oid = p.prolang "
            f"WHERE n.nspname = '{sql_literal(target)}' AND p.prokind IN ('f', 'p')"
        )
        or []
    ):
        functions.setdefault(row["name"], []).append(
            {
                "source": _flat(row["source"]),
                "language": str(row["language"] or "").lower(),
                "definition": str(row["definition"]),
            }
        )

    declared: dict[str, dict[str, str]] = {}
    manager = live_schema.query_executor.connection_manager
    connection = manager.create_connection_instance()
    try:
        for model in models:
            table = model.get("table")
            present = triggers.get(table, {})
            wanted = [
                pair
                for routine in declared_routines(model)
                for pair in routine["triggers"]
                if pair[0] in present
            ]
            if not wanted or table not in live.tables:
                continue
            probe = f"_cara_trigger_probe_{abs(hash(table)) % 10**8}"
            try:
                connection.query(
                    f'CREATE TEMP TABLE "{probe}" (LIKE "{sql_literal(target)}"."{table}")',
                    (),
                )
            except Exception:
                continue
            try:
                for name, statement in wanted:
                    spelled = _spell_trigger(connection, table, probe, name, statement)
                    if spelled:
                        declared.setdefault(table, {})[name] = trigger_shape(
                            spelled, table, probe
                        )
            finally:
                with suppress(Exception):
                    connection.query(f'DROP TABLE IF EXISTS "{probe}"', ())
    finally:
        manager.release(connection)
    return replace(
        live,
        trigger_definitions=triggers,
        declared_trigger_definitions=declared,
        function_definitions=functions,
        disabled_triggers=disabled,
    )


def _spell_trigger(connection, table: str, probe: str, name: str, statement: str):
    on_probe = _on_relation(table).sub(f'ON pg_temp."{probe}"', statement, count=1)
    try:
        connection.query(on_probe, ())
        rows = connection.query(
            "SELECT pg_get_triggerdef(oid) AS definition FROM pg_trigger "
            "WHERE tgrelid = %s::regclass AND tgname = %s",
            (f'pg_temp."{probe}"', name),
        )
    except Exception:
        return None
    row = rows[0] if isinstance(rows, list) and rows else rows
    return (
        str(row["definition"])
        if isinstance(row, dict) and row.get("definition")
        else None
    )


def _forward(up: str) -> str:
    """The entry's own statement, made safe to run over an existing object."""
    sql = _PLAIN_FUNCTION_RE.sub("CREATE OR REPLACE FUNCTION", up)
    drops = [
        f'DROP TRIGGER IF EXISTS "{match["name"]}" ON {_trigger_table(sql, match)}'
        for match in _TRIGGER_RE.finditer(sql)
        if not match["replace"]
    ]
    return "; ".join([*drops, sql])


def _trigger_table(sql: str, match: re.Match) -> str:
    target = re.search(r'\bON\s+((?:[\w"]+\.)?[\w"]+)', sql[match.end() :], re.IGNORECASE)
    return target.group(1) if target else '""'


def changed_routines(model: dict, table: str, live: LiveSchema) -> list[Operation]:
    """Entries whose function or trigger exists by name with another body,
    then every trigger that exists and does not fire (:func:`disabled_triggers`)."""
    live_triggers = (live.trigger_definitions or {}).get(table, {})
    spelled = (live.declared_trigger_definitions or {}).get(table, {})
    operations: list[Operation] = []
    present = live.objects_on(table)
    for routine in declared_routines(model):
        # An entry with an object still MISSING is ``missing_indexes``' create,
        # which runs the same ``up``. Planned here as well, the entry ran
        # twice, and a plain ``CREATE TRIGGER`` in it failed the second run
        # on "already exists" with the plan half applied.
        created = created_objects(routine["up"])
        if created is None or not created <= present:
            continue
        differing: list[str] = []
        reverses: list[str | None] = []
        for name, body, language in routine["functions"]:
            candidates = (live.function_definitions or {}).get(name) or []
            if not candidates or any(
                c["source"] == body and (not language or c["language"] == language)
                for c in candidates
            ):
                continue
            differing.append(f"function {name}()")
            reverses.append(candidates[0]["definition"] if len(candidates) == 1 else None)
        trigger_changed = False
        for name, _statement in routine["triggers"]:
            current, wanted = live_triggers.get(name), spelled.get(name)
            if not current or not wanted or trigger_shape(current, table) == wanted:
                continue
            trigger_changed = True
            differing.append(f"trigger {name}")
            reverses.append(f'DROP TRIGGER IF EXISTS "{name}" ON "{table}"; {current}')
        if not differing:
            continue
        operations.append(
            Operation(
                kind="replace_routine",
                table=table,
                key=f"{table}:{routine['label']}:definition",
                forward_sql=_forward(routine["up"]),
                reverse_sql=None if None in reverses else "; ".join(reverses),
                safety=LOCKING if trigger_changed else ADDITIVE,
                reason=(
                    f"{', '.join(differing)} in the database runs a different "
                    "body than the model declares"
                ),
                notes=(
                    "the reverse is the server's own text of the previous definition",
                ),
            )
        )
    return operations + disabled_triggers(model, table, live)


#: How a trigger that does not fire was switched off, as the reverse spells it.
_SWITCHED_OFF = {"D": "DISABLE TRIGGER", "R": "ENABLE REPLICA TRIGGER"}


def disabled_triggers(model: dict, table: str, live: LiveSchema) -> list[Operation]:
    """Declared triggers — and FOREIGN KEY enforcement triggers — that exist
    on ``table`` but do not fire.

    ``DISABLE TRIGGER audit_log_immutable`` turns the append-only audit trail
    into an editable table while name and body still match the model, so the
    state itself is compared. Every model trigger is created to fire in
    ``origin`` mode; ``replica`` mode fires only under logical replication
    apply, which is "off" for every application write.
    """
    switched_off = (live.disabled_triggers or {}).get(table, {})
    if not switched_off:
        return []
    declared = {
        name for routine in declared_routines(model) for name, _ in routine["triggers"]
    }
    operations: list[Operation] = []
    keys: dict[str, list[tuple[str, str]]] = {}
    shapes: dict[str, tuple[str | None, str | None]] = {}
    for name, (state, constraint, constrained, definition) in sorted(
        switched_off.items()
    ):
        if constraint is not None:
            keys.setdefault(constraint, []).append((name, state))
            shapes[constraint] = (constrained, definition)
            continue
        if name not in declared:
            continue
        operations.append(
            Operation(
                kind="enable_trigger",
                table=table,
                key=f"{table}:{name}:enable",
                forward_sql=f'ALTER TABLE "{table}" ENABLE TRIGGER "{name}"',
                reverse_sql=f'ALTER TABLE "{table}" {_SWITCHED_OFF.get(state, "DISABLE TRIGGER")} "{name}"',
                safety=LOCKING,
                reason=f"trigger {name} is {_off(state)} in the database",
                notes=(
                    "rows written while it was off were never checked by it; "
                    "enabling it does not re-check them",
                ),
            )
        )
    for constraint, members in sorted(keys.items()):
        operations.append(
            _enforce_foreign_key(table, constraint, members, *shapes[constraint])
        )
    return operations


def _off(state: str) -> str:
    return "DISABLED" if state == "D" else "replica-only"


def _enforce_foreign_key(
    table: str,
    constraint: str,
    members: list[tuple[str, str]],
    constrained: str | None,
    definition: str | None,
) -> Operation:
    """One operation per FOREIGN KEY whose internal triggers on ``table`` are off.

    Keyed by the CONSTRAINT, not by ``RI_ConstraintTrigger_c_<oid>``: those
    names are OIDs, so a rehearsal clone (``pg_dump`` recreates the key with
    fresh, ENABLED triggers) derived a different plan and every rehearsal of
    it failed as "the database has MOVED". ``schema:plan --rehearse`` leaves
    this kind out of the clone's artifact for the same reason it leaves owed
    backfills out: the clone cannot owe it.

    Postgres keeps such a key marked VALID while it enforced nothing, and
    switching its triggers back on re-checks nothing. Rows written meanwhile
    (a membership pointing at a deleted workspace) would then sit under a key
    every gate calls enforced, so the preflight refuses until they are
    repaired — the same question ``VALIDATE CONSTRAINT`` asks of a new key.
    """
    names = [name for name, _ in members]
    parsed = _parsed_foreign_key(definition or "") if constrained else None
    return Operation(
        kind="enforce_foreign_key",
        table=table,
        key=f"{table}:{constraint}:enforce",
        forward_sql=f'ALTER TABLE "{table}" '
        + ", ".join(f'ENABLE TRIGGER "{name}"' for name in names),
        reverse_sql=f'ALTER TABLE "{table}" '
        + ", ".join(
            f'{_SWITCHED_OFF.get(state, "DISABLE TRIGGER")} "{name}"'
            for name, state in members
        ),
        safety=LOCKING,
        reason=(
            f"FOREIGN KEY {constraint} is not enforced: its trigger(s) on {table} "
            f"({', '.join(names)}) are {_off(members[0][1])} in the database"
        ),
        preflight_sql=_foreign_key_preflight(constrained, parsed) if parsed else None,
        preflight_failure=(
            f"{constrained} holds rows with no {parsed['on']} parent for {constraint}, "
            "written while the key was off — repair them first: switching the key "
            "back on does not re-check rows, and Postgres still calls it VALID"
            if parsed
            else None
        ),
        notes=(
            "a FOREIGN KEY's internal triggers can only be switched by a superuser role",
        ),
    )


__all__ = [
    "changed_routines",
    "declared_routines",
    "disabled_triggers",
    "read_named_ddl",
    "trigger_shape",
]
