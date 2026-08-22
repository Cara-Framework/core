"""Which schema OBJECTS does the model own, and does the database have them?

Split out of ``Planner`` because it answers a different question with a
different failure mode. Column planning compares two descriptions of the same
thing — a declared column and a live column — and the risk is emitting the
wrong ALTER. Objects are named things scattered across several catalogues, and
the risk is asking the WRONG catalogue: an extension looked for among indexes,
a column looked for among constraints. That answer is always "missing", so the
entry is planned again on every single run.

Five such phantoms sat in every cheapa plan, each an ``IF NOT EXISTS`` no-op a
reviewer had to dismiss by hand. Nothing broke, which is what made it
corrosive: a plan that is never empty stops meaning anything when it is not
empty, and the empty plan is precisely the signal a deploy relies on.

So names are KIND-QUALIFIED here (``extension:pg_trgm``, ``column:search_vector``)
and matched against :meth:`LiveSchema.objects_on`, which qualifies the same way.
An entry whose SQL names nothing recognisable is REFUSED rather than guessed at
in either direction — replanning forever and silently skipping a real object are
both wrong, and only one of them is visible.
"""

from __future__ import annotations

import re

from cara.exceptions import SchemaPlanRefused
from cara.schema.LiveSchema import LiveSchema
from cara.schema.Operation import ADDITIVE, DESTRUCTIVE, LOCKING, Operation

#: Object names an ``__indexes__`` entry's SQL actually creates.
_CREATES_RE = re.compile(
    r"(?:CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:CONCURRENTLY\s+)?(?:IF\s+NOT\s+EXISTS\s+)?"
    r"|ADD\s+CONSTRAINT\s+"
    r"|CREATE\s+(?:OR\s+REPLACE\s+)?TRIGGER\s+"
    r"|CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+)"
    r"\"?(?P<name>\w+)\"?",
    re.IGNORECASE,
)

#: Extensions live in a schema-wide catalogue, not the table's namespace, and
#: the entry that installs one is conventionally labelled after the entry
#: (``ext_pg_trgm``) rather than after the extension.
_CREATES_EXTENSION_RE = re.compile(
    r"CREATE\s+EXTENSION\s+(?:IF\s+NOT\s+EXISTS\s+)?\"?(?P<name>\w+)\"?",
    re.IGNORECASE,
)

#: A column added by named DDL rather than by a field. Cara reaches for this
#: when the model DSL has no builder for the type — a GENERATED ALWAYS AS
#: ``tsvector`` is the live example — so the column exists in the database and
#: in no ``fields`` dict, and only the table's own column list can answer for
#: it.
_ADDS_COLUMN_RE = re.compile(
    r"ADD\s+COLUMN\s+(?:IF\s+NOT\s+EXISTS\s+)?\"?(?P<name>\w+)\"?",
    re.IGNORECASE,
)

# Simple column-list UNIQUE indexes can be checked before apply, including a
# partial predicate. Expression indexes deliberately do not match: guessing at
# nested SQL would turn a safety check into false confidence.
_SIMPLE_UNIQUE_INDEX_RE = re.compile(
    r"^\s*CREATE\s+UNIQUE\s+INDEX\s+(?:CONCURRENTLY\s+)?"
    r"(?:IF\s+NOT\s+EXISTS\s+)?\"?\w+\"?\s+ON\s+\"?\w+\"?\s*"
    r"\((?P<columns>[^()]+)\)"
    r"(?:\s+WHERE\s+(?P<predicate>.+?))?\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)


def _unique_index_preflight(
    up_sql: str, table: str, index_name: str
) -> tuple[str | None, str | None]:
    match = _SIMPLE_UNIQUE_INDEX_RE.match(up_sql)
    if match is None:
        return None, None
    columns = " ".join(match.group("columns").split())
    predicate = match.group("predicate")
    where = ""
    if predicate:
        where = f" WHERE {' '.join(predicate.rstrip(';').split())}"
    return (
        f'SELECT 1 FROM "{table}"{where} GROUP BY {columns} HAVING COUNT(*) > 1 LIMIT 1',
        f"{table} already holds duplicate keys for {index_name} — resolve them "
        "before building the UNIQUE index",
    )


def created_objects(up_sql: str) -> set[str] | None:
    """The objects this statement creates, or None when it cannot be read.

    An entry's ``name`` is a LABEL, not necessarily a database object: one
    entry legitimately creates two differently-named CHECK constraints, and
    keying presence on the label alone makes that entry replan forever. Since
    the plan is the thing a human reads before a production deploy, permanent
    phantom entries are not cosmetic — they train the reader to skim.

    Names are returned KIND-QUALIFIED where the object does not live in the
    per-table namespace, because otherwise the question "is it already there?"
    is asked of the wrong catalogue. ``CREATE EXTENSION pg_trgm`` creates an
    extension named ``pg_trgm``, not an index named ``ext_pg_trgm``, and
    ``ALTER TABLE product ADD COLUMN search_vector`` creates a column — both
    read as permanently missing against a set of index and constraint names.
    That was five phantom operations in every cheapa plan, forever, each one
    an ``IF NOT EXISTS`` no-op that a reader had to re-dismiss by hand.

    None means "this statement creates nothing I can name", which the caller
    reports as a refusal rather than replanning it. Guessing in either
    direction is worse: run-it-always is the phantom above, skip-it-always
    silently drops a real object from the plan.
    """
    found = {match.group("name") for match in _CREATES_RE.finditer(up_sql)}
    found |= {
        f"extension:{match.group('name')}"
        for match in _CREATES_EXTENSION_RE.finditer(up_sql)
    }
    found |= {
        f"column:{match.group('name')}" for match in _ADDS_COLUMN_RE.finditer(up_sql)
    }
    return found or None


#: PostgreSQL truncates every identifier to NAMEDATALEN-1 bytes. The stored
#: name is the truncated one, so a planner comparing the full convention name
#: against the catalogue finds nothing and plans an index that already exists —
#: on this schema, 23 of them, every single plan.
_MAX_IDENTIFIER_LENGTH = 63


def _blueprint_index_name(table: str, columns: list[str], unique: bool) -> str:
    """The name Cara's Blueprint gives an unnamed ``index``/``unique``.

    ``table.index(["a", "b"])`` becomes ``<table>_a_b_index``, truncated the
    way Postgres truncates it. Reproducing the convention is what lets the
    planner ask "does the database already have this one?" — without it, a
    field-level index is invisible on the deployed side and never planned.
    """
    name = f"{table}_{'_'.join(columns)}_{'unique' if unique else 'index'}"
    return name[:_MAX_IDENTIFIER_LENGTH]


def _declared_blueprint_indexes(
    model: dict, table: str
) -> list[tuple[str, list[str], bool]]:
    """``(name, columns, unique)`` for every field-level index the model declares.

    Covers both spellings: the per-field ``.index()`` / ``.unique()`` flags and
    the standalone ``field.index([...], name=...)`` declarations, which may
    carry an explicit name.

    Keyed by NAME, because the discoverer records a single-column
    ``field.string(...).index()`` under both spellings — once as a param flag,
    once as a standalone declaration. Emitting both would put the same
    ``CREATE INDEX`` in one plan twice, and a plan that lists an operation
    twice is a plan nobody trusts.
    """
    declared: dict[str, tuple[str, list[str], bool]] = {}

    for column, definition in (model.get("fields") or {}).items():
        params = definition.get("params") or {}
        for flag, unique in (("index", False), ("unique", True)):
            if params.get(flag):
                name = _blueprint_index_name(table, [column], unique)
                declared[name] = (name, [column], unique)

    for key, unique in (("composite_indexes", False), ("composite_uniques", True)):
        for declaration in model.get(key, []) or []:
            columns = list(declaration.get("columns") or [])
            if not columns:
                continue
            name = declaration.get("name") or _blueprint_index_name(
                table, columns, unique
            )
            declared[name] = (name, columns, unique)

    return list(declared.values())


def _adopt_unique_index(
    table: str, name: str, columns: list[str], *, index_prebuilt: bool = False
) -> Operation:
    """Attach the declared UNIQUE constraint to its (existing or just-built)
    backing index. ``ADD CONSTRAINT ... UNIQUE USING INDEX`` takes a brief
    ACCESS EXCLUSIVE lock and no table scan — the index already proves
    uniqueness."""
    return Operation(
        kind="add_unique_constraint",
        table=table,
        key=f"{table}:{name}:constraint",
        forward_sql=(
            f'ALTER TABLE "{table}" ADD CONSTRAINT {name} '
            f"UNIQUE USING INDEX {name}"
        ),
        reverse_sql=f'ALTER TABLE "{table}" DROP CONSTRAINT {name}',
        safety=LOCKING,
        reason=(
            "a bare unique index carries the declared unique constraint's "
            "name; adopting it restores the constraint form a from-zero "
            "migrate creates"
            if index_prebuilt
            else (
                f"model declares a unique constraint on {', '.join(columns)}; "
                "the CONCURRENTLY-built index becomes the constraint"
            )
        ),
        notes=(
            "brief ACCESS EXCLUSIVE lock, no scan; dropping the constraint "
            "later also drops the index it absorbed",
        ),
    )


def _slugify_check(text: str) -> str:
    """Mirror of ``ConstraintManager._slugify`` — the auto-derived CHECK name
    must match what the generator writes into a from-zero migration, or the
    same declaration would carry two names depending on which tool ran."""
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", str(text)).strip("_").lower()
    return slug or "check"


def _declared_checks(model: dict, table: str) -> list[tuple[str, str]]:
    """``(name, expression)`` for every field-level ``field.check`` the model
    declares, named exactly as the Blueprint would name it."""
    declared: list[tuple[str, str]] = []
    for check in model.get("checks", []) or []:
        expression = str(check.get("expression") or "").strip()
        if not expression:
            continue
        name = check.get("name") or f"{table}_{_slugify_check(expression)}_check"
        declared.append((name, expression))
    return declared


def declared_foreign_keys(model: dict, table: str) -> list[dict]:
    """Every FOREIGN KEY a model declares, named the way Postgres will store it.

    Two declaration sites, one naming rule (``cara/eloquent/schema/Table.py``):
    a single-column ``field.foreign("x")`` and a composite
    ``field.foreign(["a", "b"], name=...)``. An unnamed key defaults to
    ``<table>_<columns joined by _>_foreign``, which is what the Blueprint
    emits, so a declared name and a live ``conname`` are directly comparable.

    Each entry is ``{name, columns, references, on, on_delete, on_update}``.
    Raw-SQL ``__indexes__`` entries that add a foreign key are NOT included:
    those are diffed as named objects, by whole-constraint-set membership.
    """

    def named(columns: list[str], given) -> str:
        return str(given) if given else f"{table}_{'_'.join(columns)}_foreign"

    def listed(value, fallback: str | None = None) -> list[str]:
        if value is None:
            return [fallback] if fallback else []
        if isinstance(value, (list, tuple)):
            return [str(item) for item in value]
        return [str(value)]

    keys: list[dict] = []
    for column, definition in (model.get("fields") or {}).items():
        key = (definition or {}).get("foreign_key")
        if not key or not key.get("on"):
            continue
        columns = listed(key.get("field"), column)
        keys.append(
            {
                "name": named(columns, key.get("name")),
                "columns": columns,
                "references": listed(key.get("references"), "id"),
                "on": str(key["on"]),
                "on_delete": key.get("on_delete"),
                "on_update": key.get("on_update"),
            }
        )
    for key in model.get("composite_foreign_keys") or []:
        columns = listed(key.get("columns"))
        if not columns or not key.get("on"):
            continue
        keys.append(
            {
                "name": named(columns, key.get("name")),
                "columns": columns,
                "references": listed(key.get("references"), "id"),
                "on": str(key["on"]),
                "on_delete": key.get("on_delete"),
                "on_update": key.get("on_update"),
            }
        )
    return keys


def _foreign_key_sql(table: str, key: dict) -> str:
    columns = ", ".join(f'"{column}"' for column in key["columns"])
    references = ", ".join(f'"{column}"' for column in key["references"])
    clause = (
        f'ALTER TABLE "{table}" ADD CONSTRAINT {key["name"]} '
        f'FOREIGN KEY ({columns}) REFERENCES "{key["on"]}" ({references})'
    )
    if key.get("on_delete"):
        clause += f" ON DELETE {str(key['on_delete']).upper()}"
    if key.get("on_update"):
        clause += f" ON UPDATE {str(key['on_update']).upper()}"
    return f"{clause} NOT VALID"


def _foreign_key_preflight(table: str, key: dict) -> str:
    """Child rows that would break the key, under MATCH SIMPLE semantics.

    A row with ANY null in the key columns is exempt from the constraint, so
    the probe only considers rows where every column is present — otherwise it
    would report violations Postgres will never raise.
    """
    present = " AND ".join(f'c."{column}" IS NOT NULL' for column in key["columns"])
    joined = " AND ".join(
        f'p."{reference}" = c."{column}"'
        for column, reference in zip(key["columns"], key["references"], strict=False)
    )
    return (
        f'SELECT 1 FROM "{table}" c WHERE {present} AND NOT EXISTS '
        f'(SELECT 1 FROM "{key["on"]}" p WHERE {joined}) LIMIT 1'
    )


def missing_foreign_keys(model: dict, table: str, live: LiveSchema) -> list[Operation]:
    """FOREIGN KEYs the model declares and the database does not have.

    The planner omitted these entirely, and so did ``schema:check`` — which is
    how a synkronus development database came to be missing 90 of the 104
    composite ``(child_id, tenant_id) -> parent(id, tenant_id)`` keys while
    every gate called it in sync. Those keys are where tenant isolation stops
    being a query habit and becomes a storage guarantee, and their absence is
    invisible until a database that HAS them (a fresh CI one, built from the
    same migrations) starts refusing rows the development database accepts.

    Same safe two-step as :func:`missing_checks`: ``ADD CONSTRAINT ... NOT
    VALID`` takes no scan, the paired ``VALIDATE`` scans without blocking
    writes, and a preflight proves the existing rows already satisfy the key
    before either runs.
    """
    live_keys = (live.constraints or {}).get(table, set())
    operations: list[Operation] = []
    for key in declared_foreign_keys(model, table):
        name = key["name"]
        if name in live_keys:
            continue
        if key["on"] not in live.tables:
            # Nothing to point at yet. The table this key references is itself
            # created by this plan, and its own key follows on the next one.
            continue
        operations.append(
            Operation(
                kind="add_foreign_key",
                table=table,
                key=f"{table}:{name}",
                forward_sql=_foreign_key_sql(table, key),
                reverse_sql=f'ALTER TABLE "{table}" DROP CONSTRAINT IF EXISTS {name}',
                safety=LOCKING,
                reason="model declares a FOREIGN KEY absent from the database",
                notes=(
                    "attached NOT VALID: existing rows are not scanned here — "
                    "the paired VALIDATE does that without blocking writes",
                ),
            )
        )
        operations.append(
            Operation(
                kind="validate_foreign_key",
                table=table,
                key=f"{table}:{name}:validate",
                forward_sql=f'ALTER TABLE "{table}" VALIDATE CONSTRAINT {name}',
                reverse_sql=None,
                safety=LOCKING,
                reason="prove the existing rows satisfy the attached FOREIGN KEY",
                preflight_sql=_foreign_key_preflight(table, key),
                preflight_failure=(
                    f"{table} holds rows with no {key['on']} parent for {name} — "
                    "repair the data before validating the constraint"
                ),
                notes=(
                    "no reverse recorded: validation has nothing to undo — the "
                    "paired ADD CONSTRAINT's reverse removes the constraint",
                ),
            )
        )
    return operations


def missing_checks(model: dict, table: str, live: LiveSchema) -> list[Operation]:
    """CHECK constraints the model declares and the database does not have.

    ``schema:check`` has always reported these; the planner silently omitted
    them, so a deployed database could never converge on a model that grew a
    CHECK. Each lands as the safe two-step recipe: ``ADD CONSTRAINT ... NOT
    VALID`` (metadata only, no scan) then ``VALIDATE CONSTRAINT`` (scans
    without blocking writes), with a preflight proving the rows already
    satisfy it before either runs.
    """
    live_checks = live.checks.get(table, set())
    operations: list[Operation] = []
    for name, expression in _declared_checks(model, table):
        if name in live_checks:
            continue
        operations.append(
            Operation(
                kind="add_check",
                table=table,
                key=f"{table}:{name}",
                forward_sql=(
                    f'ALTER TABLE "{table}" ADD CONSTRAINT {name} '
                    f"CHECK ({expression}) NOT VALID"
                ),
                reverse_sql=f'ALTER TABLE "{table}" DROP CONSTRAINT IF EXISTS {name}',
                safety=LOCKING,
                reason="model declares a CHECK constraint absent from the database",
                notes=(
                    "attached NOT VALID: existing rows are not scanned here — "
                    "the paired VALIDATE does that without blocking writes",
                ),
            )
        )
        operations.append(
            Operation(
                kind="validate_check",
                table=table,
                key=f"{table}:{name}:validate",
                forward_sql=f'ALTER TABLE "{table}" VALIDATE CONSTRAINT {name}',
                reverse_sql=None,
                safety=LOCKING,
                reason="prove the existing rows satisfy the attached CHECK",
                preflight_sql=(
                    f'SELECT 1 FROM "{table}" WHERE NOT ({expression}) LIMIT 1'
                ),
                preflight_failure=(
                    f"{table} holds rows violating {name} — repair the data "
                    "before validating the constraint"
                ),
                notes=(
                    "no reverse recorded: validation has nothing to undo — the "
                    "paired ADD CONSTRAINT's reverse removes the constraint",
                ),
            )
        )
    return operations


def orphaned_checks(model: dict, table: str, live: LiveSchema) -> list[Operation]:
    """CHECK constraints on a model-owned table that no declaration owns.

    The mirror of :func:`missing_checks`, with the same over-claiming rule as
    :func:`orphaned_indexes`: an ``__indexes__`` entry whose SQL cannot be
    read contributes its label, so the safe failure is an orphan surviving
    one more deploy rather than a declared object being dropped.
    """
    declared = {name for name, _ in _declared_checks(model, table)}
    for index in model.get("indexes", []) or []:
        up = index.get("up")
        if up:
            declared |= created_objects(up) or {index.get("name") or ""}

    operations: list[Operation] = []
    for name in sorted(live.checks.get(table, set()) - declared):
        operations.append(
            Operation(
                kind="drop_check",
                table=table,
                key=f"{table}:{name}",
                forward_sql=f'ALTER TABLE "{table}" DROP CONSTRAINT {name}',
                reverse_sql=None,
                safety=DESTRUCTIVE,
                reason="CHECK constraint in the database that no model declares",
                restores_data=False,
                notes=(
                    "no reverse is recorded: capture its definition from "
                    "pg_constraint before dropping if you may want it back",
                ),
            )
        )
    return operations


def missing_indexes(model: dict, table: str, live: LiveSchema) -> list[Operation]:
    """Index-shaped objects the model declares and the database does not have.

    Two sources, because a model has two ways to say "index this": the
    Blueprint flags (``field.string(...).index()``, ``field.index([...])``),
    whose SQL the planner renders from the declaration, and ``__indexes__``
    named-DDL entries, whose SQL is the entry's own ``up`` — nothing is
    re-rendered there. Missing the first source is not a cosmetic gap: a
    field-level index added to a model would never reach a deployed database
    and nothing would say so.
    """
    present = live.objects_on(table)
    live_indexes = live.indexes.get(table, set())
    live_constraints = live.constraints.get(table, set())
    operations: list[Operation] = []

    for name, columns, unique in _declared_blueprint_indexes(model, table):
        column_list = ", ".join(f'"{column}"' for column in columns)
        if not unique:
            if name in present:
                continue
        else:
            # A blueprint UNIQUE is a CONSTRAINT from zero (``table.unique``
            # emits ADD CONSTRAINT, and the backing index is Postgres's own).
            # Evolve must land the same shape, or ``schema:check`` — which
            # exempts constraint-backed indexes precisely because no model
            # names them — reads the bare index as an orphan forever. Three
            # live states: the constraint exists (done), only a bare index
            # exists (adopt it), neither exists (build CONCURRENTLY, then
            # adopt).
            if name in live_constraints:
                continue
            if name in live_indexes:
                operations.append(
                    _adopt_unique_index(table, name, columns, index_prebuilt=True)
                )
                continue
            operations.append(_adopt_unique_index(table, name, columns))
        operations.append(
            Operation(
                kind="create_index",
                table=table,
                key=f"{table}:{name}",
                # CONCURRENTLY by default: on a deployed table a plain build
                # holds a write lock for its duration, and the planner has no
                # reason to choose the blocking form when the model only asked
                # for an index.
                forward_sql=(
                    f"CREATE {'UNIQUE ' if unique else ''}INDEX CONCURRENTLY "
                    f'IF NOT EXISTS {name} ON "{table}" ({column_list})'
                ),
                reverse_sql=f"DROP INDEX CONCURRENTLY IF EXISTS {name}",
                safety=ADDITIVE,
                reason=(
                    f"model declares {'a unique' if unique else 'an'} index on "
                    f"{', '.join(columns)}"
                ),
                transactional=False,
                preflight_sql=(
                    f'SELECT 1 FROM "{table}" GROUP BY {column_list} '
                    f"HAVING COUNT(*) > 1 LIMIT 1"
                )
                if unique
                else None,
                preflight_failure=(
                    f"{table} already holds duplicate {', '.join(columns)} — a "
                    f"UNIQUE index cannot be built until they are resolved"
                )
                if unique
                else None,
                notes=(
                    "built CONCURRENTLY: cannot run in a transaction, and an "
                    "interrupted build leaves an INVALID index that re-running "
                    "replaces",
                ),
            )
        )

    for index in model.get("indexes", []) or []:
        name = index.get("name")
        up = index.get("up")
        if not name or not up:
            continue
        created = created_objects(up)
        if created is None:
            raise SchemaPlanRefused(
                f"{table}.__indexes__ entry '{name}' — the planner cannot tell "
                f"what this SQL creates, so it cannot tell whether the database "
                f"already has it. Left to guess it would either replan forever "
                f"or silently drop a real object from the plan. Express it as "
                f"DDL naming its object (CREATE [UNIQUE] INDEX, ADD CONSTRAINT, "
                f"CREATE TRIGGER, CREATE FUNCTION, CREATE EXTENSION, ADD COLUMN) "
                f"— a DO block wrapping one of those reads fine."
            )
        if created <= present:
            continue
        concurrent = "CONCURRENTLY" in up.upper()
        preflight_sql, preflight_failure = _unique_index_preflight(up, table, name)
        operations.append(
            Operation(
                kind="create_index",
                table=table,
                key=f"{table}:{name}",
                forward_sql=up,
                reverse_sql=index.get("down"),
                safety=ADDITIVE if concurrent else LOCKING,
                reason="named DDL declared by the model, absent from the database",
                transactional=not concurrent,
                preflight_sql=preflight_sql,
                preflight_failure=preflight_failure,
                notes=()
                if concurrent
                else ("builds with a write lock; declare it CONCURRENTLY to avoid that",),
            )
        )
    return operations


def orphaned_indexes(model: dict, table: str, live: LiveSchema) -> list[Operation]:
    """Indexes on a model-owned table that the model no longer declares.

    Removing ``.index()`` from a model is a real instruction, and without this
    the index survives in production forever with nothing reporting it — the
    mirror of the missing-index gap, and just as silent.

    Three exclusions keep it from firing on objects the model never owned:
    the indexes Postgres creates to BACK a constraint (a primary key, a unique
    constraint — dropping those means dropping the constraint), anything a
    ``__indexes__`` entry creates, and the primary key itself.

    Classed ``destructive`` even though no row is lost: an index is the
    difference between a query and an outage, so removing one must be an
    explicit decision. Its reverse fully restores it, which is why
    ``restores_data`` stays True — the object comes back complete, unlike a
    dropped column.
    """
    declared = {name for name, _, _ in _declared_blueprint_indexes(model, table)}
    for index in model.get("indexes", []) or []:
        up = index.get("up")
        if up:
            # An entry whose SQL cannot be read contributes the entry's own
            # LABEL here, deliberately. This side of the comparison decides
            # what to DROP, so the safe direction when the planner cannot tell
            # what an entry creates is to over-claim ownership: at worst an
            # orphan survives one more deploy, where under-claiming would drop
            # a live index the model does own. The refusal raised on the
            # missing-index side is where the operator hears about it.
            declared |= created_objects(up) or {index.get("name") or ""}

    backed_by_constraint = live.constraint_indexes.get(table, set())
    present = live.indexes.get(table, set())

    operations: list[Operation] = []
    for name in sorted(present - declared - backed_by_constraint):
        if name.endswith("_pkey"):
            continue
        operations.append(
            Operation(
                kind="drop_index",
                table=table,
                key=f"{table}:{name}",
                forward_sql=f"DROP INDEX CONCURRENTLY IF EXISTS {name}",
                # Rebuilt from the catalogue's own definition, so the reverse
                # is exact rather than a reconstruction from the model.
                reverse_sql=None,
                safety=DESTRUCTIVE,
                reason="index in the database that no model declares",
                transactional=False,
                notes=(
                    "no reverse is recorded: an index the model does not declare "
                    "has no declaration to rebuild it from. Capture its "
                    "definition from pg_indexes before dropping if you may want "
                    "it back",
                ),
            )
        )
    return operations


def orphaned_tables(model_tables: set[str], live: LiveSchema) -> list[str]:
    """Tables in the database that no model declares — REPORTED, never dropped.

    A DROP TABLE derived from a diff is where an autogenerating tool does its
    worst damage, because the diff cannot distinguish an abandoned table from
    a partition child, a table an extension owns, or the framework's own
    migration tracker. So this returns prose for a human, not an operation.
    """
    ignored = {"migrations"}
    # BASE TABLES only. A VIEW is never a table a model declares, so every
    # view in the schema matched this rule and was reported as an orphan
    # inviting a DROP — advice about a relation the application reads.
    orphans = sorted(live.base_table_names() - model_tables - ignored)
    return [
        f"table '{name}' exists in the database and no model declares it — "
        f"if it is obsolete, drop it by hand; the planner will not guess"
        for name in orphans
    ]


__all__ = [
    "created_objects",
    "missing_checks",
    "missing_indexes",
    "orphaned_checks",
    "orphaned_indexes",
    "orphaned_tables",
]
