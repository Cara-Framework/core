"""What indexes does a model declare, and with which definition?

One answer for every reader. :mod:`cara.schema.Objects` asks it by NAME —
"does the database already have this one?" — and :mod:`cara.schema.IndexDrift`
by DEFINITION — "is the one it has the one declared?". Both routes a model
has are read here: the Blueprint's (field flags, ``field.index([...])``,
``field.unique([...])``) rendered the way the generator renders them, and
every ``CREATE [UNIQUE] INDEX`` an ``__indexes__`` entry runs, read out of
the entry's own SQL.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

#: PostgreSQL truncates every identifier to NAMEDATALEN-1 bytes. The stored
#: name is the truncated one, so a planner comparing the full convention name
#: against the catalogue finds nothing and plans an index that already exists —
#: on this schema, 23 of them, every single plan.
_MAX_IDENTIFIER_LENGTH = 63

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

_IDENT = r'(?:"[^"]+"|\w+)'

#: ``CREATE [UNIQUE] INDEX [CONCURRENTLY] [IF NOT EXISTS] name ON [ONLY] table``
#: — the part of an index statement that names it and its table. Everything
#: after it is the definition. It reads the server's ``pg_get_indexdef`` and a
#: model's own SQL alike; a nameless ``CREATE INDEX ON t`` has nothing to
#: compare by and is not matched.
_INDEX_HEADER_RE = re.compile(
    r"CREATE\s+(?P<unique>UNIQUE\s+)?INDEX\s+(?:CONCURRENTLY\s+)?"
    r"(?:IF\s+NOT\s+EXISTS\s+)?(?!(?:ON|CONCURRENTLY|IF)\b)(?P<name>" + _IDENT + r")"
    r"\s+ON\s+(?P<only>ONLY\s+)?(?P<table>(?:" + _IDENT + r"\.)?" + _IDENT + r")",
    re.IGNORECASE,
)


def _unique_index_preflight(
    up_sql: str, table: str, index_name: str
) -> tuple[str | None, str | None]:
    match = _SIMPLE_UNIQUE_INDEX_RE.match(up_sql)
    if match is None:
        return None, None
    columns = " ".join(match.group("columns").split())
    predicate = match.group("predicate")
    where = f" WHERE ({columns}) IS NOT NULL"  # NULLS DISTINCT: NULL keys never collide
    if predicate:
        where += f" AND ({' '.join(predicate.rstrip(';').split())})"
    return (
        f'SELECT 1 FROM "{table}"{where} GROUP BY {columns} HAVING COUNT(*) > 1 LIMIT 1',
        f"{table} already holds duplicate keys for {index_name} — resolve them "
        "before building the UNIQUE index",
    )


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


def _unquote(identifier: str) -> str:
    return identifier.strip().strip('"')


def _statement_end(sql: str, start: int) -> int:
    """Where the statement running from ``start`` ends: its ``;`` (or the
    ``$$`` closing a DO block) outside quotes and parentheses, else the end."""
    depth = 0
    quote: str | None = None
    for position in range(start, len(sql)):
        char = sql[position]
        if quote:
            if char == quote:
                quote = None
        elif char in ("'", '"'):
            quote = char
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif depth <= 0 and (char == ";" or sql.startswith("$$", position)):
            return position
    return len(sql)


@dataclass(frozen=True)
class DeclaredIndex:
    """One index a model declares, by either route.

    ``rest`` is everything after the table in its ``CREATE INDEX`` —
    ``("a", "b")`` for a blueprint index, the entry's own text (``USING gin
    (...) WHERE ...``) for a raw one — so the probe, the reshape build and
    the comparison all start from the model's own words. ``constraint``
    marks a blueprint UNIQUE, which a from-zero migrate lands as a
    CONSTRAINT rather than a bare index.
    """

    name: str
    unique: bool
    rest: str
    constraint: bool = False
    only: bool = False

    def statement(
        self, table: str, name: str | None = None, *, concurrently: bool = False
    ) -> str:
        return (
            f"CREATE {'UNIQUE ' if self.unique else ''}INDEX "
            f"{'CONCURRENTLY IF NOT EXISTS ' if concurrently else ''}"
            f'{name or self.name} ON {"ONLY " if self.only else ""}"{table}"{self.rest}'
        )


def declared_indexes(model: dict, table: str) -> list[DeclaredIndex]:
    """Every index on ``table`` the model declares, with its definition.

    Entries that create triggers, functions, constraints or extensions
    contribute nothing here — they are named objects, compared by
    :mod:`cara.schema.Objects` exactly as before. A statement aimed at
    another table belongs to that table, not to this model's.
    """
    found: dict[str, DeclaredIndex] = {}
    for name, columns, unique in _declared_blueprint_indexes(model, table):
        column_list = ", ".join(f'"{column}"' for column in columns)
        found[name] = DeclaredIndex(name, unique, f" ({column_list})", constraint=unique)

    for entry in model.get("indexes", []) or []:
        up = entry.get("up") or ""
        for match in _INDEX_HEADER_RE.finditer(up):
            if _unquote(match.group("table").split(".")[-1]) != table:
                continue
            name = _unquote(match.group("name"))
            if name in found:
                continue
            rest = up[match.end() : _statement_end(up, match.end())].rstrip()
            found[name] = DeclaredIndex(
                name,
                bool(match.group("unique")),
                rest,
                only=bool(match.group("only")),
            )
    return list(found.values())


def declared_indexes_by_table(
    models: list[dict],
) -> dict[str, list[tuple[str, bool, str]]]:
    """``{table: [(name, unique, rest)]}`` — what
    :func:`cara.schema.LiveSchema.normalize_declared_indexes` spells."""
    return {
        model["table"]: [
            (index.name, index.unique, index.rest)
            for index in declared_indexes(model, model["table"])
        ]
        for model in models
        if model.get("table")
    }


def index_parts(definition: str | None) -> tuple[bool, str] | None:
    """``(unique, rest)`` of an index statement — enough to build it again on
    another table. None when the text is not an index definition."""
    text = str(definition or "").strip()
    match = _INDEX_HEADER_RE.match(text)
    if match is None:
        return None
    return bool(match.group("unique")), text[match.end() :]


def index_shape(definition: str | None) -> str | None:
    """An index definition without its name and table — what it indexes.

    ``CREATE UNIQUE INDEX x ON public.t USING btree (a, b) WHERE (c)`` reads
    as ``UNIQUE USING btree (a, b) WHERE (c)``. None when the text is not an
    index definition this can read.
    """
    parts = index_parts(definition)
    if parts is None:
        return None
    unique, rest = parts
    return f"{'UNIQUE ' if unique else ''}{' '.join(rest.split())}"


__all__ = [
    "DeclaredIndex",
    "declared_indexes",
    "declared_indexes_by_table",
    "index_parts",
    "index_shape",
]
