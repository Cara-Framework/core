"""ModelSchema: read declared schema objects out of the MODELS, the SSOT.

Schema invariants used to be asserted by importing a hand-written migration and
inspecting its source (or regexing it). That coupling died with the
one-file-per-table regenerate: migrations are a pure function of the models, so
a test pinned to a migration file asserts nothing about the schema and breaks
the moment the file is regenerated under a new number.

These helpers read the same declarations ``make:migration`` and
``schema:check`` consume (``ModelDiscoverer``), so a test written against them
fails exactly when the real invariant is lost — not when a file is renamed.
Products call the module functions through the :class:`ModelSchema` facade.
"""

from __future__ import annotations

import re
from copy import deepcopy
from functools import cache

from cara.eloquent.migrations import ModelDiscoverer

# ``CREATE [UNIQUE] INDEX [IF NOT EXISTS] <name> ON <table> (<body>) [WHERE ...]``
_CREATE_INDEX = re.compile(
    r"CREATE\s+(?P<unique>UNIQUE\s+)?INDEX\s+(?:CONCURRENTLY\s+)?"
    r"(?:IF\s+NOT\s+EXISTS\s+)?\"?(?P<name>\w+)\"?\s+ON\s+\"?(?P<table>\w+)\"?",
    re.IGNORECASE,
)


@cache
def _scanned_models() -> tuple[dict, ...]:
    return tuple(ModelDiscoverer().discover_models())


def discovered_models() -> list[dict]:
    """Every model declaration, discovered once per test process.

    Discovery AST-parses every Python file under the workspace root — on a
    local monorepo checkout that is several thousand files, the shared kernel
    reached once per symlink. Each schema test used to pay that scan again
    (some of them two or three times), and under machine load a single test
    crossed the 60 s per-test timeout inside it. The models cannot change while
    the suite runs, so the scan is shared; every caller still gets its own deep
    copy, so a test that edits a declaration cannot leak into the next one.
    """
    return deepcopy(list(_scanned_models()))


def declared_indexes(models: list[dict] | None = None) -> dict[str, dict]:
    """Every index the models declare (``models`` defaults to discovery), by name.

    Each value carries ``table``, ``columns`` (None when the declaration is raw
    SQL whose body we do not parse), ``sql`` (None for ``field.index``) and
    ``predicate``.
    """
    found: dict[str, dict] = {}
    for model in discovered_models() if models is None else models:
        table = model.get("table") or ""

        for index in model.get("indexes", []) or []:
            sql = " ".join((index.get("up") or "").split())
            match = _CREATE_INDEX.search(sql)
            if not match:
                continue
            body, predicate = _split_index_body(sql)
            found[match.group("name")] = {
                "table": match.group("table"),
                "columns": body,
                "predicate": predicate,
                "sql": sql,
                "unique": bool(match.group("unique")),
                "model": model.get("name"),
            }

        for declaration in model.get("composite_indexes", []) or []:
            columns = list(declaration.get("columns") or [])
            name = declaration.get("name") or f"{table}_{'_'.join(columns)}_index"
            # Postgres truncates identifiers at 63 bytes.
            found[name[:63]] = {
                "table": table,
                "columns": tuple(columns),
                "predicate": "",
                "sql": None,
                "unique": False,
                "model": model.get("name"),
            }
    return found


def _split_index_body(sql: str) -> tuple[tuple[str, ...] | None, str]:
    """Split a CREATE INDEX into its column tuple and its WHERE predicate.

    Returns ``(None, predicate)`` when the column list contains an expression we
    do not attempt to tokenise into comparable columns.
    """
    try:
        after_on = sql.split(" ON ", 1)[1]
        start = after_on.index("(")
    except IndexError, ValueError:
        return None, ""

    depth = 0
    quoted = False
    end = None
    for offset in range(start, len(after_on)):
        char = after_on[offset]
        if char == "'":
            quoted = not quoted
        elif not quoted:
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
                if depth == 0:
                    end = offset
                    break
    if end is None:
        return None, ""

    columns_sql = after_on[start + 1 : end]
    tail = after_on[end + 1 :].strip()
    # A covering index's ``INCLUDE (...)`` columns are payload, not keys: the
    # predicate follows them (it read as "" — no predicate — before G19).
    if tail.upper().startswith("INCLUDE") and ")" in tail:
        tail = tail[tail.index(")") + 1 :].strip()
    predicate = ""
    if tail.upper().startswith("WHERE "):
        predicate = tail[len("WHERE ") :].strip()

    columns: list[str] = []
    token_start = 0
    depth = 0
    quoted = False
    for offset, char in enumerate(columns_sql):
        if char == "'":
            quoted = not quoted
        elif not quoted:
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
            elif char == "," and depth == 0:
                columns.append(columns_sql[token_start:offset].strip())
                token_start = offset + 1
    columns.append(columns_sql[token_start:].strip())
    return tuple(c for c in columns if c), predicate


def declared_constraint_sql() -> dict[str, str]:
    """Model-declared named constraints, by name.

    Composite foreign keys use the schema DSL, including column-scoped
    ``SET NULL`` actions. Raw named constraints in ``__indexes__`` are included
    too, so invariant tests inspect both declaration forms through one door.
    """
    found: dict[str, str] = {}
    for model in discovered_models():
        table = model.get("table") or ""
        for check in model.get("checks", []) or []:
            name = check.get("name")
            expression = check.get("expression")
            if name and expression:
                found[name] = (
                    f"ALTER TABLE {table} ADD CONSTRAINT {name} CHECK ({expression})"
                )
        for index in model.get("indexes", []) or []:
            sql = " ".join((index.get("up") or "").split())
            match = re.search(
                r"ADD\s+CONSTRAINT\s+\"?(?P<name>\w+)\"?", sql, re.IGNORECASE
            )
            if match:
                found[match.group("name")] = sql
        for foreign_key in model.get("composite_foreign_keys", []) or []:
            name = foreign_key.get("name")
            columns = foreign_key.get("columns") or []
            references = foreign_key.get("references") or []
            parent = foreign_key.get("on")
            if not name or not columns or not references or not parent:
                continue
            child_columns = ", ".join(columns)
            parent_columns = ", ".join(references)
            on_delete = foreign_key.get("on_delete")
            delete_sql = ""
            if on_delete:
                delete_sql = f" ON DELETE {on_delete.upper()}"
                delete_columns = foreign_key.get("on_delete_columns") or []
                if delete_columns:
                    delete_sql += f" ({', '.join(delete_columns)})"
            found[name] = (
                f"ALTER TABLE {table} ADD CONSTRAINT {name} "
                f"FOREIGN KEY ({child_columns}) "
                f"REFERENCES {parent} ({parent_columns}){delete_sql}"
            )
    return found


def declared_composite_uniques() -> set[tuple[str, tuple[str, ...]]]:
    """``field.unique([...])`` declarations as ``(table, columns)`` pairs."""
    found: set[tuple[str, tuple[str, ...]]] = set()
    for model in discovered_models():
        table = model.get("table") or ""
        for declaration in model.get("composite_uniques", []) or []:
            columns = tuple(declaration.get("columns") or [])
            if columns:
                found.add((table, columns))
    return found


class ModelSchema:
    """The model-declared schema, read once per process (copies per call)."""

    discovered_models = staticmethod(discovered_models)
    declared_indexes = staticmethod(declared_indexes)
    declared_constraint_sql = staticmethod(declared_constraint_sql)
    declared_composite_uniques = staticmethod(declared_composite_uniques)
    split_index_body = staticmethod(_split_index_body)
