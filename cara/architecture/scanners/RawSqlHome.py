"""RawSqlHome: raw SQL lives in repositories and nowhere else (DOCTRINE §5).

Raw SQL reaches the database in two forms, and both are repository-only:

* EXECUTING — ``DB.select`` / ``DB.select_one`` / ``DB.statement``, a
  ``DatabaseManager`` instance's equivalents, or a DB-API ``cursor()`` /
  ``cursor.execute(SQL, ...)``;
* COMPOSING — ``.where_raw`` / ``.select_raw`` / ``.order_by_raw`` /
  ``.group_by_raw`` / ``.having_raw`` / ``.join_raw`` on any builder.

A bare SQL string literal anywhere outside a raw-SQL home counts too: a
query that is merely *written* elsewhere is a query whose owner is already
wrong, whatever executes it later.

Three exemptions, and only three:

* module/class/function DOCSTRINGS — prose about a query is not a query;
* SCHEMA METADATA (``__indexes__`` / ``__views__`` assignments) — the model
  IS the schema's single source of truth (the atlas's migration rule), so
  its own DDL is a declaration, not a stray query;
* ONE deliberate QUERY COMPILER class per product, opted in by naming
  :data:`QUERY_COMPILER_MARKER` in its docstring. Doctrine allows exactly
  one such class; a second is itself a Finding, so the escape hatch cannot
  quietly become a second persistence layer.

Which trees hold a legal home is ``manifest.raw_sql_homes``: each entry is a
POSIX path fragment matched against a contiguous run of a file's
deployable-relative path parts, so ``"repositories"`` exempts a repository at
any depth while ``"commons/gates/persistence"`` exempts only that package.

A product adopting this guard over a tree that is not yet clean pins the
debt in ``seam_allowlists[ALLOWLIST_KEY]`` as ``path -> site count``: exact,
shrink-only, and stale-loud (see :mod:`cara.architecture._ratchet`). That is
the only sanctioned way to widen scope without weakening the rule.

Prior state: four product copies of this scanner enforced three different
rules — one honoured both exemptions, one only schema metadata, one neither.
The identical doctrine sentence fired differently per tree, which is the
drift DOCTRINE §11 exists to end.
"""

from __future__ import annotations

import ast
import re

from cara.architecture._ast_utils import docstring_node_ids, iter_modules
from cara.architecture._ratchet import ratchet
from cara.architecture.Finding import Finding
from cara.architecture.Manifest import Manifest

#: ``seam_allowlists`` key holding ``path -> raw-SQL site count`` for a tree a
#: product adopted the guard over before cleaning it. Shrink-only.
ALLOWLIST_KEY = "raw_sql_home"

#: Docstring marker that opts a class into being THE product's query compiler.
QUERY_COMPILER_MARKER = "Doctrine §5 query compiler"

EXEC_METHODS = frozenset({"select", "select_one", "statement"})
COMPOSE_METHODS = frozenset(
    {"where_raw", "select_raw", "order_by_raw", "group_by_raw", "having_raw", "join_raw"}
)
CURSOR_EXEC_METHODS = frozenset({"execute", "executemany"})

SQL_START = re.compile(
    r"^\s*(?:"
    r"SELECT\b[\s\S]*\bFROM\b|SELECT\s+1\b|"
    r"INSERT\b[\s\S]*\bINTO\b|UPDATE\b[\s\S]*\bSET\b|"
    r"DELETE\b[\s\S]*\bFROM\b|WITH\b[\s\S]*\b(?:SELECT|INSERT|UPDATE|DELETE)\b|"
    r"TRUNCATE\s+TABLE\b|(?:ALTER|CREATE|DROP)\s+(?:TABLE|INDEX|VIEW)\b"
    r")",
    re.IGNORECASE,
)

_SCHEMA_METADATA_NAMES = frozenset({"__indexes__", "__views__"})


def _bound_database_names(tree: ast.Module) -> set[str]:
    """Every local name that reaches the database driver directly.

    Covers the facade under any alias (``from cara.facades import DB as D``)
    and a manager instance bound from ``DatabaseManager.get_instance()`` or
    ``get_database_manager()`` — the two ways product code has historically
    re-acquired the connection while looking like a plain object.
    """
    facades: set[str] = set()
    managers: set[str] = set()
    factories: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.ImportFrom) or not node.module:
            continue
        if node.module == "cara.facades":
            facades.update(
                alias.asname or alias.name for alias in node.names if alias.name == "DB"
            )
        elif node.module == "cara.eloquent":
            for alias in node.names:
                bound = alias.asname or alias.name
                if alias.name == "DatabaseManager":
                    managers.add(bound)
                elif alias.name == "get_database_manager":
                    factories.add(bound)

    instances: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        value = node.value
        if not isinstance(value, ast.Call):
            continue
        is_instance = (
            isinstance(value.func, ast.Attribute)
            and value.func.attr == "get_instance"
            and isinstance(value.func.value, ast.Name)
            and value.func.value.id in managers
        ) or (isinstance(value.func, ast.Name) and value.func.id in factories)
        if not is_instance:
            continue
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        instances.update(target.id for target in targets if isinstance(target, ast.Name))
    return facades | instances


def _literal_text(node: ast.AST) -> str | None:
    """The static text of a string constant or f-string, if it has any."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        return "".join(
            value.value
            for value in node.values
            if isinstance(value, ast.Constant) and isinstance(value.value, str)
        )
    return None


def _contains_sql_literal(node: ast.Call) -> bool:
    return any(
        bool(text and SQL_START.search(text))
        for argument in (*node.args, *(keyword.value for keyword in node.keywords))
        if (text := _literal_text(argument)) is not None
    )


def _schema_metadata_node_ids(tree: ast.Module) -> set[int]:
    """Nodes inside an ``__indexes__`` / ``__views__`` declaration."""
    ids: set[int] = set()
    for node in ast.walk(tree):
        if not isinstance(node, (ast.Assign, ast.AnnAssign)) or node.value is None:
            continue
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        if not any(
            isinstance(target, ast.Name) and target.id in _SCHEMA_METADATA_NAMES
            for target in targets
        ):
            continue
        ids.update(id(child) for child in ast.walk(node.value))
    return ids


def query_compiler_classes(tree: ast.Module) -> list[ast.ClassDef]:
    """Top-level classes whose docstring opts into the compiler exemption."""
    return [
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef)
        and QUERY_COMPILER_MARKER in (ast.get_docstring(node, clean=False) or "")
    ]


def raw_sql_findings(tree: ast.Module, rel: str) -> list[Finding]:
    """Every raw-SQL site in one module, exemptions already applied.

    Public so a product may assert the rule against a synthetic tree without
    reimplementing it — the mistake this scanner replaces.
    """
    database_names = _bound_database_names(tree)
    compiler_ids = {
        id(child)
        for compiler in query_compiler_classes(tree)
        for child in ast.walk(compiler)
    }
    findings: list[Finding] = []
    for node in ast.walk(tree):
        if id(node) in compiler_ids:
            continue
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        receiver = node.func.value
        method = node.func.attr
        receiver_is_database = (
            isinstance(receiver, ast.Name) and receiver.id in database_names
        )
        if method in COMPOSE_METHODS or (
            method in EXEC_METHODS
            and (receiver_is_database or _contains_sql_literal(node))
        ):
            findings.append(Finding(rel, node.lineno, f".{method}(...) composes raw SQL"))
        elif method == "cursor":
            findings.append(
                Finding(rel, node.lineno, ".cursor(...) reaches the driver directly")
            )
        elif method in CURSOR_EXEC_METHODS and _contains_sql_literal(node):
            findings.append(
                Finding(rel, node.lineno, f".{method}(SQL, ...) executes SQL")
            )

    excluded = docstring_node_ids(tree) | _schema_metadata_node_ids(tree) | compiler_ids
    for node in ast.walk(tree):
        if id(node) in excluded:
            continue
        text = _literal_text(node)
        if text and SQL_START.search(text):
            findings.append(Finding(rel, node.lineno, "SQL literal outside a repository"))
    return findings


def _is_home(rel: str, homes: frozenset[str]) -> bool:
    parts = rel.split("/")
    for home in homes:
        segments = [segment for segment in home.split("/") if segment]
        if not segments:
            continue
        span = len(segments)
        if any(parts[i : i + span] == segments for i in range(len(parts) - span + 1)):
            return True
    return False


class RawSqlHome:
    """Raw SQL only inside a declared repository home (DOCTRINE §5)."""

    @staticmethod
    def scan(manifest: Manifest) -> list[Finding]:
        pinned = manifest.seam_allowlists.get(ALLOWLIST_KEY, {})
        findings: list[Finding] = []
        counts: dict[str, int] = {}
        compilers: list[str] = []
        for _path, rel, tree in iter_modules(
            manifest.roots.scan_dirs("raw_sql_home"), manifest.roots.deployable
        ):
            if _is_home(rel, manifest.raw_sql_homes):
                continue
            compilers.extend(
                f"{rel}:{node.lineno} {node.name}"
                for node in query_compiler_classes(tree)
            )
            hits = raw_sql_findings(tree, rel)
            if not hits:
                continue
            # A file the product has NOT pinned reports every site, so the
            # message names the statement to move. A pinned file reports only
            # its count, through the ratchet, so growth and stale pins fail
            # without re-listing debt the product already acknowledged.
            if rel in pinned:
                counts[rel] = len(hits)
            else:
                findings.extend(hits)
        findings.extend(
            ratchet(
                key=ALLOWLIST_KEY,
                current=counts,
                pinned=pinned,
                message="raw SQL outside a repository home",
            )
        )
        if len(compilers) > 1:
            findings.append(
                Finding(
                    "app/architecture_manifest.py",
                    0,
                    "only ONE query-compiler class is legal product-wide, found: "
                    + ", ".join(sorted(compilers)),
                )
            )
        return findings
