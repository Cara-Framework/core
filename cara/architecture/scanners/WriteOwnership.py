"""WriteOwnership: a shared table is never a shared pen (DOCTRINE §7).

Every model-backed table has one declared owner: ``api-owned``,
``services-owned`` or ``shared-gate-owned``. This scanner resolves direct ORM
class writes, query-builder writes and literal write SQL, then rejects writes
outside the owning deployable/gate. Existing cross-owner writes are exact,
shrink-only ``seam_allowlists["write_ownership"]`` debt keyed by
``path::table``.
"""

from __future__ import annotations

import ast
import re
from collections import defaultdict
from pathlib import Path

from cara.architecture._ast_utils import parse, python_files, relpath
from cara.architecture.Finding import Finding
from cara.architecture.Manifest import Manifest

WRITE_KEY = "write_ownership"
OWNERS = frozenset({"api-owned", "services-owned", "shared-gate-owned"})
_MUTATORS = frozenset(
    {
        "create",
        "create_many",
        "decrement",
        "delete",
        "destroy",
        "force_delete",
        "first_or_create",
        "increment",
        "insert",
        "insert_get_id",
        "save",
        "truncate",
        "update",
        "update_or_create",
        "upsert",
    }
)
_RAW_SQL_CALLS = frozenset(
    {"delete", "execute", "insert", "statement", "unprepared", "update"}
)
_WRITE_SQL = re.compile(
    r"\b(?:insert\s+into|update|delete\s+from|truncate(?:\s+table)?)\s+"
    r"(?:[\"`]?[A-Za-z_][A-Za-z0-9_]*[\"`]?\.)?"
    r"[\"`]?([A-Za-z_][A-Za-z0-9_]*)[\"`]?",
    re.IGNORECASE,
)
_SQL_NON_TABLE_TOKENS = frozenset({"returning", "set", "values", "where"})


def _model_tables(manifest: Manifest) -> tuple[dict[str, str], dict[str, str]]:
    by_class: dict[str, str] = {}
    by_table: dict[str, str] = {}
    root = manifest.roots.kernel.get("models")
    if root is None or not root.is_dir():
        return by_class, by_table
    for path in python_files(root):
        tree = parse(path)
        if tree is None:
            continue
        for node in tree.body:
            if not isinstance(node, ast.ClassDef):
                continue
            table: str | None = None
            for item in node.body:
                if not isinstance(item, (ast.Assign, ast.AnnAssign)):
                    continue
                targets = item.targets if isinstance(item, ast.Assign) else [item.target]
                value = item.value
                if not any(
                    isinstance(target, ast.Name) and target.id == "__table__"
                    for target in targets
                ):
                    continue
                if isinstance(value, ast.Constant) and isinstance(value.value, str):
                    table = value.value
                    break
            if table is None:
                continue
            by_class[node.name] = table
            by_table[table] = node.name
    return by_class, by_table


def _root_name(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return _root_name(node.value)
    if isinstance(node, ast.Call):
        return _root_name(node.func)
    if isinstance(node, ast.Subscript):
        return _root_name(node.value)
    if isinstance(node, ast.Await):
        return _root_name(node.value)
    return ""


def _literal(node: ast.AST) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        pieces: list[str] = []
        for value in node.values:
            if not isinstance(value, ast.Constant) or not isinstance(value.value, str):
                return None
            pieces.append(value.value)
        return "".join(pieces)
    return None


def _query_table(call: ast.Call) -> str | None:
    node: ast.AST = call.func
    while isinstance(node, (ast.Attribute, ast.Call, ast.Subscript)):
        if isinstance(node, ast.Call):
            if (
                isinstance(node.func, ast.Attribute)
                and node.func.attr in {"from_", "table"}
                and node.args
            ):
                return _literal(node.args[0])
            node = node.func
        elif isinstance(node, ast.Attribute):
            node = node.value
        else:
            node = node.value
    return None


def _raw_sql_tables(call: ast.Call) -> list[str]:
    if not isinstance(call.func, ast.Attribute):
        return []
    if call.func.attr not in _RAW_SQL_CALLS or not call.args:
        return []
    sql = _literal(call.args[0])
    if sql is None:
        return []
    return [
        match.group(1)
        for match in _WRITE_SQL.finditer(sql)
        if match.group(1).casefold() not in _SQL_NON_TABLE_TOKENS
    ]


def _annotation_name(node: ast.AST | None) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value.rsplit(".", 1)[-1]
    if isinstance(node, ast.Subscript):
        return _annotation_name(node.value)
    return ""


def _model_symbols(
    tree: ast.Module,
    model_tables: dict[str, str],
) -> dict[str, str]:
    symbols = dict(model_tables)
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            for alias in node.names:
                table = model_tables.get(alias.name)
                if table is not None:
                    symbols[alias.asname or alias.name] = table
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            arguments = [
                *node.args.posonlyargs,
                *node.args.args,
                *node.args.kwonlyargs,
            ]
            if node.args.vararg is not None:
                arguments.append(node.args.vararg)
            if node.args.kwarg is not None:
                arguments.append(node.args.kwarg)
            for argument in arguments:
                table = symbols.get(_annotation_name(argument.annotation))
                if table is not None:
                    symbols[argument.arg] = table
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            table = symbols.get(_annotation_name(node.annotation))
            if table is not None:
                symbols[node.target.id] = table
        elif isinstance(node, (ast.Assign, ast.NamedExpr)):
            value = node.value
            table = symbols.get(_root_name(value))
            if table is None:
                continue
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for target in targets:
                if isinstance(target, ast.Name):
                    symbols[target.id] = table
    return symbols


def _writes(tree: ast.Module, model_tables: dict[str, str]) -> list[tuple[int, str]]:
    model_symbols = _model_symbols(tree, model_tables)
    hits: set[tuple[int, str]] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        for table in _raw_sql_tables(node):
            hits.add((node.lineno, table))
        if not isinstance(node.func, ast.Attribute) or node.func.attr not in _MUTATORS:
            continue
        table = _query_table(node)
        if table:
            hits.add((node.lineno, table))
            continue
        model = _root_name(node.func.value)
        if model in model_symbols:
            hits.add((node.lineno, model_symbols[model]))
    return sorted(hits)


def _shared_gate_path(rel: str) -> bool:
    parts = Path(rel).parts
    return any(
        left == "gates" and right == "persistence"
        for left, right in zip(parts, parts[1:], strict=False)
    )


def _legal_owner(manifest: Manifest, rel: str, table: str) -> bool:
    owner = manifest.write_ownership.get(table)
    if owner == f"{manifest.deployable}-owned":
        return True
    return owner == "shared-gate-owned" and _shared_gate_path(rel)


class WriteOwnership:
    """Enforce model-table ownership against statically visible writes."""

    @staticmethod
    def scan(manifest: Manifest) -> list[Finding]:
        model_tables, declared_models = _model_tables(manifest)
        ownership = manifest.write_ownership
        findings: list[Finding] = []

        invalid = sorted(
            f"{table}={owner}"
            for table, owner in ownership.items()
            if owner not in OWNERS
        )
        for entry in invalid:
            findings.append(
                Finding(
                    "app/architecture_manifest.py",
                    0,
                    f"invalid write owner {entry!r}; expected one of {sorted(OWNERS)}",
                )
            )
        missing = sorted(set(declared_models) - set(ownership))
        for table in missing:
            findings.append(
                Finding(
                    "app/architecture_manifest.py",
                    0,
                    f"model table {table!r} has no write owner",
                )
            )
        stale = sorted(
            set(ownership) - set(declared_models) - manifest.model_less_write_tables
        )
        for table in stale:
            findings.append(
                Finding(
                    "app/architecture_manifest.py",
                    0,
                    f"write owner for unknown table {table!r} is stale",
                )
            )

        found: dict[str, int] = defaultdict(int)
        seen: set[Path] = set()
        for root in manifest.roots.scan_dirs(WRITE_KEY):
            for path in python_files(root):
                resolved = path.resolve()
                if resolved in seen or path.name == "__init__.py":
                    continue
                seen.add(resolved)
                rel = relpath(path, manifest.roots.deployable)
                tree = parse(path)
                if tree is None:
                    continue
                for _, table in _writes(tree, model_tables):
                    if table not in ownership:
                        findings.append(
                            Finding(rel, 0, f"write targets unowned table {table!r}")
                        )
                    elif not _legal_owner(manifest, rel, table):
                        found[f"{rel}::{table}"] += 1

        allowlist = manifest.seam_allowlists.get(WRITE_KEY, {})
        for identity, count in sorted(found.items()):
            pinned = allowlist.get(identity)
            path, table = identity.split("::", 1)
            owner = ownership[table]
            if pinned is None:
                findings.append(
                    Finding(
                        path,
                        0,
                        f"{count} cross-owner write(s) to {table!r} ({owner})",
                    )
                )
            elif count > pinned:
                findings.append(
                    Finding(
                        path,
                        0,
                        f"write-ownership debt grew for {identity}: {pinned} -> {count}",
                    )
                )
            elif count < pinned:
                findings.append(
                    Finding(
                        path,
                        0,
                        f"stale write-ownership pin for {identity}: "
                        f"{pinned}, now {count}",
                    )
                )
        for identity, pinned in sorted(allowlist.items()):
            if identity not in found:
                findings.append(
                    Finding(
                        identity.split("::", 1)[0],
                        0,
                        f"stale write-ownership pin for {identity}: "
                        f"{pinned}, violation resolved",
                    )
                )
        return findings
