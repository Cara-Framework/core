"""TransactionOwnership: use-case services own business transactions.

DOCTRINE §8 gives transaction ownership to the use-case service. Transport
edges and repositories must therefore not open, commit or roll back business
transactions. A repository may own one fully-contained atomic persistence
primitive only when the product manifest names that exact method.

Cara's ``atomic()`` (``with atomic():``, ``@atomic()``, ``atomic().run``)
opens the same transaction as ``DB.transaction()``, so it counts too.

The receiver need not be the ``DB`` facade: a manager INJECTED into the class
or function (``self.db.transaction()``, ``db.transaction()`` on a parameter,
``connection.commit()``, a name resolved from the container as ``"DB"``, or a
parameter annotated with a database/connection type) opens the same
transaction and counts the same.

Existing violations are counted per file in the exact, shrink-only
``seam_allowlists["transaction_ownership"]`` census.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path

from cara.architecture._ast_utils import (
    IMPORT_NODE_TYPES,
    nodes_of,
    parse,
    python_files,
    relpath,
)
from cara.architecture.Finding import Finding
from cara.architecture.Manifest import Manifest

TRANSACTION_KEY = "transaction_ownership"
_TRANSACTION_METHODS = frozenset(
    {
        "after_commit",
        "begin",
        "begin_transaction",
        "commit",
        "commit_open_transactions",
        "rollback",
        "transaction",
    }
)


@dataclass(frozen=True, slots=True)
class _TransactionCall:
    line: int
    identity: str
    operation: str


def _is_edge_path(path: Path, edge_layers: frozenset[str]) -> bool:
    parts = path.parts
    return any(
        part in edge_layers
        and (
            index == 0
            or parts[index - 1] in {"app", "packages"}
            or "packages" in parts[:index]
        )
        for index, part in enumerate(parts)
    )


def _is_repository_path(path: Path) -> bool:
    parts = path.parts
    return "repositories" in parts or any(
        left == "gates" and right == "persistence"
        for left, right in zip(parts, parts[1:], strict=False)
    )


def _call_name(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        prefix = _call_name(node.value)
        return f"{prefix}.{node.attr}" if prefix else node.attr
    if isinstance(node, ast.Call):
        return _call_name(node.func)
    return ""


#: Receiver names that conventionally hold an injected database manager or
#: connection (compared without leading underscores, case-insensitively).
_DB_RECEIVER_NAMES = frozenset(
    {
        "db",
        "database",
        "connection",
        "conn",
        "db_manager",
        "database_manager",
        "connection_manager",
        "db_connection",
        "database_connection",
    }
)
#: An annotation naming one of these types marks the annotated name a manager.
_DB_ANNOTATION_TOKENS = ("DatabaseManager", "Connection", "ConnectionResolver")
_CONTAINER_LOOKUPS = frozenset({"make", "resolve"})


def _is_db_receiver_name(name: str) -> bool:
    bare = name.lstrip("_").lower()
    return (
        bare in _DB_RECEIVER_NAMES
        or bare.endswith(("_db", "_connection"))
        or bare.startswith("db_")
    )


def _resolves_db(node: ast.AST) -> bool:
    """``app().make("DB")`` / ``resolve("DB")`` / ``container.make("DB")``."""
    if not isinstance(node, ast.Call) or not node.args:
        return False
    name = _call_name(node.func).rsplit(".", 1)[-1]
    target = node.args[0]
    return (
        name in _CONTAINER_LOOKUPS
        and isinstance(target, ast.Constant)
        and target.value == "DB"
    )


def _annotated_db(annotation: ast.AST | None) -> bool:
    if annotation is None:
        return False
    text = ast.unparse(annotation)
    return any(token in text for token in _DB_ANNOTATION_TOKENS) or text == "DB"


def _db_names(tree: ast.Module) -> set[str]:
    names = {"DB"}
    for node in tree.body:
        if not isinstance(node, ast.ImportFrom):
            continue
        if node.module not in {"cara", "cara.facades"}:
            continue
        for alias in node.names:
            if alias.name == "DB":
                names.add(alias.asname or alias.name)
    # Injected managers: parameters / attributes annotated with a database
    # type, and names bound to a container lookup of ``"DB"``.
    for node in nodes_of(tree, (ast.arg, ast.AnnAssign, ast.Assign)):
        if isinstance(node, ast.arg) and _annotated_db(node.annotation):
            names.add(node.arg)
        elif isinstance(node, ast.AnnAssign) and (
            _annotated_db(node.annotation)
            or (node.value is not None and _resolves_db(node.value))
        ):
            names.add(_target_name(node.target))
        elif isinstance(node, ast.Assign) and _resolves_db(node.value):
            names.update(_target_name(target) for target in node.targets)
    names.discard("")
    return names


def _target_name(target: ast.AST) -> str:
    if isinstance(target, ast.Name):
        return target.id
    if isinstance(target, ast.Attribute):
        return target.attr
    return ""


_ATOMIC_MODULES = frozenset({"cara.eloquent", "cara.eloquent.Atomic"})


def _atomic_names(tree: ast.Module) -> set[str]:
    """Local names bound to Cara's ``atomic`` (aliases included)."""
    names: set[str] = set()
    for node in nodes_of(tree, IMPORT_NODE_TYPES):
        if isinstance(node, ast.ImportFrom) and node.module in _ATOMIC_MODULES:
            for alias in node.names:
                if alias.name == "atomic":
                    names.add(alias.asname or alias.name)
    return names


def _db_operation(
    call: ast.Call, db_names: set[str], atomic_names: set[str] = frozenset()
) -> str | None:
    if isinstance(call.func, ast.Name) and call.func.id in atomic_names:
        return "atomic"
    name = _call_name(call.func)
    operation = name.rsplit(".", 1)[-1]
    if operation not in _TRANSACTION_METHODS:
        return None
    parts = name.split(".")
    if any(part in db_names for part in parts) or "DB" in parts:
        return operation
    receiver = call.func.value if isinstance(call.func, ast.Attribute) else None
    if receiver is None:
        return None
    if _resolves_db(receiver):
        return operation
    # The object the method is called on: ``self.db`` -> ``db``,
    # ``db`` -> ``db``, ``self.db.connection("x")`` -> ``connection``.
    owner = receiver.func if isinstance(receiver, ast.Call) else receiver
    owner_name = _call_name(owner).rsplit(".", 1)[-1]
    if owner_name and _is_db_receiver_name(owner_name):
        return operation
    return None


class _CallVisitor(ast.NodeVisitor):
    def __init__(self, rel: str, db_names: set[str], atomic_names: set[str]) -> None:
        self.rel = rel
        self.db_names = db_names
        self.atomic_names = atomic_names
        self.classes: list[str] = []
        self.functions: list[str] = []
        self.calls: list[_TransactionCall] = []

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self.classes.append(node.name)
        self.generic_visit(node)
        self.classes.pop()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self.functions.append(node.name)
        self.generic_visit(node)
        self.functions.pop()

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self.functions.append(node.name)
        self.generic_visit(node)
        self.functions.pop()

    def visit_Call(self, node: ast.Call) -> None:
        operation = _db_operation(node, self.db_names, self.atomic_names)
        if operation is not None:
            owner = ".".join([*self.classes, *self.functions]) or "<module>"
            self.calls.append(
                _TransactionCall(
                    line=node.lineno,
                    identity=f"{self.rel}::{owner}",
                    operation=operation,
                )
            )
        self.generic_visit(node)


def _calls(tree: ast.Module, rel: str) -> list[_TransactionCall]:
    visitor = _CallVisitor(rel, _db_names(tree), _atomic_names(tree))
    visitor.visit(tree)
    return visitor.calls


def _label(operation: str) -> str:
    return "atomic()" if operation == "atomic" else f"DB.{operation}"


class TransactionOwnership:
    """Keep transaction control out of transport edges and repositories."""

    @staticmethod
    def scan(manifest: Manifest) -> list[Finding]:
        found: dict[str, list[_TransactionCall]] = {}
        declared_atomic = manifest.atomic_repository_methods
        seen_atomic: set[str] = set()
        findings: list[Finding] = []
        seen: set[Path] = set()

        for root in manifest.roots.scan_dirs(TRANSACTION_KEY):
            for path in python_files(root):
                resolved = path.resolve()
                if resolved in seen or path.name == "__init__.py":
                    continue
                seen.add(resolved)
                rel = relpath(path, manifest.roots.deployable)
                relative = Path(rel)
                edge = _is_edge_path(relative, manifest.flow_edge_layers)
                repository = _is_repository_path(relative)
                if not edge and not repository:
                    continue
                tree = parse(path)
                if tree is None:
                    continue
                illegal: list[_TransactionCall] = []
                for call in _calls(tree, rel):
                    if repository and call.identity in declared_atomic:
                        seen_atomic.add(call.identity)
                        continue
                    illegal.append(call)
                if illegal:
                    found[rel] = illegal

        for identity in sorted(declared_atomic - seen_atomic):
            findings.append(
                Finding(
                    identity.split("::", 1)[0],
                    0,
                    f"stale atomic_repository_methods entry {identity!r}",
                )
            )

        allowlist = manifest.seam_allowlists.get(TRANSACTION_KEY, {})
        for rel, calls in sorted(found.items()):
            count = len(calls)
            pinned = allowlist.get(rel)
            detail = "; ".join(
                f"line {call.line}: {_label(call.operation)} in {call.identity}"
                for call in calls
            )
            if pinned is None:
                findings.append(
                    Finding(
                        rel,
                        calls[0].line,
                        f"{count} transaction-ownership violation(s): {detail}",
                    )
                )
            elif count > pinned:
                findings.append(
                    Finding(
                        rel,
                        calls[0].line,
                        f"transaction-ownership debt grew {pinned} -> {count}",
                    )
                )
            elif count < pinned:
                findings.append(
                    Finding(
                        rel,
                        calls[0].line,
                        f"stale transaction-ownership pin ({pinned}) — "
                        f"only {count} remain",
                    )
                )
        for rel, pinned in sorted(allowlist.items()):
            if rel not in found:
                findings.append(
                    Finding(
                        rel,
                        0,
                        f"stale transaction-ownership pin ({pinned}) — no hits remain",
                    )
                )
        return findings
