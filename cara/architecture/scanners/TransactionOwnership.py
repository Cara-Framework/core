"""TransactionOwnership: use-case services own business transactions.

DOCTRINE §8 gives transaction ownership to the use-case service. Transport
edges and repositories must therefore not open, commit or roll back business
transactions. A repository may own one fully-contained atomic persistence
primitive only when the product manifest names that exact method.

Existing violations are counted per file in the exact, shrink-only
``seam_allowlists["transaction_ownership"]`` census.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path

from cara.architecture._ast_utils import parse, python_files, relpath
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
    return names


def _db_operation(call: ast.Call, db_names: set[str]) -> str | None:
    name = _call_name(call.func)
    operation = name.rsplit(".", 1)[-1]
    if operation not in _TRANSACTION_METHODS:
        return None
    parts = name.split(".")
    if any(part in db_names for part in parts) or "DB" in parts:
        return operation
    return None


class _CallVisitor(ast.NodeVisitor):
    def __init__(self, rel: str, db_names: set[str]) -> None:
        self.rel = rel
        self.db_names = db_names
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
        operation = _db_operation(node, self.db_names)
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
    visitor = _CallVisitor(rel, _db_names(tree))
    visitor.visit(tree)
    return visitor.calls


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
                f"line {call.line}: DB.{call.operation} in {call.identity}"
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
