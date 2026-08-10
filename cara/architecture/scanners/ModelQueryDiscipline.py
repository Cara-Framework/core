"""ModelQueryDiscipline: only a repository queries a model (DOCTRINE §5).

A controller, service, job or resource that calls ``Model.where(...)`` has
made the ORM its interface. The query is then untestable without a database,
unreusable from the sibling layer, and invisible to whoever later has to
reason about which code paths touch a table.

The receiver must be a name imported from one of
``manifest.model_import_roots``, so ``dict.get()``, ``response.first()`` and a
passed-in instance's ``product.save()`` never match — only a class-level
builder call does. The whole CHAIN counts: ``Model.without_scope().first()``
is the same reach as ``Model.first()``, and a scanner that only looked at the
head of the chain would miss it.

Three carve-outs, each narrow and each earned:

* a single-argument primary-key lookup (``Model.find(pk)``) — the documented
  central-model read, with no filter to relocate;
* an ORM call inside the row-locking statement of a ``DB.transaction()`` —
  an owner fence must stay beside the transaction whose atomicity it
  protects, or it is not a fence;
* a ``manifest.inline_orm_allow_tag`` comment (``# allow-inline-orm: why``)
  on or just above the call — the documented, shrink-only local opt-out.

Scope is ``roots.scan_dirs("model_query_discipline")``: a product declares
which layers must go through a repository. Repository trees are excluded by
not being declared, never by a special case.

A whole file the product has not yet moved behind a repository is pinned in
``seam_allowlists[ALLOWLIST_KEY]`` as ``path -> call count`` — exact and
shrink-only (:mod:`cara.architecture._ratchet`). This replaces the shape a
product guard reached for first: silently skipping a subtree inside the
iterator, which left the exemption invisible, unbounded and unable to expire.
"""

from __future__ import annotations

import ast
import re

from cara.architecture._ast_utils import iter_modules, read_source
from cara.architecture._ratchet import ratchet
from cara.architecture.Finding import Finding
from cara.architecture.Manifest import Manifest

#: ``seam_allowlists`` key holding ``path -> inline ORM call count``.
ALLOWLIST_KEY = "model_query_discipline"

#: Builder/mutation methods that mean "this is a query", not plain attribute use.
ORM_METHODS = frozenset(
    {
        "where",
        "where_in",
        "where_not_in",
        "where_null",
        "where_not_null",
        "where_raw",
        "or_where",
        "where_has",
        "get",
        "first",
        "first_or_fail",
        "find",
        "find_or_fail",
        "all",
        "create",
        "save",
        "update",
        "delete",
        "paginate",
        "simple_paginate",
        "count",
        "sum",
        "avg",
        "max",
        "min",
        "with_",
        "order_by",
        "limit",
        "offset",
        "pluck",
        "exists",
        "first_or_create",
        "update_or_create",
        "insert",
        "chunk",
    }
)
TRANSACTION_METHOD = "transaction"
ROW_LOCK_METHOD = "lock_for_update"
#: How far above a call the allow tag may sit (the call's own line, plus the
#: two above it, so a wrapped call keeps the tag readable).
TAG_LOOKBEHIND = 3


def _imported_model_names(tree: ast.Module, roots: tuple[str, ...]) -> set[str]:
    names: set[str] = set()
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.ImportFrom)
            and node.module
            and any(node.module == r or node.module.startswith(r + ".") for r in roots)
        ):
            names.update(alias.asname or alias.name for alias in node.names)
    return names


def _chain_root(expression: ast.AST, models: set[str]) -> str | None:
    """The imported model a builder chain started from, if it started at one."""
    current = expression
    while isinstance(current, ast.Call) and isinstance(current.func, ast.Attribute):
        current = current.func.value
    while isinstance(current, ast.Attribute):
        current = current.value
    if isinstance(current, ast.Name) and current.id in models:
        return current.id
    return None


def _is_transaction(node: ast.AST) -> bool:
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == TRANSACTION_METHOD
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "DB"
    )


def _locking_transaction_owns(node: ast.Call, parents: dict[ast.AST, ast.AST]) -> bool:
    statement: ast.stmt | None = None
    transaction: ast.With | None = None
    current: ast.AST | None = node
    while current is not None:
        if statement is None and isinstance(current, ast.stmt):
            statement = current
        if isinstance(current, ast.With) and any(
            _is_transaction(item.context_expr) for item in current.items
        ):
            transaction = current
            break
        current = parents.get(current)
    if transaction is None or statement is None:
        return False
    return any(
        isinstance(candidate, ast.Call)
        and isinstance(candidate.func, ast.Attribute)
        and candidate.func.attr == ROW_LOCK_METHOD
        for candidate in ast.walk(statement)
    )


def _is_primary_key_lookup(node: ast.Call) -> bool:
    return (
        isinstance(node.func, ast.Attribute)
        and node.func.attr == "find"
        and len(node.args) == 1
        and not node.keywords
    )


class ModelQueryDiscipline:
    """Model queries live in repositories, not in the layers above them."""

    @staticmethod
    def scan(manifest: Manifest) -> list[Finding]:
        allow = re.compile(rf"#\s*{re.escape(manifest.inline_orm_allow_tag)}:\s*(\S.*)$")
        pinned = manifest.seam_allowlists.get(ALLOWLIST_KEY, {})
        findings: list[Finding] = []
        counts: dict[str, int] = {}
        for path, rel, tree in iter_modules(
            manifest.roots.scan_dirs("model_query_discipline"),
            manifest.roots.deployable,
        ):
            models = _imported_model_names(tree, manifest.model_import_roots)
            if not models:
                continue
            # ``iter_modules`` already proved the file parses, so this cannot
            # normally fail; going through ``read_source`` keeps a
            # mid-change tree (a file deleted between glob and read) a
            # skipped file rather than a crashed pack.
            lines = (read_source(path) or "").splitlines()
            parents = {
                child: parent
                for parent in ast.walk(tree)
                for child in ast.iter_child_nodes(parent)
            }
            hits: list[Finding] = []
            for node in ast.walk(tree):
                if (
                    not isinstance(node, ast.Call)
                    or not isinstance(node.func, ast.Attribute)
                    or node.func.attr not in ORM_METHODS
                ):
                    continue
                model = _chain_root(node.func.value, models)
                if model is None or _is_primary_key_lookup(node):
                    continue
                if _locking_transaction_owns(node, parents):
                    continue
                start = max(0, node.lineno - TAG_LOOKBEHIND)
                end = min(len(lines), node.end_lineno or node.lineno)
                if any(allow.search(line) for line in lines[start:end]):
                    continue
                hits.append(
                    Finding(
                        rel,
                        node.lineno,
                        f"{model}.{node.func.attr}(...) queries a model outside a "
                        f"repository — move the query into one",
                    )
                )
            if not hits:
                continue
            if rel in pinned:
                counts[rel] = len(hits)
            else:
                findings.extend(hits)
        return findings + ratchet(
            key=ALLOWLIST_KEY,
            current=counts,
            pinned=pinned,
            message="inline model query outside a repository",
        )
