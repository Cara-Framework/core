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

Inside a MODEL's own file the class is never an imported name — the chain is
spelled ``cls.where(...)`` or ``self.__class__.where(...).update(...)``. That
is the identical class-level reach, so both are read as model receivers when
the scanned file itself sits under a model home (``model_import_roots``
rendered as path fragments). Requiring an imported name everywhere made every
model-resident repository use-case invisible: a find-or-create with its own
``DB.transaction()`` could live on a model and the census stayed empty. A bare
``self`` is NOT a receiver — ``self.update(...)`` on a loaded row is an
intrinsic state transition, which is exactly what §5 leaves to the model.

Two carve-outs, each narrow and each earned:

* a single-argument primary-key lookup (``Model.find(pk)``) — the documented
  central-model read, with no filter to relocate;
* an ORM call inside the row-locking statement of a ``DB.transaction()`` —
  an owner fence must stay beside the transaction whose atomicity it
  protects, or it is not a fence.

Scope is ``roots.scan_dirs("model_query_discipline")``. Declared repository
homes are excluded through ``manifest.raw_sql_homes``: ORM and raw SQL share
one persistence-boundary vocabulary, including a nested kernel repository
inside a broader scanned gate tree.

A whole file the product has not yet moved behind a repository is pinned in
``seam_allowlists[_ALLOWLIST_KEY]`` as ``path -> call count`` — exact and
shrink-only (:mod:`cara.architecture._ratchet`). This replaces the shape a
product guard reached for first: silently skipping a subtree inside the
iterator, which left the exemption invisible, unbounded and unable to expire.
"""

from __future__ import annotations

import ast

from cara.architecture._ast_utils import _path_has_fragment, iter_modules
from cara.architecture._ratchet import _ratchet
from cara.architecture.Finding import Finding
from cara.architecture.Manifest import Manifest

#: ``seam_allowlists`` key holding ``path -> inline ORM call count``.
_ALLOWLIST_KEY = "model_query_discipline"

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


def _chain_root(
    expression: ast.AST,
    models: set[str],
    *,
    model_home: bool = False,
) -> str | None:
    """The model a builder chain started from, if it started at one."""
    current = expression
    through_dunder_class = False
    while isinstance(current, ast.Call) and isinstance(current.func, ast.Attribute):
        current = current.func.value
    while isinstance(current, ast.Attribute):
        through_dunder_class = through_dunder_class or current.attr == "__class__"
        current = current.value
    if not isinstance(current, ast.Name):
        return None
    if current.id in models:
        return current.id
    if not model_home:
        return None
    if current.id == "cls":
        return "cls"
    return "self.__class__" if current.id == "self" and through_dunder_class else None


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
        pinned = manifest.seam_allowlists.get(_ALLOWLIST_KEY, {})
        findings: list[Finding] = []
        counts: dict[str, int] = {}
        # ``app.models`` / ``commons.models`` as path fragments: a file that
        # lives there defines models, so ``cls`` is one.
        model_homes = tuple(
            root.replace(".", "/") for root in manifest.model_import_roots
        )
        for _path, rel, tree in iter_modules(
            manifest.roots.scan_dirs("model_query_discipline"),
            manifest.roots.deployable,
        ):
            if _path_has_fragment(rel, manifest.raw_sql_homes):
                continue
            model_home = _path_has_fragment(rel, model_homes)
            models = _imported_model_names(tree, manifest.model_import_roots)
            if not models and not model_home:
                continue
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
                model = _chain_root(node.func.value, models, model_home=model_home)
                if model is None or _is_primary_key_lookup(node):
                    continue
                if _locking_transaction_owns(node, parents):
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
        return findings + _ratchet(
            key=_ALLOWLIST_KEY,
            current=counts,
            pinned=pinned,
            message="inline model query outside a repository",
        )
