"""The name a reader would say for an expression node — for source guards.

Architecture guards read source rather than importing it, so each needs the
same reduction from an AST node to a name. Two readings cover them:

* :func:`terminal_name` — the trailing identifier: ``Registry.bind`` and
  ``bind`` both say ``bind``. Use it to match a decorator, a base class or a
  method regardless of how it was reached.
* :func:`dotted_name` — the whole attribute chain: ``DB.table`` stays
  ``DB.table``. A chain rooted in something that is not a name (a call, a
  subscript) keeps only the attributes after it, so ``Model.where(...).update``
  reads ``update`` and ``foo().bar.baz`` reads ``bar.baz``.
"""

from __future__ import annotations

import ast


def terminal_name(node: ast.AST) -> str | None:
    """The trailing identifier of a ``Name``/``Attribute`` node, else None."""
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def dotted_name(node: ast.AST) -> str:
    """The dotted attribute chain of ``node``; empty when it has none."""
    parts: list[str] = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
    return ".".join(reversed(parts))
