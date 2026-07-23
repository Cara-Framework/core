"""Shared pure-AST helpers for the Guard Pack scanners.

Leading underscore: an internal module, not part of the package's public
surface (mirrors ``cara/commands/_optional.py``). Every function here is
stdlib-only, side-effect free, and safe to call without booting any
application — the boot-free contract every scanner and craft command in
``cara/architecture/`` must uphold.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

STDLIB: frozenset[str] = frozenset(sys.stdlib_module_names) | {"__future__"}

_UPPER_RE_CHARS = frozenset("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")


def is_upper_const(name: str) -> bool:
    """True for an UPPER_SNAKE identifier (a constant, never a class/def)."""
    return (
        bool(name)
        and not name.startswith("_")
        and name == name.upper()
        and any(c.isalpha() for c in name)
        and set(name) <= _UPPER_RE_CHARS
    )


def python_files(base: Path) -> list[Path]:
    """Every ``*.py`` under ``base``, sorted, ``__pycache__`` excluded."""
    if not base.is_dir():
        return []
    return sorted(p for p in base.rglob("*.py") if "__pycache__" not in p.parts)


def relpath(path: Path, root: Path) -> str:
    """POSIX-style path relative to ``root`` (falls back to the name)."""
    # Preserve the logical path through a deployable's symlinked dev kernel.
    # Resolving first turns ``api/commons/models/Foo.py`` into a sibling path
    # outside ``api/`` and collapses every finding to the basename.
    try:
        return path.absolute().relative_to(root.absolute()).as_posix()
    except ValueError:
        pass
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return path.name


def parse(path: Path) -> ast.Module | None:
    """Parse a file; ``None`` on a syntax error (a scanner's own concern —
    py_compile / the test suite catches genuine syntax breakage)."""
    try:
        return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except SyntaxError, UnicodeDecodeError:
        return None


def is_type_checking_if(node: ast.stmt) -> bool:
    if not isinstance(node, ast.If):
        return False
    test = node.test
    return (isinstance(test, ast.Name) and test.id == "TYPE_CHECKING") or (
        isinstance(test, ast.Attribute) and test.attr == "TYPE_CHECKING"
    )


def module_level_imports(tree: ast.Module) -> list[ast.stmt]:
    """Runtime module-level ``Import``/``ImportFrom`` nodes.

    Recurses into module-level ``if``/``try``/``with`` bodies (they execute
    at import time); skips ``if TYPE_CHECKING:`` bodies (they never
    execute); never descends into a function or class BODY beyond a
    class's own top level (class bodies execute at class-definition time,
    i.e. still at import time, so those recurse too).
    """
    out: list[ast.stmt] = []

    def visit(body: list[ast.stmt]) -> None:
        for node in body:
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                out.append(node)
                continue
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if isinstance(node, ast.ClassDef):
                visit(node.body)
                continue
            if is_type_checking_if(node):
                visit(node.orelse or [])
                continue
            for attr in ("body", "orelse", "finalbody"):
                sub = getattr(node, attr, None)
                if sub:
                    visit(sub)
            for handler in getattr(node, "handlers", []) or []:
                visit(handler.body)
            for case in getattr(node, "cases", []) or []:
                visit(case.body)

    visit(tree.body)
    return out


def function_local_imports(tree: ast.Module) -> list[ast.stmt]:
    """``Import``/``ImportFrom`` nodes whose enclosing scope is a function."""
    found: list[ast.stmt] = []

    def visit(body: list[ast.stmt], in_function: bool) -> None:
        for node in body:
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                if in_function:
                    found.append(node)
                continue
            entering = in_function or isinstance(
                node, (ast.FunctionDef, ast.AsyncFunctionDef)
            )
            for attr in ("body", "orelse", "finalbody"):
                visit(getattr(node, attr, []) or [], entering)
            for handler in getattr(node, "handlers", []) or []:
                visit(handler.body, entering)
            for case in getattr(node, "cases", []) or []:
                visit(case.body, entering)

    visit(tree.body, False)
    return found


def docstring_node_ids(tree: ast.Module) -> set[int]:
    """``id()`` of every module/class/function docstring Constant node."""
    ids: set[int] = set()
    for node in ast.walk(tree):
        if (
            not isinstance(
                node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)
            )
            or not node.body
        ):
            continue
        first = node.body[0]
        if (
            isinstance(first, ast.Expr)
            and isinstance(first.value, ast.Constant)
            and isinstance(first.value.value, str)
        ):
            ids.add(id(first.value))
    return ids


def dunder_all(tree: ast.Module) -> list[str] | None:
    """The literal ``__all__`` list/tuple of string constants, if declared."""
    for node in tree.body:
        target = None
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
        elif isinstance(node, ast.AnnAssign):
            target = node.target
        if (
            target is not None
            and isinstance(target, ast.Name)
            and target.id == "__all__"
            and isinstance(node.value, (ast.List, ast.Tuple))
        ):
            return [e.value for e in node.value.elts if isinstance(e, ast.Constant)]
    return None


def public_names(path: Path) -> list[str]:
    """Public surface of a module: ``__all__`` if declared, else top-level
    classes/functions and UPPER_SNAKE constants, ``_``-prefixed excluded."""
    tree = parse(path)
    if tree is None:
        return []
    declared = dunder_all(tree)
    if declared is not None:
        return sorted(set(declared))
    names: set[str] = set()
    for node in tree.body:
        if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            if not node.name.startswith("_"):
                names.add(node.name)
        elif isinstance(node, ast.Assign):
            for t in node.targets:
                if isinstance(t, ast.Name) and is_upper_const(t.id):
                    names.add(t.id)
        elif (
            isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
            and is_upper_const(node.target.id)
            and node.value is not None
        ):
            names.add(node.target.id)
    return sorted(names)


def module_object_names(pkg: Path) -> set[str]:
    """Submodule names this package's ``__init__`` binds as MODULE OBJECTS
    (``from . import X`` with no asname, X a real submodule) — the
    module-object contract exemption (§5.1): X's own symbols stay
    module-qualified and are exempt from barrel-superset/deep-import
    checks."""
    init = pkg / "__init__.py"
    if not init.exists():
        return set()
    tree = parse(init)
    if tree is None:
        return set()
    out: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.ImportFrom) and node.level == 1 and not node.module:
            for alias in node.names:
                if alias.asname is None and (pkg / f"{alias.name}.py").exists():
                    out.add(alias.name)
    return out
