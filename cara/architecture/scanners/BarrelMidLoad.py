"""BarrelMidLoad: the import that binds a half-built module (DOCTRINE §5.1).

A module inside package ``P`` that writes ``from P import X`` — where
``P/__init__.py`` re-exports ``X`` as a class via ``from .X import X``, so the
exported name COLLIDES with a submodule filename — is order-fragile. While
``P``'s ``__init__`` is still executing, that import resolves to the
half-built SUBMODULE object rather than the class, and the failure surfaces
far away, at call time::

    TypeError: 'module' object is not callable

Type checkers cannot see it: they resolve the final re-exported symbol, never
the partial-init state. The fix is always mechanical, and always the same::

    from P import X          ->   from P.X import X

The same fragility applies to an ANCESTOR barrel: a file in ``P.sub`` doing
``from P import X`` is loaded BY ``P``'s init sweep, so ``P`` is mid-load by
construction. One exemption, and it is structural rather than declared: a
name the ancestor's ``__init__`` binds BEFORE it imports any other submodule
of that package can never be observed half-built, so a deliberately
first-pinned re-export stays legal.

Two shapes are deliberately NOT findings. ``from . import X`` (no asname)
binds a module ON PURPOSE — the module-object contract — and a name that does
not collide with a submodule filename was never ambiguous to begin with.

Prior state: this scanner's logic lived in four hand-maintained product copies
of ``test_import_wiring.py``, one of whose headers admitted it was ported from
another product's reference copy. The bug it prevents has shipped to production
at least twice.
"""

from __future__ import annotations

import ast
from pathlib import Path

from cara.architecture._ast_utils import parse, python_files, relpath
from cara.architecture.Finding import Finding
from cara.architecture.Manifest import Manifest


def _class_reexports(package: Path) -> set[str]:
    """Names the barrel binds via ``from .X import X`` — name == submodule.

    Only a COLLIDING re-export is order-fragile; a namespace module exported
    with ``from . import X`` is module-valued by design and stays out.
    """
    tree = parse(package / "__init__.py") if (package / "__init__.py").is_file() else None
    if tree is None:
        return set()
    names: set[str] = set()
    for node in tree.body:
        if not isinstance(node, ast.ImportFrom) or node.level != 1 or node.module is None:
            continue
        for alias in node.names:
            exported = alias.asname or alias.name
            if node.module == exported and alias.name == exported:
                names.add(exported)
    return names


def _pinned_first_reexports(package: Path, dotted: str) -> set[str]:
    """Colliding re-exports bound BEFORE the barrel loads any other submodule.

    The window closes at the first import that can re-enter this package: a
    relative import of a different submodule, or an absolute import under the
    package's own dotted prefix. Foreign absolute imports (stdlib, framework)
    cannot load this package's submodules, so they leave the window open.
    """
    tree = parse(package / "__init__.py") if (package / "__init__.py").is_file() else None
    if tree is None:
        return set()
    pinned: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.ImportFrom):
            if node.module == "__future__":
                continue
            if node.level >= 1:
                if (
                    node.level == 1
                    and node.module
                    and "." not in node.module
                    and any(alias.name == node.module for alias in node.names)
                ):
                    pinned.add(node.module)
                    continue
                break
            if node.module and (
                node.module == dotted or node.module.startswith(dotted + ".")
            ):
                break
        elif isinstance(node, ast.Import):
            if any(
                alias.name == dotted or alias.name.startswith(dotted + ".")
                for alias in node.names
            ):
                break
        # Non-import statements (docstring, __all__, constants) are neutral.
    return pinned


def _absolute_module(node: ast.ImportFrom, own_package: str) -> str | None:
    """The dotted absolute module an ``ImportFrom`` targets."""
    if node.level == 0:
        return node.module
    parts = own_package.split(".")
    if node.level - 1 > len(parts):
        return None
    base = parts[: len(parts) - (node.level - 1)] if node.level > 1 else parts
    if node.module:
        base = [*base, node.module]
    return ".".join(base) if base else None


class BarrelMidLoad:
    """No import can bind a barrel's half-built submodule (§5.1)."""

    @staticmethod
    def scan(manifest: Manifest) -> list[Finding]:
        deployable = manifest.roots.deployable
        reexports: dict[str, set[str]] = {}
        pinned: dict[str, set[str]] = {}

        def colliding(package: str) -> set[str]:
            if package not in reexports:
                reexports[package] = _class_reexports(
                    deployable / package.replace(".", "/")
                )
            return reexports[package]

        def first_pinned(package: str) -> set[str]:
            if package not in pinned:
                pinned[package] = _pinned_first_reexports(
                    deployable / package.replace(".", "/"), package
                )
            return pinned[package]

        findings: list[Finding] = []
        for root in manifest.roots.scan_dirs("barrel_mid_load"):
            for path in python_files(root):
                if path.name == "__init__.py" or "tests" in path.parts:
                    continue
                rel = relpath(path, deployable)
                tree = parse(path)
                if tree is None:
                    continue
                own_package = ".".join(Path(rel).with_suffix("").parts[:-1])
                for node in ast.walk(tree):
                    if not isinstance(node, ast.ImportFrom):
                        continue
                    module = _absolute_module(node, own_package)
                    if module is None:
                        continue
                    if module == own_package:
                        unsafe = colliding(module)
                    elif own_package.startswith(module + "."):
                        unsafe = colliding(module) - first_pinned(module)
                    else:
                        continue
                    findings.extend(
                        Finding(
                            rel,
                            node.lineno,
                            f"`from {module} import {alias.name}` binds a half-built "
                            f"module while the barrel loads — write "
                            f"`from {module}.{alias.name} import {alias.name}`",
                        )
                        for alias in node.names
                        if alias.name in unsafe
                    )
        return findings
