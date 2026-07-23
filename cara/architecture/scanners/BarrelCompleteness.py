"""BarrelCompleteness: every barrel is a superset with an alphabetical
``__all__`` (DOCTRINE §5.1).

"Barrels are generated and verified, never hand-curated. Every public name
is re-exported (domain ``__init__`` AND layer barrel), ``__all__``
alphabetical, completeness guard-enforced." This scanner walks every
barrel-managed package — each of the manifest's domain-partitioned layers
(``app/<layer>/`` and its domain subpackages) plus each dev-only kernel
package (``roots.kernel`` — ``commons/models`` and its subpackages) — and
checks, for every package holding an ``__init__.py``:

* every direct child module's public surface (``__all__`` if declared,
  else top-level classes/functions/UPPER constants) is re-exported by the
  package's own ``__all__`` (the superset rule), and likewise every direct
  child SUBPACKAGE's public surface;
* ``__all__`` is sorted (plain ASCII string order — ``sorted()``);
* a submodule bound as a MODULE OBJECT (``from . import X`` in this same
  ``__init__``) is exempt from the name-level superset check — its symbols
  are deliberately kept module-qualified (the module-object contract).

A package with child modules/subpackages but no ``__all__`` at all is
itself a Finding (nothing generated it, or generation was hand-reverted).
"""

from __future__ import annotations

import ast
from pathlib import Path

from cara.architecture._ast_utils import (
    dunder_all,
    module_object_names,
    parse,
    public_names,
    relpath,
)
from cara.architecture.Finding import Finding
from cara.architecture.Manifest import Manifest


def _child_dirs_with_init(pkg_dir: Path) -> list[Path]:
    return sorted(
        p
        for p in pkg_dir.iterdir()
        if p.is_dir() and p.name != "__pycache__" and (p / "__init__.py").is_file()
    )


def _walk_barrel_dirs(base: Path):
    if not base.is_dir():
        return
    yield base
    for child in _child_dirs_with_init(base):
        yield from _walk_barrel_dirs(child)


def _expected_exports(pkg_dir: Path, module_objects: set[str]) -> set[str]:
    expected: set[str] = set()
    for py in sorted(pkg_dir.glob("*.py")):
        if py.stem in ("__init__", *module_objects):
            continue
        expected.update(public_names(py))
    for sub in _child_dirs_with_init(pkg_dir):
        if sub.name in module_objects:
            continue
        expected.update(public_names(sub / "__init__.py"))
    return expected


def _bound_names(tree: ast.Module) -> set[str]:
    bound: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.ImportFrom):
            bound.update(alias.asname or alias.name for alias in node.names)
        elif isinstance(node, ast.Import):
            bound.update(
                (alias.asname or alias.name).split(".")[0] for alias in node.names
            )
        elif isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            bound.add(node.name)
        elif isinstance(node, ast.Assign):
            bound.update(
                target.id for target in node.targets if isinstance(target, ast.Name)
            )
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            bound.add(node.target.id)
    return bound


def _check_package(pkg_dir: Path, manifest: Manifest) -> list[Finding]:
    init = pkg_dir / "__init__.py"
    tree = parse(init)
    if tree is None:
        return []
    rel = relpath(init, manifest.roots.deployable)
    module_objects = module_object_names(pkg_dir)
    expected = _expected_exports(pkg_dir, module_objects)
    declared = dunder_all(tree)

    if declared is None:
        if expected:
            return [
                Finding(
                    rel,
                    0,
                    f"no __all__ declared but {len(expected)} public name(s) "
                    f"are exported by direct submodules — regenerate the barrel",
                )
            ]
        return []

    findings: list[Finding] = []
    missing = sorted(expected - set(declared))
    if missing:
        findings.append(
            Finding(
                rel,
                0,
                f"__all__ is missing re-export(s): {', '.join(missing)}",
            )
        )
    if declared != sorted(declared):
        findings.append(Finding(rel, 0, "__all__ is not alphabetically sorted"))
    unbound = sorted(set(declared) - _bound_names(tree))
    if unbound:
        findings.append(
            Finding(
                rel,
                0,
                "__all__ contains name(s) never bound by the barrel: "
                + ", ".join(unbound),
            )
        )
    return findings


class BarrelCompleteness:
    """Every barrel-managed package's ``__all__`` is a sorted superset."""

    @staticmethod
    def scan(manifest: Manifest) -> list[Finding]:
        findings: list[Finding] = []
        roots: list[Path] = [manifest.roots.app / layer for layer in manifest.layers]
        roots.extend(
            pkg_dir
            for pkg, pkg_dir in manifest.roots.kernel.items()
            if pkg in manifest.kernel_barrel_packages
        )
        for root in roots:
            for pkg_dir in _walk_barrel_dirs(root):
                findings.extend(_check_package(pkg_dir, manifest))
        return findings
