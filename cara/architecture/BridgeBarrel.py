"""BridgeBarrel: generate a deployable's ``app/<kernel-pkg>`` bridges (DOCTRINE §2).

Application code reaches the kernel only as ``app.*`` (§2's single runtime
namespace). In development ``app/models``, ``app/contracts``, ``app/gates``
and ``app/shared`` are thin BRIDGE barrels over ``commons/<pkg>``. They used
to be hand-listed, one ``from commons.<pkg> import (...)`` block per wave of
work; fifty hands later the blocks overlapped, ``__all__`` had drifted out of
order by hundreds of entries and nothing local said so. A bridge is a pure
function of the kernel package it fronts, so it is GENERATED here and
``arch:barrels --check`` fails on drift, like every other barrel.

The rule, read from what the bridges actually hold:

* the exported surface is the kernel package's literal ``__all__``, minus the
  kernel-internal subpackages the manifest declares
  (``Manifest.kernel_internal_subpackages`` — §2 names ``gates/persistence``);
* one sorted ``from commons.<pkg> import (...)`` binds all of it — module
  objects (``envelopes``, the ``shared`` themes) and names alike — and
  ``__all__`` is the same sorted list;
* a package ``build:vendor-commons`` copies VERBATIM keeps its sub-paths, so
  its bridge also mirrors every kernel submodule into ``sys.modules`` under
  its ``app.<pkg>`` name (single module identity for deep imports), skipping
  the kernel-internal subpackages. A FLATTENED package
  (``Manifest.vendor_flattened_packages``: ``models``) is not mirrored: after
  the flat copy its deep paths no longer exist, so dev must not offer them;
* the module docstring is the one hand-owned part (each deployable words its
  own); everything else is regenerated. A bridge carries nothing the kernel
  does not export — local interfaces live in ``app/ports`` (§2).

A kernel package with an empty surface needs no bridge: one is generated only
when there is something behind it or the file already exists.
"""

from __future__ import annotations

import ast
from pathlib import Path

from cara.architecture.Manifest import Manifest

from .BarrelPlan import BarrelPlan

# Same width contract as ``BarrelGenerator.MAX_LINE`` (ruff fixed point).
_MAX_LINE = 88

_MIRROR = '''
def _alias_kernel(package, alias_name: str, *, skip: tuple[str, ...] = ()) -> None:
    """Mirror *package*'s submodules into ``sys.modules`` under *alias_name*.

    The SAME module objects answer to the kernel name and the ``app.*`` name,
    so a deep import (``from {app}.{pkg}.<Module> import X``) never builds a
    second module instance: exception classes, registries and module state
    stay shared.
    """
    for _info in _pkgutil.iter_modules(package.__path__):
        if _info.name in skip:
            continue
        _module = _importlib.import_module(f"{{package.__name__}}.{{_info.name}}")
        _child = f"{{alias_name}}.{{_info.name}}"
        _sys.modules.setdefault(_child, _module)
        if _info.ispkg:
            _alias_kernel(_module, _child)


_alias_kernel(_kernel, __name__{skip})'''


def _literal_all(init: Path) -> list[str] | None:
    """The kernel ``__all__`` read FRESH (never the shared parse cache: the
    kernel barrels may have been rewritten earlier in this same pass)."""
    tree = ast.parse(init.read_text(encoding="utf-8"))
    for node in tree.body:
        target = None
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
        elif isinstance(node, ast.AnnAssign):
            target = node.target
        if (
            isinstance(target, ast.Name)
            and target.id == "__all__"
            and isinstance(node.value, (ast.List, ast.Tuple))
        ):
            return [
                e.value
                for e in node.value.elts
                if isinstance(e, ast.Constant) and isinstance(e.value, str)
            ]
    return None


def _docstring(init: Path) -> str | None:
    if not init.is_file():
        return None
    source = init.read_text(encoding="utf-8")
    tree = ast.parse(source)
    first = tree.body[0] if tree.body else None
    if (
        isinstance(first, ast.Expr)
        and isinstance(first.value, ast.Constant)
        and isinstance(first.value.value, str)
    ):
        return "\n".join(source.splitlines()[first.lineno - 1 : first.end_lineno])
    return None


def _from_import(module: str, names: list[str]) -> str:
    one = f"from {module} import {', '.join(names)}"
    if len(one) <= _MAX_LINE:
        return one
    inner = "\n".join(f"    {name}," for name in names)
    return f"from {module} import (\n{inner}\n)"


def _dunder_all(names: list[str]) -> str:
    if not names:
        return "__all__: list[str] = []"
    inner = "\n".join(f'    "{name}",' for name in names)
    return f"__all__ = [\n{inner}\n]"


class BridgeBarrel:
    """Render and (re)write every ``app/<kernel-pkg>/__init__.py`` bridge."""

    @staticmethod
    def internal_children(manifest: Manifest, pkg: str) -> tuple[str, ...]:
        """The direct subpackages of ``commons/<pkg>`` app code must not reach."""
        return tuple(
            sorted(
                entry.split(".", 1)[1]
                for entry in manifest.kernel_internal_subpackages
                if entry.split(".", 1)[0] == pkg and "." in entry
            )
        )

    @staticmethod
    def render(
        manifest: Manifest, pkg: str, kernel_all: list[str], doc: str | None
    ) -> str:
        kernel_root = manifest.roots.kernel_dev_root_name
        kernel_module = f"{kernel_root}.{pkg}"
        internal = BridgeBarrel.internal_children(manifest, pkg)
        names = sorted(set(kernel_all) - set(internal))
        mirrored = pkg not in manifest.vendor_flattened_packages

        parts = [
            doc
            or f'"""{pkg.capitalize()} bridge — re-exports ``{kernel_module}`` '
            '(DOCTRINE §2)."""',
            "# Generated by ``craft arch:barrels`` from the kernel package's\n"
            "# ``__all__`` (DOCTRINE §2, §5.1): never hand-edit below the docstring.\n"
            "# ``build:vendor-commons`` replaces this file at production build.",
        ]
        imports: list[str] = []
        if mirrored:
            imports.append(
                "import importlib as _importlib\n"
                "import pkgutil as _pkgutil\n"
                "import sys as _sys\n\n"
                f"import {kernel_module} as _kernel"
            )
        if names:
            imports.append(_from_import(kernel_module, names))
        if imports:
            parts[-1] = parts[-1] + "\n" + "\n".join(imports)
        if mirrored:
            skip = ""
            if internal:
                inner = ", ".join(f'"{child}"' for child in internal)
                skip = f", skip=({inner},)"
            # ``_MIRROR`` opens with one newline: with the "\n\n" join that is
            # the two blank lines ruff puts before a top-level ``def``.
            parts.append(
                _MIRROR.format(app=manifest.roots.app_namespace, pkg=pkg, skip=skip)
            )
        parts.append(_dunder_all(names))
        return "\n\n".join(parts) + "\n"

    @staticmethod
    def regenerate(manifest: Manifest, write: bool, plan: BarrelPlan) -> None:
        """Every kernel package's bridge, in this deployable, from its kernel."""
        for pkg in sorted(manifest.kernel_packages):
            kernel_dir = manifest.roots.kernel.get(pkg)
            if kernel_dir is None or not (kernel_dir / "__init__.py").is_file():
                continue
            kernel_all = _literal_all(kernel_dir / "__init__.py")
            if kernel_all is None:
                # A computed kernel ``__all__`` cannot be bridged by reading it;
                # BarrelCompleteness reports it (fail closed, §9).
                continue
            bridge = manifest.roots.app / pkg / "__init__.py"
            if not kernel_all and not bridge.is_file():
                continue
            source = BridgeBarrel.render(manifest, pkg, kernel_all, _docstring(bridge))
            if bridge.is_file() and bridge.read_text(encoding="utf-8") == source:
                continue
            deployable = manifest.roots.deployable
            plan.changed.append(
                str(bridge.relative_to(deployable))
                if bridge.is_relative_to(deployable)
                else str(bridge)
            )
            if write:
                bridge.parent.mkdir(parents=True, exist_ok=True)
                bridge.write_text(source, encoding="utf-8")
