"""BarrelGenerator: idempotent AST barrel generation (DOCTRINE §5.1).

"Barrels are generated and verified, never hand-curated." This is the
generator: for every domain-partitioned layer (``manifest.layers``) and
every dev-only kernel package (``manifest.roots.kernel``, recursing into
nested subpackages — e.g. ``commons/models/core/``), it regenerates each
package's ``__init__.py`` as the alphabetical, superset re-export of its
direct children's public names.

A second run over unchanged sources is a no-op (idempotence) because the
generator PRESERVES everything a hand-author may have deliberately added
to an existing barrel:

* the module docstring, verbatim;
* ``__future__`` imports, kept first, never exported;
* non-import top-level statements (constants) right after the docstring;
* a MODULE-OBJECT bind (``from . import X``) verbatim — ``X``'s own
  symbols stay module-qualified and are exempt from name-level generation
  (the module-object contract);
* an aliased relative import (``from .X import Y as Z``) verbatim;
* an underscore-prefixed relative re-export, if the existing ``__all__``
  already listed it;
* any statement placed AFTER the existing ``__all__`` — a deliberate late
  bind (e.g. a documented cycle-breaker that must import after the
  package surface is built);
* a foreign (absolute, non-self) import, verbatim.

Everything else is regenerated: the alphabetical superset of every direct
child module's/subpackage's public surface. ``check()`` reports drift
without writing; ``write()`` regenerates in place.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass, field
from pathlib import Path

from cara.architecture._ast_utils import dunder_all, is_upper_const, parse, public_names
from cara.architecture.Manifest import Manifest

MAX_LINE = 88


def _declared_all(path: Path) -> list[str]:
    """Only the EXPLICIT ``__all__`` list (never the derived-names
    fallback ``public_names`` uses) — needed to tell "not yet generated"
    apart from "generated with an empty surface"."""
    if not path.exists():
        return []
    tree = parse(path)
    if tree is None:
        return []
    declared = dunder_all(tree)
    return list(declared) if declared is not None else []


def _fmt_import(relmod: str, names: list[str]) -> str:
    names = sorted(names)
    one = f"from {relmod} import {', '.join(names)}"
    if len(one) <= MAX_LINE:
        return one
    inner = "\n".join(f"    {n}," for n in names)
    return f"from {relmod} import (\n{inner}\n)"


def _fmt_all(names: list[str]) -> str:
    if not names:
        return "__all__: list[str] = []"
    inner = "\n".join(f'    "{n}",' for n in sorted(set(names)))
    return f"__all__ = [\n{inner}\n]"


def _child_dirs(pkg_dir: Path) -> list[Path]:
    return sorted(
        p
        for p in pkg_dir.iterdir()
        if p.is_dir() and p.name != "__pycache__" and any(p.glob("*.py"))
    )


def _is_module_object(pkg_dir: Path, name: str) -> bool:
    """True only for a genuine module-object binding: the target is a real
    submodule/subpackage AND does not itself export a same-named symbol
    (class-per-file re-exports resolve to the CLASS, not the module)."""
    leaf = pkg_dir / f"{name}.py"
    if leaf.exists():
        return name not in public_names(leaf)
    sub_init = pkg_dir / name / "__init__.py"
    if sub_init.exists():
        return name not in _declared_all(sub_init)
    return False


class _Preserved:
    """What an existing ``__init__.py`` contributes to the regenerated file."""

    def __init__(self, init: Path, self_dotted: str | None):
        self.future_stmts: list[str] = []
        self.const_stmts: list[str] = []
        self.doc: str | None = None
        self.post_stmts: list[str] = []
        self.stmts: list[str] = []
        self.names: list[str] = []
        self.module_objects: set[str] = set()
        self.existing_all: set[str] = set()
        self.rel_candidates: list[tuple[str, str]] = []
        if not init.exists():
            return
        source = init.read_text(encoding="utf-8")
        if not source.strip():
            return
        tree = ast.parse(source)
        lines = source.splitlines()
        pkg = init.parent
        body = tree.body
        if (
            body
            and isinstance(body[0], ast.Expr)
            and isinstance(body[0].value, ast.Constant)
            and isinstance(body[0].value.value, str)
        ):
            self.doc = "\n".join(lines[body[0].lineno - 1 : body[0].end_lineno])

        def is_all_stmt(node: ast.stmt) -> bool:
            if isinstance(node, ast.Assign):
                return any(
                    isinstance(t, ast.Name) and t.id == "__all__" for t in node.targets
                )
            if isinstance(node, ast.AnnAssign):
                return isinstance(node.target, ast.Name) and node.target.id == "__all__"
            return False

        all_lineno: int | None = None
        for node in body:
            if is_all_stmt(node):
                all_lineno = node.lineno
                if node.value is not None and isinstance(
                    node.value, (ast.List, ast.Tuple)
                ):
                    self.existing_all = {
                        e.value for e in node.value.elts if isinstance(e, ast.Constant)
                    }

        def with_leading_comments(node: ast.stmt) -> str:
            start = node.lineno - 1
            i = start - 1
            while i >= 0 and lines[i].strip().startswith("#"):
                i -= 1
            return "\n".join(lines[i + 1 : node.end_lineno])

        for index, node in enumerate(body):
            if (
                index == 0
                and isinstance(node, ast.Expr)
                and isinstance(node.value, ast.Constant)
                and isinstance(node.value.value, str)
            ):
                continue
            if is_all_stmt(node):
                continue
            if (
                all_lineno is not None
                and node.lineno > all_lineno
                and isinstance(node, (ast.Import, ast.ImportFrom))
            ):
                self.post_stmts.append(with_leading_comments(node))
                for alias in node.names:
                    bound = (
                        (alias.asname or alias.name).split(".")[0]
                        if isinstance(node, ast.Import)
                        else (alias.asname or alias.name)
                    )
                    if not bound.startswith("_"):
                        self.names.append(bound)
                continue
            if isinstance(node, ast.ImportFrom) and node.level == 0:
                if node.module == "__future__":
                    self.future_stmts.append(
                        "\n".join(lines[node.lineno - 1 : node.end_lineno])
                    )
                    continue
                if (
                    self_dotted
                    and node.module
                    and (
                        node.module == self_dotted
                        or node.module.startswith(self_dotted + ".")
                    )
                ):
                    relmod = "." + node.module[len(self_dotted) :].lstrip(".")
                    for alias in node.names:
                        if alias.asname and alias.asname != alias.name:
                            self.stmts.append(
                                f"from {relmod} import {alias.name} as {alias.asname}"
                            )
                            self.names.append(alias.asname)
                        elif alias.name.startswith("_"):
                            self.stmts.append(f"from {relmod} import {alias.name}")
                            if alias.name in self.existing_all:
                                self.names.append(alias.name)
                        else:
                            self.rel_candidates.append((relmod, alias.name))
                    continue
                self.stmts.append(with_leading_comments(node))
                for alias in node.names:
                    bound = alias.asname or alias.name
                    if not bound.startswith("_"):
                        self.names.append(bound)
            elif isinstance(node, ast.Import):
                self.stmts.append(with_leading_comments(node))
                for alias in node.names:
                    bound = (alias.asname or alias.name).split(".")[0]
                    if not bound.startswith("_"):
                        self.names.append(bound)
            elif isinstance(node, ast.ImportFrom) and node.level >= 1:
                base_parts = (node.module or "").split(".") if node.module else []
                mod_objs = [
                    a
                    for a in node.names
                    if a.asname is None
                    and _is_module_object(_descend(pkg, base_parts), a.name)
                ]
                aliases = [a for a in node.names if a.asname and a.asname != a.name]
                unders = [
                    a
                    for a in node.names
                    if a.asname is None and a.name.startswith("_") and a not in mod_objs
                ]
                rel = "." * node.level + (node.module or "")
                if mod_objs:
                    self.stmts.append(
                        f"from {rel} import {', '.join(a.name for a in mod_objs)}"
                    )
                    for a in mod_objs:
                        self.names.append(a.name)
                        if node.level == 1 and not node.module:
                            self.module_objects.add(a.name)
                for a in aliases:
                    self.stmts.append(f"from {rel} import {a.name} as {a.asname}")
                    self.names.append(a.asname)
                for a in unders:
                    self.stmts.append(f"from {rel} import {a.name}")
                    if a.name in self.existing_all:
                        self.names.append(a.name)
                for a in node.names:
                    if (
                        a.asname is None
                        and not a.name.startswith("_")
                        and a not in mod_objs
                    ):
                        self.rel_candidates.append((rel, a.name))
            else:
                self.const_stmts.append(with_leading_comments(node))
                if isinstance(node, ast.Assign):
                    for t in node.targets:
                        if isinstance(t, ast.Name) and is_upper_const(t.id):
                            self.names.append(t.id)
                elif (
                    isinstance(node, ast.AnnAssign)
                    and isinstance(node.target, ast.Name)
                    and is_upper_const(node.target.id)
                ):
                    self.names.append(node.target.id)


def _descend(base: Path, parts: list[str]) -> Path:
    for part in parts:
        base = base / part
    return base


def _pin_block(pin_name: str) -> str:
    return (
        f"# {pin_name} binds FIRST (package-namespace pin): a reader that hits\n"
        f"# this barrel mid-init must see the CLASS, never the half-built\n"
        f"# submodule. `# isort: skip` stops a sorter from re-burying it.\n"
        f"from .{pin_name} import {pin_name}  # isort: skip"
    )


def _compose_init(
    preserved: _Preserved,
    exports: dict[str, list[str]],
    default_doc: str,
    pin_name: str | None,
) -> tuple[str, list[str], list[str]]:
    """Returns ``(source, all_names, dropped_shadowed)``. ``exports`` maps a
    relative-import module string (``".ChildModule"``) to the names it
    contributes (already filtered for the module-object exemption);
    ``pin_name`` — when set — is emitted as a verbatim import block BEFORE
    every other import (a package-namespace pin, e.g. a job base class that
    must bind before any sibling can subclass it mid-init) and always joins
    ``__all__``, even though ``exports`` never carries it."""
    preserved_names = set(preserved.names)
    dropped: list[str] = []
    gen_imports: list[str] = []
    exported: list[str] = []
    for relmod in sorted(exports):
        dropped.extend(f"{relmod}:{n}" for n in exports[relmod] if n in preserved_names)
        names = [n for n in exports[relmod] if n not in preserved_names]
        if not names:
            continue
        gen_imports.append(_fmt_import(relmod, names))
        exported.extend(names)

    parts = [preserved.doc if preserved.doc else f'"""{default_doc}"""']
    if preserved.future_stmts:
        parts.append("\n".join(preserved.future_stmts))
    if preserved.const_stmts:
        parts.append("\n\n".join(preserved.const_stmts))
    if pin_name:
        parts.append(_pin_block(pin_name))
    if preserved.stmts:
        parts.append("\n".join(preserved.stmts))
    if gen_imports:
        parts.append("\n".join(gen_imports))

    all_names = set(exported) | preserved_names | ({pin_name} if pin_name else set())
    residual: dict[str, list[str]] = {}
    for relmod, name in preserved.rel_candidates:
        if (
            name not in all_names
            and relmod.lstrip(".").split(".")[0] not in preserved.module_objects
        ):
            residual.setdefault(relmod, []).append(name)
            all_names.add(name)
    if residual:
        parts.append(
            "\n".join(
                _fmt_import(rm, sorted(set(ns))) for rm, ns in sorted(residual.items())
            )
        )

    all_names_sorted = sorted(all_names)
    parts.append(_fmt_all(all_names_sorted))
    if preserved.post_stmts:
        parts.append("\n".join(preserved.post_stmts))
    return "\n\n".join(parts) + "\n", all_names_sorted, dropped


def _module_exports(pkg_dir: Path) -> dict[str, list[str]]:
    """``{".ChildModule": [names]}`` for every direct ``*.py`` child plus
    ``{".subpkg": [names]}`` for every direct subpackage's DECLARED
    ``__all__`` (subpackages are regenerated depth-first before their
    parent, so their ``__all__`` is always current by the time this
    reads it)."""
    out: dict[str, list[str]] = {}
    for py in sorted(pkg_dir.glob("*.py")):
        if py.stem == "__init__":
            continue
        names = public_names(py)
        if names:
            out[f".{py.stem}"] = names
    for sub in _child_dirs(pkg_dir):
        names = _declared_all(sub / "__init__.py")
        if names:
            out[f".{sub.name}"] = sorted(names)
    return out


def _dotted_name(path: Path, root: Path, prefix: str) -> str:
    if path == root:
        return prefix
    return prefix + "." + ".".join(path.relative_to(root).parts)


@dataclass(slots=True)
class BarrelPlan:
    """Result of one generator pass."""

    changed: list[str] = field(default_factory=list)
    collisions: list[str] = field(default_factory=list)


def _regenerate_tree(
    pkg_dir: Path,
    root: Path,
    prefix: str,
    default_doc: str,
    deployable_root: Path,
    write: bool,
    plan: BarrelPlan,
    pin_stem: str | None = None,
) -> list[str]:
    """Depth-first regeneration; returns this package's resulting ``__all__``."""
    for sub in _child_dirs(pkg_dir):
        _regenerate_tree(
            sub,
            root,
            prefix,
            f"{default_doc} — {sub.name} subpackage.",
            deployable_root,
            write,
            plan,
        )

    init = pkg_dir / "__init__.py"
    dotted = _dotted_name(pkg_dir, root, prefix)
    exports = _module_exports(pkg_dir)
    preserved = _Preserved(init, dotted)
    exports = {
        relmod: names
        for relmod, names in exports.items()
        if relmod.lstrip(".").split(".")[0] not in preserved.module_objects
    }

    pin_name = pin_stem if pin_stem and (pkg_dir / f"{pin_stem}.py").exists() else None
    if pin_name:
        exports.pop(f".{pin_name}", None)

    # collision check across direct contributors
    name_owner: dict[str, str] = {}
    for relmod, names in exports.items():
        for name in names:
            if name in name_owner and name_owner[name] != relmod:
                plan.collisions.append(
                    f"{dotted}: {name!r} exported by both {name_owner[name]} and {relmod}"
                )
            name_owner[name] = relmod

    source, all_names, _dropped = _compose_init(preserved, exports, default_doc, pin_name)
    if not init.exists() or init.read_text(encoding="utf-8") != source:
        rel = (
            str(init.relative_to(deployable_root))
            if init.is_relative_to(deployable_root)
            else str(init)
        )
        plan.changed.append(rel)
        if write:
            init.write_text(source, encoding="utf-8")
    return all_names


class BarrelGenerator:
    """Idempotent AST barrel generation for every layer + kernel package."""

    @staticmethod
    def check(manifest: Manifest) -> BarrelPlan:
        return BarrelGenerator._run(manifest, write=False)

    @staticmethod
    def write(manifest: Manifest) -> BarrelPlan:
        return BarrelGenerator._run(manifest, write=True)

    @staticmethod
    def _run(manifest: Manifest, write: bool) -> BarrelPlan:
        plan = BarrelPlan()
        deployable_root = manifest.roots.deployable

        for layer in manifest.layers:
            layer_dir = manifest.roots.app / layer
            if not layer_dir.is_dir():
                continue
            prefix = f"{manifest.roots.app.name}.{layer}"
            pin_stem = manifest.job_root_class if layer in manifest.job_roots else None
            _regenerate_tree(
                layer_dir,
                layer_dir,
                prefix,
                f"{layer.capitalize()} — layer barrel (generated, DOCTRINE §5.1).",
                deployable_root,
                write,
                plan,
                pin_stem=pin_stem,
            )

        for pkg_name, pkg_dir in manifest.roots.kernel.items():
            if pkg_name not in manifest.kernel_barrel_packages:
                continue
            if not pkg_dir.is_dir():
                continue
            prefix = f"{manifest.roots.kernel_dev_root_name}.{pkg_name}"
            _regenerate_tree(
                pkg_dir,
                pkg_dir,
                prefix,
                f"{pkg_name.capitalize()} — kernel package (generated, DOCTRINE §5.1).",
                deployable_root,
                write,
                plan,
            )

        return plan
