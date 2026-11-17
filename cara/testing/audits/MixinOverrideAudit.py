"""MixinOverrideAudit: a class never redefines a method of its own mixin slice.

The mixin split keeps files inside their line budget by moving a class's
behaviour into ``*Mixin`` slices the class inherits (DOCTRINE §6: mixins are
slices with byte-identical public surfaces, not bases with hooks). The slices
and the class body share ONE namespace, and Python resolves a clash silently
in the class body's favour: a host method named like a mixin's helper
replaces it for the mixin's own callers too — the signatures need not even
match, so the first symptom is a 500 on a route that calls the mixin.

A same-class duplicate guard cannot see this: the two definitions live in
different classes and different files. This audit resolves a ``*Mixin`` base
by class name among the scanned tree's modules (recursively through mixins
of mixins) and reports every host method whose name a slice already defines.
Dunder methods are exempt.
"""

from __future__ import annotations

import ast
from collections.abc import Sequence
from pathlib import Path

from ._scan import _iter_sources
from .MixinOverrideFinding import MixinOverrideFinding


def _base_name(node: ast.expr) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def _methods(node: ast.ClassDef) -> dict[str, int]:
    return {
        item.name: item.lineno
        for item in node.body
        if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef))
        and not item.name.startswith("__")
    }


class MixinOverrideAudit:
    """Find host methods that silently shadow a method of their own mixin."""

    def __init__(self, suffix: str = "Mixin") -> None:
        self._suffix = suffix

    def scan_sources(
        self, sources: Sequence[tuple[str, str]]
    ) -> list[MixinOverrideFinding]:
        classes: dict[str, list[tuple[str, ast.ClassDef]]] = {}
        for source, path in sources:
            try:
                tree = ast.parse(source)
            except SyntaxError:
                continue
            for node in tree.body:
                if isinstance(node, ast.ClassDef):
                    classes.setdefault(node.name, []).append((path, node))

        def slice_methods(name: str, seen: set[str]) -> dict[str, str]:
            found: dict[str, str] = {}
            for _path, node in classes.get(name, []):
                for base in node.bases:
                    base_name = _base_name(base)
                    if (
                        base_name
                        and base_name.endswith(self._suffix)
                        and base_name not in seen
                    ):
                        seen.add(base_name)
                        found.update(slice_methods(base_name, seen))
                found.update(dict.fromkeys(_methods(node), name))
            return found

        findings: list[MixinOverrideFinding] = []
        for defs in classes.values():
            for path, node in defs:
                inherited: dict[str, str] = {}
                for base in node.bases:
                    base_name = _base_name(base)
                    if (
                        base_name
                        and base_name.endswith(self._suffix)
                        and base_name in classes
                    ):
                        inherited.update(slice_methods(base_name, {base_name}))
                for method, line in _methods(node).items():
                    if method in inherited:
                        findings.append(
                            MixinOverrideFinding(
                                path, line, node.name, method, inherited[method]
                            )
                        )
        return sorted(findings, key=lambda f: (f.path, f.line))

    def scan_tree(
        self, root: Path, directories: Sequence[str] | None = None
    ) -> list[MixinOverrideFinding]:
        return self.scan_sources(list(_iter_sources(root, directories)))

    @staticmethod
    def report(findings: Sequence[MixinOverrideFinding]) -> str:
        return (
            "A class body silently shadows a method of its own mixin slice — "
            "rename one of them:\n" + "\n".join(str(finding) for finding in findings)
        )
