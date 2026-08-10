"""SourceShape: hard file/class/edge budgets (DOCTRINE §5).

The product manifest supplies exact source roots. Generated barrels are
excluded; every other Python source file is governed by three rules:

* files above the hard line limit are refactoring debt;
* a file declares at most one public top-level class, named for the file;
* public methods on controller/job edge classes stay within the edge-method
  limit.

Existing violations are exact, shrink-only debts in ``seam_allowlists``:

* ``source_shape_lines``: ``path -> current line count``;
* ``source_shape_classes``: ``path -> current public-class count``;
* ``source_shape_edge_methods``:
  ``path::Class.method -> current method line count``.

Exact counts make both growth and silent stale pins fail. Products cannot hide
an oversized file behind a boolean exemption.
"""

from __future__ import annotations

import ast
from pathlib import Path

from cara.architecture._ast_utils import parse, python_files, read_source, relpath
from cara.architecture._ratchet import ratchet as _ratchet
from cara.architecture.Finding import Finding
from cara.architecture.Manifest import Manifest

LINES_KEY = "source_shape_lines"
CLASSES_KEY = "source_shape_classes"
EDGE_METHODS_KEY = "source_shape_edge_methods"


def _is_edge_path(path: Path, edge_layers: frozenset[str], app_root: str) -> bool:
    """True when ``path`` names a layer directory that is a real edge root.

    ``app_root`` is ``manifest.roots.app_path_prefix``, never the literal
    ``"app"``: a hardcoded root name makes this predicate answer False for
    every file of a tree rooted under any other name, so the edge-method
    budget silently governed nothing at all. See ``ManifestRoots``.
    """
    parts = path.parts
    return any(
        part in edge_layers
        and (
            index == 0
            or parts[index - 1] in {app_root, "packages"}
            or "packages" in parts[:index]
        )
        for index, part in enumerate(parts)
    )


class SourceShape:
    """Enforce hard source budgets with exact shrink-only debt counts."""

    @staticmethod
    def scan(manifest: Manifest) -> list[Finding]:
        roots = manifest.roots.scan_dirs("source_shape")
        hard_limit = manifest.source_shape_hard_limit
        edge_limit = manifest.source_shape_edge_method_limit
        if hard_limit <= 0 or edge_limit <= 0:
            return [
                Finding(
                    "app/architecture_manifest.py",
                    0,
                    "source-shape limits must be positive integers",
                )
            ]

        oversized: dict[str, int] = {}
        multiclass: dict[str, int] = {}
        edge_methods: dict[str, int] = {}
        naming_findings: list[Finding] = []
        seen: set[Path] = set()

        for root in roots:
            for path in python_files(root):
                resolved = path.resolve()
                if resolved in seen or path.name == "__init__.py":
                    continue
                seen.add(resolved)
                rel = relpath(path, manifest.roots.deployable)
                # Read AFTER the file has proved readable, the way the sibling
                # source-law scanners do. Counting lines first meant a single
                # non-UTF-8 byte anywhere under a scan root raised out of the
                # scanner and took the whole pack down, instead of that one
                # file being skipped like every other unparseable file.
                tree = parse(path)
                source = read_source(path)
                if tree is None or source is None:
                    continue
                line_count = len(source.splitlines())
                if line_count > hard_limit:
                    oversized[rel] = line_count
                public_classes = [
                    node
                    for node in tree.body
                    if isinstance(node, ast.ClassDef) and not node.name.startswith("_")
                ]
                if len(public_classes) > 1:
                    multiclass[rel] = len(public_classes)
                elif len(public_classes) == 1 and public_classes[0].name != path.stem:
                    naming_findings.append(
                        Finding(
                            rel,
                            public_classes[0].lineno,
                            f"public class {public_classes[0].name!r} must be named "
                            f"for file {path.stem!r}",
                        )
                    )

                relative_path = Path(rel)
                if not _is_edge_path(
                    relative_path,
                    manifest.source_shape_edge_layers,
                    manifest.roots.app_path_prefix,
                ):
                    continue
                for class_node in public_classes:
                    for node in class_node.body:
                        if not isinstance(
                            node, (ast.FunctionDef, ast.AsyncFunctionDef)
                        ) or node.name.startswith("_"):
                            continue
                        lines = (node.end_lineno or node.lineno) - node.lineno + 1
                        if lines > edge_limit:
                            identity = f"{rel}::{class_node.name}.{node.name}"
                            edge_methods[identity] = lines

        allowlists = manifest.seam_allowlists
        return (
            naming_findings
            + _ratchet(
                key=LINES_KEY,
                current=oversized,
                pinned=allowlists.get(LINES_KEY, {}),
                message=f"file exceeds hard {hard_limit}-line limit",
            )
            + _ratchet(
                key=CLASSES_KEY,
                current=multiclass,
                pinned=allowlists.get(CLASSES_KEY, {}),
                message="file declares multiple public classes",
            )
            + _ratchet(
                key=EDGE_METHODS_KEY,
                current=edge_methods,
                pinned=allowlists.get(EDGE_METHODS_KEY, {}),
                message=f"edge method exceeds {edge_limit}-line limit",
            )
        )
