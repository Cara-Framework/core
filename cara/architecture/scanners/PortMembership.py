"""PortMembership: ports must earn their keep (DOCTRINE §2).

"``app/ports`` must not become the next junk drawer. A port exists only
when it is (a) a real boundary the consumer owns, (b) an implementation
that can plausibly be swapped or an external-system edge, or (c) a stable
capability used by more than one use-case." This scanner makes (b)/(c)
mechanical: every class declared in the manifest's ``ports`` layer
(``manifest.layers`` must include a layer literally named ``"ports"`` —
absent that, the check no-ops) needs EITHER

* at least two DISTINCT implementor classes elsewhere in the scanned
  trees (a class whose bases reference the port by name — a real seam
  actually being swapped), OR
* a documented ``manifest.port_membership_tags`` comment (default
  ``"# port: <reason>"``) on the class's own line or its leading comment
  block — the (a)/(b) escape hatch for a genuine external-system edge
  that has (and may always have) exactly one implementation.

A port satisfying neither is auto-minted ceremony, not a boundary.
"""

from __future__ import annotations

import ast

from cara.architecture._ast_utils import parse, python_files, relpath
from cara.architecture.Finding import Finding
from cara.architecture.Manifest import Manifest

PORTS_LAYER = "ports"


def _leading_comment_block(lines: list[str], lineno: int) -> str:
    """The class def line plus any contiguous comment lines directly above it."""
    start = lineno - 1
    i = start - 1
    while i >= 0 and lines[i].strip().startswith("#"):
        i -= 1
    return "\n".join(lines[i + 1 : lineno])


def _port_classes(manifest: Manifest) -> list[tuple[str, str, int, str]]:
    """(class name, repo-relative file, lineno, leading-comment-block) for
    every top-level class declared under the ports layer."""
    ports_dir = manifest.roots.app / PORTS_LAYER
    out: list[tuple[str, str, int, str]] = []
    for path in python_files(ports_dir):
        if path.name == "__init__.py":
            continue
        tree = parse(path)
        if tree is None:
            continue
        source = path.read_text(encoding="utf-8")
        lines = source.splitlines()
        rel = relpath(path, manifest.roots.deployable)
        for node in tree.body:
            if isinstance(node, ast.ClassDef):
                block = _leading_comment_block(lines, node.lineno)
                out.append((node.name, rel, node.lineno, block))
    return out


def _base_names(node: ast.ClassDef) -> set[str]:
    names: set[str] = set()
    for base in node.bases:
        if isinstance(base, ast.Name):
            names.add(base.id)
        elif isinstance(base, ast.Attribute):
            names.add(base.attr)
    return names


def _implementor_count(manifest: Manifest, port_name: str, port_file: str) -> int:
    files_implementing: set[str] = set()
    for base in manifest.roots.scan_dirs():
        for path in python_files(base):
            rel = relpath(path, manifest.roots.deployable)
            if rel == port_file:
                continue
            tree = parse(path)
            if tree is None:
                continue
            for node in ast.walk(tree):
                if isinstance(node, ast.ClassDef) and port_name in _base_names(node):
                    files_implementing.add(rel)
    return len(files_implementing)


class PortMembership:
    """A port needs >=2 implementors or a documented ``# port: <reason>`` tag."""

    @staticmethod
    def scan(manifest: Manifest) -> list[Finding]:
        if PORTS_LAYER not in manifest.layers:
            return []
        findings: list[Finding] = []
        for name, rel, lineno, block in _port_classes(manifest):
            if manifest.port_membership_tags in block:
                continue
            count = _implementor_count(manifest, name, rel)
            if count < 2:
                findings.append(
                    Finding(
                        rel,
                        lineno,
                        f"port {name!r} has {count} implementor(s) — needs >=2, or a "
                        f"documented {manifest.port_membership_tags} <reason> tag",
                    )
                )
        return findings
