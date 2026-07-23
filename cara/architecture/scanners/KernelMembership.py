"""KernelMembership: kernel direction, purity, and single-consumer eviction
(DOCTRINE §2).

Three checks over the dev-only kernel (``manifest.roots.kernel``):

* **Direction.** ``models`` imports NOTHING else in the kernel; ``contracts``
  may import ``models`` (typing) and itself, never ``gates``/``shared``.
  Violations are counted per file against ``manifest.seam_allowlists
  ["kernel_direction"]`` — the same dated, shrink-only sunset-debt
  mechanism VerticalSliceSeams uses: a NEW file or a GROWN count is a
  leak, a SHRUNK count is a stale pin to ratchet down. Only absolute
  (``level == 0``) imports are resolved — kernel packages are siblings
  under one dotted root, not nested, so a cross-package reach is
  overwhelmingly written that way in practice.
* **Purity.** A module named in ``manifest.pure_modules`` (kernel domain-math
  that computes over values it is handed) must import nothing from
  ``manifest.side_effect_facade_roots`` (the DB/Cache/Bus-shaped
  singletons) — reaching for one turns a pure function into a hidden
  data-access seam.
* **Single-consumer eviction.** A module living in the kernel's ``shared``
  package (§2: "shared" membership requires >=2 PROVABLE consumer trees)
  that only ONE of ``manifest.roots.consumer_app_roots`` actually imports
  is evicted-in-waiting; ``manifest.single_consumer_allowlist`` pins the
  current known set (shrink-only). This check no-ops when fewer than two
  consumer trees are present (a sibling deployable not checked out) —
  a whole-repo fact that per-service CI cannot evaluate alone.
"""

from __future__ import annotations

import ast
from pathlib import Path

from cara.architecture._ast_utils import parse, python_files, relpath
from cara.architecture.Finding import Finding
from cara.architecture.Manifest import Manifest

SEAM_KEY = "kernel_direction"


def _direction_violations(
    pkg_dir: Path, manifest: Manifest, forbidden: frozenset[str]
) -> dict[str, list[str]]:
    """repo-relative kernel file -> hit descriptions for forbidden imports."""
    kernel_root = manifest.roots.kernel_dev_root_name
    hits: dict[str, list[str]] = {}
    for path in python_files(pkg_dir):
        tree = parse(path)
        if tree is None:
            continue
        rel = relpath(path, manifest.roots.deployable)
        for node in ast.walk(tree):
            bases: list[str] = []
            lineno = getattr(node, "lineno", 0)
            if isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                bases = [node.module]
            elif isinstance(node, ast.Import):
                bases = [a.name for a in node.names]
            else:
                continue
            for base in bases:
                prefix = f"{kernel_root}."
                if not base.startswith(prefix):
                    continue
                pkg_name = base[len(prefix) :].split(".")[0]
                if pkg_name in forbidden:
                    hits.setdefault(rel, []).append(f"{rel}:{lineno}: imports {base}")
    return hits


def _seam_findings(
    hits: dict[str, list[str]], allowlist: dict[str, int], label: str
) -> list[Finding]:
    findings: list[Finding] = []
    for rel, entries in sorted(hits.items()):
        pinned = allowlist.get(rel)
        if pinned is None:
            findings.append(
                Finding(
                    rel,
                    0,
                    f"{label}: {len(entries)} forbidden import(s) — {'; '.join(entries)}",
                )
            )
        elif len(entries) > pinned:
            findings.append(
                Finding(
                    rel,
                    0,
                    f"{label}: count grew {pinned} -> {len(entries)} (shrink-only)",
                )
            )
    for rel, pinned in sorted(allowlist.items()):
        if rel not in hits:
            findings.append(
                Finding(
                    rel,
                    0,
                    f"{label}: stale allowlist pin ({pinned}) — the file has no hits",
                )
            )
    return findings


class KernelMembership:
    """Direction rules + purity + single-consumer eviction (§2)."""

    @staticmethod
    def scan(manifest: Manifest) -> list[Finding]:
        return (
            KernelMembership._direction(manifest)
            + KernelMembership._purity(manifest)
            + KernelMembership._single_consumer(manifest)
        )

    @staticmethod
    def _direction(manifest: Manifest) -> list[Finding]:
        kernel = manifest.roots.kernel
        allowlist = manifest.seam_allowlists.get(SEAM_KEY, {})
        hits: dict[str, list[str]] = {}
        if "models" in kernel:
            forbidden = manifest.kernel_packages - {"models"}
            for rel, entries in _direction_violations(
                kernel["models"], manifest, forbidden
            ).items():
                hits.setdefault(rel, []).extend(entries)
        if "contracts" in kernel:
            forbidden = manifest.kernel_packages - {"models", "contracts"}
            for rel, entries in _direction_violations(
                kernel["contracts"], manifest, forbidden
            ).items():
                hits.setdefault(rel, []).extend(entries)
        return _seam_findings(hits, allowlist, "kernel direction violation")

    @staticmethod
    def _purity(manifest: Manifest) -> list[Finding]:
        if not manifest.pure_modules or not manifest.side_effect_facade_roots:
            return []
        findings: list[Finding] = []
        for pkg_dir in manifest.roots.kernel.values():
            for path in python_files(pkg_dir):
                if path.stem not in manifest.pure_modules:
                    continue
                tree = parse(path)
                if tree is None:
                    continue
                rel = relpath(path, manifest.roots.deployable)
                for node in ast.walk(tree):
                    if not (isinstance(node, ast.ImportFrom) and node.module):
                        continue
                    if any(
                        node.module == root or node.module.startswith(root + ".")
                        for root in manifest.side_effect_facade_roots
                    ):
                        names = ", ".join(a.name for a in node.names)
                        findings.append(
                            Finding(
                                rel,
                                node.lineno,
                                f"pure module imports {names} from {node.module} — a "
                                f"pure domain-math module must not reach for a "
                                f"side-effect facade",
                            )
                        )
        return findings

    @staticmethod
    def _single_consumer(manifest: Manifest) -> list[Finding]:
        shared_dir = manifest.roots.kernel.get("shared")
        consumer_roots = tuple(r for r in manifest.roots.consumer_app_roots if r.is_dir())
        if shared_dir is None or len(consumer_roots) < 2:
            return []
        kernel_root = manifest.roots.kernel_dev_root_name
        findings: list[Finding] = []
        for path in python_files(shared_dir):
            if path.stem == "__init__":
                continue
            stem = path.stem
            if stem in manifest.single_consumer_allowlist:
                continue
            consuming = 0
            for tree_root in consumer_roots:
                if _tree_consumes_name(tree_root, kernel_root, stem):
                    consuming += 1
            if consuming == 1:
                rel = relpath(path, manifest.roots.deployable)
                findings.append(
                    Finding(
                        rel,
                        0,
                        f"'{stem}' is consumed by exactly one process tree — kernel "
                        f"'shared' membership requires >=2 provable consumers "
                        f"(evacuate it, or pin it in single_consumer_allowlist)",
                    )
                )
        return findings


def _tree_consumes_name(tree_root: Path, kernel_root: str, name: str) -> bool:
    shared_roots = ("app.shared", f"{kernel_root}.shared")
    for path in python_files(tree_root):
        tree = parse(path)
        if tree is None:
            continue
        for node in ast.walk(tree):
            if not (isinstance(node, ast.ImportFrom) and node.module):
                continue
            if not any(
                node.module == r or node.module.startswith(r + ".") for r in shared_roots
            ):
                continue
            if any(a.name == name for a in node.names):
                return True
    return False
