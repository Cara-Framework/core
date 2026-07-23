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
  package (§2: "shared" membership requires >=2 PROVABLE consumer processes)
  that only ONE group in ``manifest.roots.consumer_roots`` actually imports
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
                        names_found = {
                            a.name
                            for a in node.names
                            if not manifest.side_effect_facade_names
                            or a.name in manifest.side_effect_facade_names
                        }
                        if not names_found:
                            continue
                        names = ", ".join(sorted(names_found))
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
        consumer_groups = {
            name: tuple(root for root in roots if root.is_dir())
            for name, roots in manifest.roots.consumer_roots.items()
        }
        consumer_groups = {
            name: roots for name, roots in consumer_groups.items() if roots
        }
        if shared_dir is None or len(consumer_groups) < 2:
            return []
        kernel_root = manifest.roots.kernel_dev_root_name
        stems = {
            path.stem for path in python_files(shared_dir) if path.stem != "__init__"
        }
        barrel_symbols = _shared_barrel_symbols(shared_dir, stems)
        consumed_by_group = {
            name: _consumed_shared_stems(
                roots,
                kernel_root,
                stems,
                barrel_symbols,
                skip_consumer_barrel=True,
            )
            for name, roots in consumer_groups.items()
        }
        kernel_consumers = _consumed_shared_stems(
            tuple(
                root
                for package, root in manifest.roots.kernel.items()
                if package != "shared"
            ),
            kernel_root,
            stems,
            barrel_symbols,
        )
        findings: list[Finding] = []
        for path in python_files(shared_dir):
            if path.stem == "__init__":
                continue
            stem = path.stem
            if stem in manifest.single_consumer_allowlist:
                continue
            consuming = sum(stem in used for used in consumed_by_group.values())
            if consuming == 1 and stem not in kernel_consumers:
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


def _shared_barrel_symbols(
    shared_dir: Path, stems: set[str]
) -> dict[tuple[str, str], str]:
    """(barrel suffix, exported symbol) -> defining module stem."""
    mapping: dict[tuple[str, str], str] = {}
    for init in sorted(shared_dir.rglob("__init__.py")):
        tree = parse(init)
        if tree is None:
            continue
        suffix = init.parent.relative_to(shared_dir).as_posix().replace("/", ".")
        suffix = "" if suffix == "." else suffix
        for node in tree.body:
            if not isinstance(node, ast.ImportFrom) or node.level == 0:
                continue
            if node.module:
                candidate = node.module.split(".")[-1]
                for alias in node.names:
                    if candidate in stems:
                        mapping[(suffix, alias.asname or alias.name)] = candidate
            else:
                for alias in node.names:
                    if alias.name in stems:
                        mapping[(suffix, alias.asname or alias.name)] = alias.name
    return mapping


def _consumed_shared_stems(
    tree_roots: tuple[Path, ...],
    kernel_root: str,
    stems: set[str],
    barrel_symbols: dict[tuple[str, str], str],
    *,
    skip_consumer_barrel: bool = False,
) -> set[str]:
    shared_roots = ("app.shared", f"{kernel_root}.shared")
    consumed: set[str] = set()
    for tree_root in tree_roots:
        for path in python_files(tree_root):
            if skip_consumer_barrel and path == tree_root / "shared" / "__init__.py":
                continue
            tree = parse(path)
            if tree is None:
                continue
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom) and node.module:
                    matched = next(
                        (
                            root
                            for root in shared_roots
                            if node.module == root or node.module.startswith(root + ".")
                        ),
                        None,
                    )
                    if matched is None:
                        continue
                    suffix = node.module[len(matched) :].lstrip(".")
                    candidate = suffix.split(".")[-1] if suffix else ""
                    if candidate in stems:
                        consumed.add(candidate)
                    for alias in node.names:
                        resolved = barrel_symbols.get(
                            (suffix, alias.asname or alias.name)
                        )
                        if resolved is not None:
                            consumed.add(resolved)
                        elif alias.name in stems:
                            consumed.add(alias.name)
                elif isinstance(node, ast.Import):
                    for alias in node.names:
                        matched = next(
                            (
                                root
                                for root in shared_roots
                                if alias.name.startswith(root + ".")
                            ),
                            None,
                        )
                        if matched is None:
                            continue
                        candidate = alias.name.split(".")[-1]
                        if candidate in stems:
                            consumed.add(candidate)
    return consumed
