"""JobIdempotency: every queued job declares its idempotency identity
(DOCTRINE §8).

"Every job declares ``idempotency_params``." Walks every class riding the
manifest's job base class (``manifest.job_root_class``, default
``BaseJob``) under the manifest's job roots (``manifest.job_roots`` —
``app/jobs/**`` and, if the manifest declares a ``packages`` root, every
``packages/*/jobs/**``) and requires ONE of:

* a class-level ``manifest.idempotency_field_name`` assignment on the
  class itself or an ancestor within the scanned trees (inherited
  contracts count — a subclass shares its parent's identity fields), or
* a documented opt-out on the class body::

      # idempotency: none — <why this job has no duplicate-dispatch risk>

  (the em-dash and a reason are mandatory — an undocumented opt-out is
  indistinguishable from a forgotten declaration), or
* a dated ``manifest.job_idempotency_exemptions`` pin
  (``"<path>::<ClassName>"``) — pre-rule jobs, shrink-only.
"""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass
from pathlib import Path

from cara.architecture._ast_utils import parse, python_files, relpath
from cara.architecture.Finding import Finding
from cara.architecture.Manifest import Manifest

_EXEMPTION_TAG = re.compile(r"#[ \t]*idempotency:[ \t]*none[ \t]*—[ \t]*\S+")


@dataclass(slots=True)
class _ClassInfo:
    bases: list[str]
    declares: bool
    rel: str
    tagged: bool


def _job_files(manifest: Manifest) -> list[Path]:
    files: list[Path] = []
    for job_root in manifest.job_roots:
        files.extend(python_files(manifest.roots.app / job_root))
    if manifest.roots.packages is not None and manifest.roots.packages.is_dir():
        for package in sorted(manifest.roots.packages.iterdir()):
            if not package.is_dir():
                continue
            for job_root in manifest.job_roots:
                files.extend(python_files(package / job_root))
    return sorted(set(files))


def _collect(manifest: Manifest) -> dict[str, list[_ClassInfo]]:
    classes: dict[str, list[_ClassInfo]] = {}
    field_name = manifest.idempotency_field_name
    for path in _job_files(manifest):
        tree = parse(path)
        if tree is None:
            continue
        source = path.read_text(encoding="utf-8")
        lines = source.splitlines()
        rel = relpath(path, manifest.roots.deployable)
        for node in ast.walk(tree):
            if not isinstance(node, ast.ClassDef):
                continue
            bases: list[str] = []
            for base in node.bases:
                if isinstance(base, ast.Name):
                    bases.append(base.id)
                elif isinstance(base, ast.Attribute):
                    bases.append(base.attr)
            declares = any(
                (
                    isinstance(stmt, ast.Assign)
                    and any(
                        isinstance(t, ast.Name) and t.id == field_name
                        for t in stmt.targets
                    )
                )
                or (
                    isinstance(stmt, ast.AnnAssign)
                    and isinstance(stmt.target, ast.Name)
                    and stmt.target.id == field_name
                )
                for stmt in node.body
            )
            segment = "\n".join(lines[node.lineno - 1 : node.end_lineno])
            tagged = bool(_EXEMPTION_TAG.search(segment))
            classes.setdefault(node.name, []).append(
                _ClassInfo(bases, declares, rel, tagged)
            )
    return classes


def _rides_base(
    name: str,
    root_class: str,
    classes: dict[str, list[_ClassInfo]],
    seen: frozenset[str] = frozenset(),
) -> bool:
    if name == root_class:
        return True
    if name in seen or name not in classes:
        return False
    return any(
        _rides_base(base, root_class, classes, seen | {name})
        for info in classes[name]
        for base in info.bases
    )


def _chain_declares(
    name: str, classes: dict[str, list[_ClassInfo]], seen: frozenset[str] = frozenset()
) -> bool:
    if name in seen or name not in classes:
        return False
    return any(
        info.declares
        or any(_chain_declares(base, classes, seen | {name}) for base in info.bases)
        for info in classes[name]
    )


class JobIdempotency:
    """Every queued job declares (or documents an opt-out for) idempotency."""

    @staticmethod
    def scan(manifest: Manifest) -> list[Finding]:
        classes = _collect(manifest)
        root_class = manifest.job_root_class
        exemptions = manifest.job_idempotency_exemptions
        findings: list[Finding] = []
        satisfied: set[str] = set()
        known: set[str] = set()
        for name, infos in classes.items():
            if name == root_class or not _rides_base(name, root_class, classes):
                continue
            for info in infos:
                key = f"{info.rel}::{name}"
                known.add(key)
                ok = info.declares or info.tagged or _chain_declares(name, classes)
                if ok:
                    satisfied.add(key)
                    continue
                if key in exemptions:
                    continue
                findings.append(
                    Finding(
                        info.rel,
                        0,
                        f"{name}: declare class-level `{manifest.idempotency_field_name}` "
                        f"(or the documented opt-out `# idempotency: none — <reason>`)",
                    )
                )
        for key in sorted(exemptions):
            if key in satisfied:
                findings.append(
                    Finding(
                        "job_idempotency_exemptions",
                        0,
                        f"{key}: now declares/tags — delete the pin",
                    )
                )
            elif key not in known:
                findings.append(
                    Finding(
                        "job_idempotency_exemptions",
                        0,
                        f"{key}: no such job class exists — delete the pin",
                    )
                )
        return findings
