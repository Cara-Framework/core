"""InlineImports: the ``# local:`` reason-tag law (DOCTRINE §5.1).

Imports live at the top of the file. A function-local import is legal in
exactly three cases, and MUST carry a ``# local: <reason>`` tag on its own
line naming which:

    # local: envelope body        (only inside a kernel envelope-body dir —
                                   shells parse without the app installed)
    # local: cycle with <module>  (a proven cycle-breaker; must name the
                                   module that completes the cycle)
    # local: heavy optional dep   (browser engines, connector SDKs — boot
                                   speed and optionality)

An untagged function-local import is a Finding, as is an unrecognised
reason or an ``envelope body`` tag outside the manifest's declared envelope
directories. ``manifest.inline_import_exemptions`` is a documented, dated
escape hatch for pre-rule imports that cannot yet carry a truthful tag —
shrink-only by convention, exactly like the product guards' ``_EXEMPT``.
"""

from __future__ import annotations

from pathlib import Path

from cara.architecture._ast_utils import (
    function_local_imports,
    parse,
    python_files,
    relpath,
)
from cara.architecture.Finding import Finding
from cara.architecture.Manifest import Manifest

TAG = "# local:"
CYCLE_PREFIX = "cycle with"
LEGAL_PREFIXES = ("envelope body", "heavy optional dep", CYCLE_PREFIX)


def _first_imported_name(node) -> str:
    alias = node.names[0]
    return alias.asname or alias.name


def _envelope_dirs(manifest: Manifest) -> tuple[Path, ...]:
    """Directories where an ``envelope body`` tag is truthful: any kernel
    package directory named ``envelopes`` (the shape every product uses
    for cross-process job payload shells, DOCTRINE §8)."""
    dirs: list[Path] = []
    for pkg_dir in manifest.roots.kernel.values():
        candidate = pkg_dir / "envelopes"
        if candidate.is_dir():
            dirs.append(candidate)
    return tuple(dirs)


class InlineImports:
    """Every function-local import carries a legal ``# local:`` reason (§5.1)."""

    @staticmethod
    def scan(manifest: Manifest) -> list[Finding]:
        findings: list[Finding] = []
        envelope_dirs = _envelope_dirs(manifest)
        scan_bases = list(manifest.roots.scan_dirs()) + list(
            manifest.roots.kernel.values()
        )
        for base in scan_bases:
            for path in python_files(base):
                tree = parse(path)
                if tree is None:
                    continue
                source = path.read_text(encoding="utf-8")
                lines = source.splitlines()
                rel = relpath(path, manifest.roots.deployable)
                resolved = path.resolve()
                in_envelopes = any(d.resolve() in resolved.parents for d in envelope_dirs)
                for node in function_local_imports(tree):
                    line = lines[node.lineno - 1] if node.lineno - 1 < len(lines) else ""
                    where_key = (rel, _first_imported_name(node))
                    if TAG not in line:
                        if where_key in manifest.inline_import_exemptions:
                            continue
                        findings.append(
                            Finding(
                                rel,
                                node.lineno,
                                f"function-local import without a '# local: <reason>' "
                                f"tag: {line.strip()}",
                            )
                        )
                        continue
                    reason = line.split(TAG, 1)[1].strip()
                    if not reason.startswith(LEGAL_PREFIXES):
                        findings.append(
                            Finding(
                                rel,
                                node.lineno,
                                f"unknown local-import reason {reason!r} (legal: "
                                f"{', '.join(LEGAL_PREFIXES)}<module>)",
                            )
                        )
                        continue
                    if (
                        reason.startswith(CYCLE_PREFIX)
                        and not reason[len(CYCLE_PREFIX) :].strip()
                    ):
                        findings.append(
                            Finding(rel, node.lineno, "'cycle with' names no module")
                        )
                    if reason.startswith("envelope body") and not in_envelopes:
                        findings.append(
                            Finding(
                                rel,
                                node.lineno,
                                "'envelope body' tag outside a declared envelope directory",
                            )
                        )
        return findings
