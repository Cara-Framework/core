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
directories. Pre-rule imports that cannot yet carry a truthful tag are pinned
by exact ``path::imported-name -> hit count`` entries in
``seam_allowlists["inline_imports"]``. New identities, growth, shrinkage
without a ratchet update, and stale entries all fail.
"""

from __future__ import annotations

from collections import defaultdict
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
SEAM_KEY = "inline_imports"


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
        scan_bases = list(manifest.roots.scan_dirs("inline_imports")) + list(
            manifest.roots.kernel.values()
        )
        seen: set[Path] = set()
        exercised_exemptions: set[tuple[str, str]] = set()
        untagged: dict[str, list[tuple[int, str]]] = defaultdict(list)
        for base in scan_bases:
            for path in python_files(base):
                resolved = path.resolve()
                if resolved in seen:
                    continue
                seen.add(resolved)
                tree = parse(path)
                if tree is None:
                    continue
                source = path.read_text(encoding="utf-8")
                lines = source.splitlines()
                rel = relpath(path, manifest.roots.deployable)
                in_envelopes = any(d.resolve() in resolved.parents for d in envelope_dirs)
                for node in function_local_imports(tree):
                    start = max(node.lineno - 1, 0)
                    end = min(node.end_lineno or node.lineno, len(lines))
                    import_lines = lines[start:end]
                    line = import_lines[0] if import_lines else ""
                    tagged_line = next(
                        (candidate for candidate in import_lines if TAG in candidate),
                        None,
                    )
                    where_key = (rel, _first_imported_name(node))
                    if tagged_line is None:
                        if where_key in manifest.inline_import_exemptions:
                            exercised_exemptions.add(where_key)
                            continue
                        identity = f"{rel}::{where_key[1]}"
                        untagged[identity].append((node.lineno, line.strip()))
                        continue
                    reason = tagged_line.split(TAG, 1)[1].strip()
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
        for rel, imported_name in sorted(
            manifest.inline_import_exemptions - exercised_exemptions
        ):
            findings.append(
                Finding(
                    rel,
                    0,
                    f"stale inline-import exemption for {imported_name!r} — delete it",
                )
            )
        allowlist = manifest.seam_allowlists.get(SEAM_KEY, {})
        for identity, hits in sorted(untagged.items()):
            rel = identity.split("::", 1)[0]
            count = len(hits)
            pinned = allowlist.get(identity)
            detail = "; ".join(f"line {line}: {source}" for line, source in hits)
            if pinned is None:
                findings.append(
                    Finding(
                        rel,
                        hits[0][0],
                        f"{count} function-local import(s) without a "
                        f"'# local: <reason>' tag for {identity}: {detail}",
                    )
                )
            elif count > pinned:
                findings.append(
                    Finding(
                        rel,
                        hits[0][0],
                        f"inline-import debt grew for {identity}: {pinned} -> {count}",
                    )
                )
            elif count < pinned:
                findings.append(
                    Finding(
                        rel,
                        hits[0][0],
                        f"stale inline-import pin for {identity}: {pinned}, now {count}",
                    )
                )
        for identity, pinned in sorted(allowlist.items()):
            if identity not in untagged:
                findings.append(
                    Finding(
                        identity.split("::", 1)[0],
                        0,
                        f"stale inline-import pin for {identity}: {pinned}, "
                        "violation resolved",
                    )
                )
        return findings
