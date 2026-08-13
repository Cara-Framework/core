"""SeamLocations."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class SeamLocations:
    """DOCTRINE §4 — the Four Legal Seams a plugin token may appear at.

    ``composition_roots`` (Seam 2) and ``manifest_files`` (Seam 4) are
    deployable-relative paths; ``data_vocabulary_prefixes`` (Seam 1) are
    deployable-relative directory prefixes (typically a kernel models
    package) whose UPPER_SNAKE constants are exempt. ``owned_integration_prefixes``
    declares capability lanes that are not plug-ins (for example a
    ``discovery/<provider>`` lane) and the exact provider tokens each lane
    owns. The lane is still scanned: only its owned tokens are legal, so a
    dependency on one vendor inside another vendor's lane remains a finding.
    Seam 3 (generic, parameterized ingress routes) never touches an
    *identifier* or a scanned string-literal position, so it needs no location
    here.
    """

    composition_roots: frozenset[str] = frozenset()
    manifest_files: frozenset[str] = frozenset()
    data_vocabulary_prefixes: tuple[str, ...] = ()
    owned_integration_prefixes: dict[str, frozenset[str]] = field(default_factory=dict)
