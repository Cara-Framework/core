"""Finding: one Guard Pack violation.

Every scanner under ``cara/architecture/scanners/`` is a pure function of a
:class:`~cara.architecture.Manifest.Manifest` that returns ``list[Finding]``.
A ``Finding`` is deliberately dumb — no severity levels, no codes — because
DOCTRINE §11 treats every guard failure the same way: it fails the build.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Finding:
    """WHERE (``path`` + ``line``) and WHAT (``message``) of one violation.

    ``path`` is always relative to the manifest's deployable root (never an
    absolute filesystem path) so a Finding reads identically whether printed
    by ``craft arch:check`` in a container or asserted against in a pytest
    fixture built from a ``tmp_path``. ``line`` is 1-indexed; ``0`` marks a
    finding that pins a whole file or registry entry rather than one
    statement (e.g. a memberless domain).
    """

    path: str
    line: int
    message: str

    def __str__(self) -> str:  # pragma: no cover - trivial
        if self.line:
            return f"{self.path}:{self.line}: {self.message}"
        return f"{self.path}: {self.message}"
