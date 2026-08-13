"""The shrink-only debt ratchet shared by counting scanners.

DOCTRINE §11 allows a product to adopt a guard over a tree that is not yet
clean, on one condition: the debt is EXACT and may only shrink. An exact
count makes three different mistakes all fail loudly —

* a NEW violation in an unpinned file (the guard's whole purpose);
* GROWTH in a pinned file (debt quietly compounding);
* a STALE pin (the file got better, or was fixed and the pin outlived it) —

which is why a boolean "this file is exempt" flag is never acceptable: it
only catches the first, and it never expires.

Leading underscore: internal to ``cara/architecture``. Scanners expose the
behaviour through their own ``seam_allowlists`` key, so a product reads the
debt in the vocabulary of the rule it owes, not of this helper.
"""

from __future__ import annotations

from collections.abc import Mapping

from cara.architecture.Finding import Finding


def _ratchet(
    *,
    key: str,
    current: Mapping[str, int],
    pinned: Mapping[str, int],
    message: str,
) -> list[Finding]:
    """Compare live counts against pinned ones; anything but equality fails.

    ``current`` and ``pinned`` are keyed by an identity whose leading
    ``::``-free segment is the path a Finding reports against (so a scanner
    may pin ``path`` or ``path::Class.method`` with the same helper).
    """
    findings: list[Finding] = []
    for identity, count in sorted(current.items()):
        path = identity.split("::", 1)[0]
        expected = pinned.get(identity)
        if expected is None:
            findings.append(Finding(path, 0, f"{message}: {identity} ({count})"))
        elif count > expected:
            findings.append(
                Finding(path, 0, f"{key} debt grew for {identity}: {expected} -> {count}")
            )
        elif count < expected:
            findings.append(
                Finding(
                    path, 0, f"stale {key} pin for {identity}: {expected}, now {count}"
                )
            )
    for identity, expected in sorted(pinned.items()):
        if identity not in current:
            path = identity.split("::", 1)[0]
            findings.append(
                Finding(
                    path,
                    0,
                    f"stale {key} pin for {identity}: {expected}, violation resolved",
                )
            )
    return findings
