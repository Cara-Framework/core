"""Glob patterns confined to an allowlist of cache-key families.

A shared Redis store holds read-through caches next to state nothing
rebuilds (revocations, lockouts, job locks, leases). An operator-supplied
eviction glob must therefore be confined: a pattern is accepted only when
its LITERAL HEAD — everything before its first glob metacharacter — starts
with one of the allowed family prefixes. ``*`` or ``a*`` has a head that no
real family prefix contains, so it is refused instead of sweeping the store.
"""

from __future__ import annotations

import re
from collections.abc import Iterable

# Redis glob metacharacters: the literal head of a pattern ends at the first.
_GLOB_META = re.compile(r"[*?\[\\]")


def glob_literal_head(pattern: str) -> str:
    """Return the part of ``pattern`` before its first glob metacharacter."""
    return _GLOB_META.split(pattern, maxsplit=1)[0]


def patterns_within_families(
    patterns: Iterable[object], families: tuple[str, ...]
) -> list[str]:
    """Strip, validate and de-duplicate ``patterns`` against ``families``.

    Raises ``ValueError`` on an empty list, a non-string / blank entry, or a
    pattern whose literal head escapes every family. Order is preserved.
    """
    items = list(patterns or [])
    if not items:
        raise ValueError("patterns is required — no implicit eviction set exists.")
    if not families:
        raise ValueError("no cache family is allowlisted.")
    targets: list[str] = []
    for pattern in items:
        if not isinstance(pattern, str) or not pattern.strip():
            raise ValueError("patterns must be non-empty strings.")
        target = pattern.strip()
        if not glob_literal_head(target).startswith(families):
            raise ValueError(
                f"{target} can reach keys outside the read caches. "
                f"Patterns must start with one of: {', '.join(families)}"
            )
        targets.append(target)
    return list(dict.fromkeys(targets))
