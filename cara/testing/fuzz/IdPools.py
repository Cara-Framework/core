"""Real ids a fuzz strategy may use, keyed by parameter or body-field name."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence


class IdPools:
    """Name -> real values, with the plural / ``_public_id`` / ``_id`` forms folded.

    A product fills it from its own database (a scratch one, for writes) so a
    generated request can name a row that exists and reach the handler body
    instead of stopping at the 404 every fake id gets.
    """

    def __init__(self, values: Mapping[str, Sequence[str]] | None = None) -> None:
        self.values: dict[str, list[str]] = {
            name: [str(v) for v in pool] for name, pool in (values or {}).items() if pool
        }

    def for_name(self, name: str) -> list[str]:
        """The pool a parameter called ``name`` draws from (empty when none)."""
        if name in self.values:
            return self.values[name]
        singular = re.sub(r"s$", "", name)
        for candidate in (singular, f"{singular}_public_id", f"{name}_public_id"):
            if candidate in self.values:
                return self.values[candidate]
        stem = re.sub(r"(_public)?_ids?$", "", singular)
        for candidate in (f"{stem}_public_id", f"{stem}_id", stem):
            if candidate in self.values:
                return self.values[candidate]
        return []
