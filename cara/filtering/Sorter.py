"""Composable sort dimension — peer of ``Filter`` for ORDER BY.

A ``Sorter`` describes ONE sort strategy end-to-end:

* its canonical name (``"price_asc"``, ``"recent"``, ``"trending"``)
* its UI label / group / aliases (e.g. ``"savings"`` aliased to
  ``"discount_pct"`` without two registry rows)
* how to apply itself to a Cara QueryBuilder
* its describe() output for the same wizard introspection that
  ``Filter.describe`` produces

Why a framework instead of a per-repo ``if sort_by == "...":`` ladder:

* Adding a sort = one file. The HTTP layer's ``in:...`` validation
  rule, the repo's ORDER BY clause, the wizard's option list, and
  the cache key all stay in lockstep.
* Frontend wizards / dashboards can introspect the registry and
  render a sort dropdown without hand-listing options.
* The same registry can be reused from any list endpoint
  (records, feeds, search results) — adding a new sort dimension
  propagates automatically.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class Sorter(ABC):
    """One sort dimension. Subclass to add a new dimension."""

    #: Canonical, snake_case sort name.
    name: str = ""

    #: Human-readable wizard label.
    label: str = ""

    #: Short help text.
    description: str = ""

    #: Alternate names that should resolve to this sorter without
    #: growing the registry (e.g. ``"savings"`` → ``"discount_pct"``).
    aliases: tuple[str, ...] = ()

    @abstractmethod
    def apply(self, query: Any) -> Any:
        """Apply ``ORDER BY`` to a Cara QueryBuilder and return it.

        The caller has already configured the SELECT, WHERE, GROUP BY
        etc.; the sorter only adds ordering. Implementations may
        also adjust the SELECT list (e.g. a popularity sort joins a
        related table and selects ``COUNT(...) AS hit_count``)
        — encapsulating that here keeps the repo body sort-agnostic.
        """

    def describe(self) -> dict[str, Any]:
        """JSON-serialisable spec for the wizard / docs payload."""
        return {
            "name": self.name,
            "label": self.label or self.name.replace("_", " ").title(),
            "description": self.description,
            "aliases": list(self.aliases),
        }

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} name={self.name!r}>"
