"""Composable sort dimension — peer of ``Filter`` for ORDER BY.

A ``Sorter`` describes ONE sort strategy end-to-end:

* its canonical name (``"price_asc"``, ``"newest"``, ``"trending"``)
* its UI label / group / aliases (e.g. ``"newest"`` aliased to
  ``"recent"`` for backward-compat without two registry rows)
* how to apply itself to a Cara QueryBuilder
* its describe() output for the same wizard introspection that
  ``Filter.describe`` produces

Why a framework instead of a per-repo ``if sort_by == "...":`` ladder:

* Adding a sort = one file. The HTTP layer's ``in:...`` validation
  rule, the repo's ORDER BY clause, the wizard's option list, and
  the cache key all stay in lockstep.
* Storefront wizards / dashboards can introspect the registry and
  render a sort dropdown without hand-listing options.
* The same registry can be reused from any list endpoint
  (products, deals, sponsored, search results) — adding a new
  sort dimension propagates automatically.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, Iterable, List, Optional, Tuple


class Sorter(ABC):
    """One sort dimension. Subclass to add a new dimension."""

    #: Canonical, snake_case sort name.
    name: str = ""

    #: Human-readable wizard label.
    label: str = ""

    #: Short help text.
    description: str = ""

    #: Alternate names that should resolve to this sorter (kept for
    #: backwards-compat without growing the registry — e.g.
    #: ``"newest"`` aliases to the canonical ``"recent"``).
    aliases: Tuple[str, ...] = ()

    #: True for the default sort that should be picked when the
    #: caller didn't supply one. Exactly one ``Sorter`` per registry
    #: should set this; ``SortRegistry`` validates uniqueness.
    is_default: bool = False

    @abstractmethod
    def apply(self, query: Any) -> Any:
        """Apply ``ORDER BY`` to a Cara QueryBuilder and return it.

        The caller has already configured the SELECT, WHERE, GROUP BY
        etc.; the sorter only adds ordering. Implementations may
        also adjust the SELECT list (e.g. trending sort joins
        outbound_click and selects ``COUNT(...) AS click_count``)
        — encapsulating that here keeps the repo body sort-agnostic.
        """

    def describe(self) -> Dict[str, Any]:
        """JSON-serialisable spec for the wizard / docs payload."""
        return {
            "name": self.name,
            "label": self.label or self.name.replace("_", " ").title(),
            "description": self.description,
            "aliases": list(self.aliases),
            "is_default": self.is_default,
        }

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} name={self.name!r}>"


class SortRegistry:
    """Name-unique, alias-aware registry of ``Sorter`` instances.

    Mirrors the shape of ``FilterSet`` so consumers (FormRequests,
    repos, the schema endpoint) treat sorting and filtering the same
    way.
    """

    def __init__(
        self,
        sorters: Iterable[Sorter],
        *,
        default: Optional[str] = None,
    ) -> None:
        """Build a registry, with optional registry-level default.

        Args:
            sorters: The sorters this registry exposes.
            default: Optional default-sorter name (or alias). When
                set, overrides whichever sorter has ``is_default = True``
                at the class level — useful when the same Sorter
                class is reused across registries that disagree on
                the default (``RecentSorter`` is the catalogue
                default but the deal feed wants ``BestDealSorter``
                first).
        """
        self._sorters: List[Sorter] = list(sorters)

        if not self._sorters:
            raise ValueError("SortRegistry requires at least one sorter")

        seen: Dict[str, Sorter] = {}
        class_defaults: List[Sorter] = []
        for s in self._sorters:
            if not s.name:
                raise ValueError(
                    f"Sorter {s.__class__.__name__!r} has no ``name`` attribute"
                )
            if s.name in seen:
                raise ValueError(
                    f"Duplicate sorter name {s.name!r} in registry "
                    f"({seen[s.name].__class__.__name__} vs "
                    f"{s.__class__.__name__})"
                )
            seen[s.name] = s
            for alias in s.aliases:
                if alias in seen:
                    raise ValueError(
                        f"Sorter alias {alias!r} (from {s.name!r}) collides "
                        f"with name of {seen[alias].__class__.__name__}"
                    )
                seen[alias] = s
            if s.is_default:
                class_defaults.append(s)

        # ``default=`` kwarg wins. When unset, fall back to the
        # ``is_default`` class flag (legacy semantics).
        chosen: Optional[Sorter] = None
        if default is not None:
            chosen = seen.get(default)
            if chosen is None:
                raise ValueError(
                    f"SortRegistry default={default!r} doesn't match any "
                    f"sorter name or alias in this registry"
                )
        else:
            if not class_defaults:
                raise ValueError(
                    "SortRegistry needs exactly one default sorter — "
                    "set ``is_default = True`` on one or pass "
                    "``default=<name>`` explicitly"
                )
            if len(class_defaults) > 1:
                names = ", ".join(d.name for d in class_defaults)
                raise ValueError(
                    f"Multiple class-level default sorters: {names}. "
                    f"Resolve by passing ``default=<name>`` to the registry."
                )
            chosen = class_defaults[0]

        self._default: Sorter = chosen
        self._by_name: Dict[str, Sorter] = seen

    # ── Resolution ─────────────────────────────────────────────────

    def resolve(self, name: Optional[str]) -> Sorter:
        """Return the sorter matching ``name`` (or the default if missing).

        Unknown names also fall back to the default rather than
        raising — keeping the listing endpoint usable when the
        storefront sends a stale sort value during a deploy.
        """
        if not name:
            return self._default
        return self._by_name.get(name, self._default)

    # ── Composition ────────────────────────────────────────────────

    def apply(self, query: Any, name: Optional[str]) -> Tuple[Any, Sorter]:
        """Resolve and apply the sort, returning ``(query, sorter)``.

        The caller often wants the resolved sorter back (e.g. to
        echo the canonical name in the response meta), which is
        why this returns a tuple instead of just the query.
        """
        sorter = self.resolve(name)
        return sorter.apply(query), sorter

    # ── Introspection ──────────────────────────────────────────────

    def names(self) -> List[str]:
        """Canonical names in declaration order (no aliases)."""
        return [s.name for s in self._sorters]

    def all_names(self) -> List[str]:
        """Canonical names + aliases. Used to build the ``in:`` rule."""
        out: List[str] = []
        for s in self._sorters:
            out.append(s.name)
            out.extend(s.aliases)
        return out

    def validation_rule(self) -> str:
        """Cara FormRequest rule string for ``sort_by`` payload key.

        Auto-generated from the registry so a new sorter dimension
        never has to coordinate with the FormRequest manually.
        """
        return "nullable|string|in:" + ",".join(self.all_names())

    def describe(self) -> Dict[str, Any]:
        """JSON-serialisable spec for the wizard."""
        return {
            "name": "sort_by",
            "label": "Sort by",
            "default": self._default.name,
            "options": [s.describe() for s in self._sorters],
        }
