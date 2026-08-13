"""SortRegistry."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from cara.exceptions import InvalidArgumentException
from cara.filtering.Sorter import Sorter


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
        default: str,
    ) -> None:
        """Build a registry, with optional registry-level default.

        Args:
            sorters: The sorters this registry exposes.
            default: Required canonical default-sorter name or alias.
        """
        self._sorters: list[Sorter] = list(sorters)

        if not self._sorters:
            raise InvalidArgumentException("SortRegistry requires at least one sorter")

        seen: dict[str, Sorter] = {}
        for s in self._sorters:
            if not s.name:
                raise InvalidArgumentException(
                    f"Sorter {s.__class__.__name__!r} has no ``name`` attribute"
                )
            if s.name in seen:
                raise InvalidArgumentException(
                    f"Duplicate sorter name {s.name!r} in registry "
                    f"({seen[s.name].__class__.__name__} vs "
                    f"{s.__class__.__name__})"
                )
            seen[s.name] = s
            for alias in s.aliases:
                if alias in seen:
                    raise InvalidArgumentException(
                        f"Sorter alias {alias!r} (from {s.name!r}) collides "
                        f"with name of {seen[alias].__class__.__name__}"
                    )
                seen[alias] = s
        chosen = seen.get(default)
        if chosen is None:
            raise InvalidArgumentException(
                f"SortRegistry default={default!r} doesn't match any "
                f"sorter name or alias in this registry"
            )

        self._default: Sorter = chosen
        self._by_name: dict[str, Sorter] = seen

    # ── Resolution ─────────────────────────────────────────────────

    def resolve(self, name: str | None) -> Sorter:
        """Return the sorter matching ``name`` (or the default if missing).

        Unknown names also fall back to the default rather than
        raising — keeping the index endpoint usable when the
        client sends a stale sort value during a deploy.
        """
        if not name:
            return self._default
        return self._by_name.get(name, self._default)

    # ── Composition ────────────────────────────────────────────────

    def apply(self, query: Any, name: str | None) -> tuple[Any, Sorter]:
        """Resolve and apply the sort, returning ``(query, sorter)``.

        The caller often wants the resolved sorter back (e.g. to
        echo the canonical name in the response meta), which is
        why this returns a tuple instead of just the query.
        """
        sorter = self.resolve(name)
        return sorter.apply(query), sorter

    # ── Introspection ──────────────────────────────────────────────

    def names(self) -> list[str]:
        """Canonical names in declaration order (no aliases)."""
        return [s.name for s in self._sorters]

    def all_names(self) -> list[str]:
        """Canonical names + aliases. Used to build the ``in:`` rule."""
        out: list[str] = []
        for s in self._sorters:
            out.append(s.name)
            out.extend(s.aliases)
        return out

    def validation_rule(self) -> str:
        """Cara FormRequest rule string for ``sort_by`` payload key.

        Auto-generated from the registry so a new sorter dimension
        never has to coordinate with the FormRequest manually.
        """
        return f"nullable|string|in:{','.join(self.all_names())}"

    def describe(self) -> dict[str, Any]:
        """JSON-serialisable spec for the wizard."""
        return {
            "name": "sort_by",
            "label": "Sort by",
            "default": self._default.name,
            "options": [s.describe() for s in self._sorters],
        }
