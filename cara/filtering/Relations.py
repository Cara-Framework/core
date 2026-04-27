"""``RelationSet`` — composable, named eager-load preset.

Before this module, every list endpoint hand-rolled the same
eager-load tuple:

::

    Product.active().with_(["images", "current_price", "container"])
    Product.active().with_(["images", "current_price", "container", "details", "videos"])
    Product.active().with_(["images", "current_price"])

Twenty-plus call sites, three subtly different shapes. Adding a
relation to "the card payload" required a global grep + edit; the
admin browser loaded a different shape than the storefront for no
business reason; ``BrowsingHistoryRepository`` hard-coded its own
list and silently fell behind when ``current_price`` was added.

A ``RelationSet`` solves the same problem ``FilterSet`` solves for
filters: name a bundle, compose by inheritance (``with_``,
``without``, ``only``), and consume from one canonical place.

Pattern
-------

::

    from app.filtering import RelationSet

    # Storefront product card — minimum needed for ProductResource
    # to render an image, a price, and a container link.
    PRODUCT_CARD_RELATIONS = RelationSet(
        "images", "current_price", "container",
        name="product.card",
    )

    # Storefront product detail — card + the heavy fields needed
    # for the show / detail endpoint.
    PRODUCT_DETAIL_RELATIONS = PRODUCT_CARD_RELATIONS.with_(
        "details", "videos",
        name="product.detail",
    )

A ``RelationSet`` IS a ``tuple`` (subclass) so it Just Works
everywhere a sequence of relation names is expected — Cara's
``Model.with_(*rels)`` flattens it automatically because its
``isinstance(_, (list, tuple))`` branch matches. ``FilterPipeline.with_``
also accepts it directly. Adopt the canonical preset and let
``RelationSet`` own the membership.
"""

from __future__ import annotations

from typing import Tuple


class RelationSet(tuple):
    """Ordered, name-unique tuple of eager-load relation names.

    Subclasses ``tuple`` so existing code that passes a list/tuple
    of relation names — e.g. ``Model.with_(rels)`` in Cara's
    Eloquent — flattens it automatically. The composition methods
    (``with_``, ``without``, ``only``) return new ``RelationSet``
    instances rather than mutating the source.
    """

    name: str

    def __new__(cls, *relations: str, name: str = "") -> "RelationSet":
        cleaned: list = []
        seen: set = set()
        for r in relations:
            if not r:
                continue
            if r in seen:
                # Dedup silently — a derived set built via
                # ``base.with_(*more)`` may legitimately overlap.
                continue
            seen.add(r)
            cleaned.append(r)
        instance = super().__new__(cls, cleaned)
        # ``tuple`` is immutable, but Python lets us assign extra
        # attributes on subclasses. ``name`` is purely descriptive
        # — a debug / wizard label — and never affects equality or
        # hashing (those flow from the tuple contents).
        instance.name = name
        return instance

    # ── Composition ────────────────────────────────────────────────

    def with_(self, *relations: str, name: str = "") -> "RelationSet":
        """Return a new set with extra relations appended.

        Duplicates are silently dropped — the canonical order of
        the base set is preserved, with new entries appended at the
        end (only if not already present).

        Args:
            *relations: Relation names to append.
            name: Optional name for the derived set. Empty by
                default; callers usually pass a descriptive label
                ("product.detail") so debug output and the wizard
                schema can identify which preset is in play.
        """
        return RelationSet(*self, *relations, name=name)

    def without(self, *drop: str, name: str = "") -> "RelationSet":
        """Return a new set with the named relations removed.

        Used for endpoint-specific trimming (a skinny widget that
        doesn't render videos can declare
        ``PRODUCT_DETAIL_RELATIONS.without("videos")``).
        """
        skip = set(drop)
        return RelationSet(
            *(r for r in self if r not in skip),
            name=name,
        )

    def only(self, *keep: str, name: str = "") -> "RelationSet":
        """Return a new set keeping ONLY the named relations.

        Order matches the canonical preset, not ``keep``'s order —
        the canonical preset always defines the order, callers just
        prune.
        """
        keep_set = set(keep)
        return RelationSet(
            *(r for r in self if r in keep_set),
            name=name,
        )

    # ── Conversion ─────────────────────────────────────────────────

    def to_tuple(self) -> Tuple[str, ...]:
        """Materialise as a plain ``tuple`` (for callers that want a non-RelationSet)."""
        return tuple(self)

    def to_list(self) -> list:
        """Materialise as a fresh list — handy for ``builder.with_(rel.to_list())``."""
        return list(self)

    def describe(self) -> dict:
        """JSON-serialisable spec — used by the wizard / docs.

        Mirrors ``FilterSet.describe()`` / ``SortRegistry.describe()``
        so a future "endpoint contract" introspection endpoint can
        emit one schema covering filter, sort, and eager-load
        dimensions in one shape.
        """
        return {
            "name": self.name,
            "relations": list(self),
            "count": len(self),
        }

    def __repr__(self) -> str:
        label = f" name={self.name!r}" if self.name else ""
        return f"<RelationSet{label} {list(self)}>"


def relations(*items: str, name: str = "") -> RelationSet:
    """Sugar constructor — ``relations("images", "current_price")``.

    Equivalent to the constructor; exists so call sites in
    ``sets.py`` read like a free function instead of a class
    instantiation.
    """
    return RelationSet(*items, name=name)


__all__ = ["RelationSet", "relations"]
