"""
HasRelationships Concern

Single Responsibility: Handle relationship operations for Eloquent models.
Clean separation of relationship logic from main model class.
"""

from __future__ import annotations

from typing import Any, Self


class HasRelationships:
    """
    Mixin for handling model relationships.

    ``Model`` — this concern's only consumer — rebinds attach/detach,
    save_many/detach_many, attach_related, get_related and
    relations_to_dict to the ``_ModelRelations`` / ``_ModelData``
    implementations, so the copies that used to live here never ran. What
    remains is the relation cache API plus the two eager-loading entry
    points ``Model`` does NOT rebind.
    """

    # ===== Relationship Access =====

    def add_relation(self, relations: dict[str, Any]) -> HasRelationships:
        """Add loaded relationships to the model."""
        if not hasattr(self, "_relations"):
            self.__dict__["_relations"] = {}

        if isinstance(relations, dict):
            self._relations.update(relations)

        return self

    def set_relation(self, relation: str, value: Any) -> HasRelationships:
        """Set a relationship value."""
        if not hasattr(self, "_relations"):
            self.__dict__["_relations"] = {}

        self._relations[relation] = value
        return self

    def unset_relation(self, relation: str) -> HasRelationships:
        """Remove a relationship."""
        if hasattr(self, "_relations") and relation in self._relations:
            del self._relations[relation]

        return self

    def is_relation_loaded(self, relation: str) -> bool:
        """Check if a relationship is loaded."""
        return hasattr(self, "_relations") and relation in self._relations

    # ===== Eager Loading =====

    @classmethod
    def with_(cls, *relations):
        """Laravel-style with() for eager loading relationships."""
        instance = cls()
        return instance.query().with_(*relations)

    def load(self, *relations) -> Self:
        """
        Lazy eager-load relationships on an already-fetched model instance.

        Laravel parity::

            p = Model.find(1)
            p.load("author", "tags", "parent.owner")

        Accepts dotted nested relations and list/dict specs (same shapes as
        ``Model.with_()``). Results are merged into ``self._relations`` so
        subsequent attribute access returns the cached value.
        """
        if not relations:
            return self

        # Re-query the model alone, but attach the requested eagers. We
        # don't care about the record itself — the query builder populates
        # ``_relations`` on the hydrated model via the eager-load pipeline,
        # and we copy those back onto ``self``.
        primary = self.get_primary_key() if hasattr(self, "get_primary_key") else "id"
        value = getattr(self, primary, None)
        if value is None:
            return self

        reloaded = self.__class__.query().with_(*relations).where(primary, value).first()
        if reloaded is not None:
            loaded = getattr(reloaded, "_relations", {}) or {}
            for name, val in loaded.items():
                self.set_relation(name, val)
        return self
