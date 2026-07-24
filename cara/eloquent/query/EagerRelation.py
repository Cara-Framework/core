from __future__ import annotations

from typing import Self

"""
EagerRelations - Simple eager loading component
"""


class EagerRelations:
    """Complete eager relations management with all required attributes."""

    def __init__(self):
        self.relations = []
        self.nested_eagers = {}  # 🔧 For nested eager loading
        self.callback_eagers = {}  # 🔧 For callback-based eager loading

    def register(self, *relations) -> Self:
        """Register relations for eager loading (Laravel-style with *args)."""
        try:
            for relation in relations:
                if isinstance(relation, str):
                    self.with_relation(relation)
                elif isinstance(relation, (list, tuple)):
                    # Handle list/tuple of relations
                    for rel in relation:
                        if isinstance(rel, str):
                            self.with_relation(rel)
                elif isinstance(relation, dict):
                    # Handle nested relations with callbacks
                    for rel_name, callback in relation.items():
                        self.with_relation(rel_name)
                        if callable(callback):
                            self.with_callback(rel_name, callback)
        except Exception as e:
            from cara.facades import Log

            Log.error("Error in register: %s", str(e))
            raise
        return self

    def with_relation(self, relation: str) -> Self:
        """Add a relation to eager load."""
        self.relations.append(relation)

        # Handle nested relations (e.g., "posts.comments")
        if "." in relation:
            parts = relation.split(".")
            self.nested_eagers[parts[0]] = parts[1:]

        return self

    def with_callback(self, relation: str, callback) -> Self:
        """Add callback for eager loading."""
        self.callback_eagers[relation] = callback
        return self

    def get_relations(self):
        """Get all relations to load."""
        return self.relations.copy()
