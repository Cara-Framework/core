"""
HasRelationships Concern

Single Responsibility: Handle relationship operations for Eloquent models.
Clean separation of relationship logic from main model class.
"""

from typing import Any, Dict, List


class HasRelationships:
    """
    Mixin for handling model relationships.

    This concern handles:
    - Relationship loading and access
    - Eager loading
    - Relationship serialization
    - Relationship manipulation
    """

    def __init__(self, **kwargs):
        # Initialize relationship storage
        self.__dict__["_relations"] = {}
        self.__dict__["_with"] = ()
        super().__init__(**kwargs)

    # ===== Relationship Access =====

    def add_relation(self, relations: Dict[str, Any]) -> "HasRelationships":
        """Add loaded relationships to the model."""
        if not hasattr(self, "_relations"):
            self.__dict__["_relations"] = {}

        if isinstance(relations, dict):
            self._relations.update(relations)

        return self

    def get_related(self, relation: str) -> Any:
        """Get a loaded relationship."""
        return self._relations.get(relation)

    def related(self, relation: str) -> Any:
        """Alias for get_related."""
        return self.get_related(relation)

    def set_relation(self, relation: str, value: Any) -> "HasRelationships":
        """Set a relationship value."""
        if not hasattr(self, "_relations"):
            self.__dict__["_relations"] = {}

        self._relations[relation] = value
        return self

    def unset_relation(self, relation: str) -> "HasRelationships":
        """Remove a relationship."""
        if hasattr(self, "_relations") and relation in self._relations:
            del self._relations[relation]

        return self

    def is_relation_loaded(self, relation: str) -> bool:
        """Check if a relationship is loaded."""
        return hasattr(self, "_relations") and relation in self._relations

    # ===== Relationship Manipulation =====

    def attach(self, relation: str, related_record: Any) -> bool:
        """Attach a related record to a relationship."""
        if hasattr(self.__class__, relation):
            relationship = getattr(self.__class__, relation)
            if hasattr(relationship, "attach"):
                return relationship.attach(self, related_record)

        return False

    def detach(self, relation: str, related_record: Any) -> bool:
        """Detach a related record from a relationship."""
        if hasattr(self.__class__, relation):
            relationship = getattr(self.__class__, relation)
            if hasattr(relationship, "detach"):
                return relationship.detach(self, related_record)

        return False

    def save_many(self, relation: str, relating_records: List[Any]) -> bool:
        """Save multiple related records."""
        success = True

        for record in relating_records:
            if not self.attach(relation, record):
                success = False

        return success

    def detach_many(self, relation: str, relating_records: List[Any]) -> bool:
        """Detach multiple related records."""
        success = True

        for record in relating_records:
            if not self.detach(relation, record):
                success = False

        return success

    def attach_related(self, relation: str, related_record: Any) -> bool:
        """Attach and save a related record."""
        if hasattr(self.__class__, relation):
            relationship = getattr(self.__class__, relation)
            if hasattr(relationship, "attach_related"):
                return relationship.attach_related(self, related_record)

        return False

    # ===== Eager Loading =====

    @classmethod
    def with_(cls, *relations):
        """Laravel-style with() for eager loading relationships."""
        return cls.query().with_(*relations)

    @classmethod
    def with_and_load(cls, *relations):
        """Load all records with specified relationships."""
        return cls.query().with_(*relations).get()

    @classmethod
    def load(cls, *loads):
        """Alias for with_and_load for Laravel compatibility."""
        return cls.query().with_(*loads)

    @classmethod
    def load_missing(cls, *loads):
        """Load missing relationships on already loaded models."""
        return cls.query().with_(*loads)

    # ===== Serialization Support =====

    def _serialize_relations(self) -> Dict[str, Any]:
        """Serialize loaded relationships."""
        if not hasattr(self, "_relations"):
            return {}

        result = {}

        for relation_name, relation_value in self._relations.items():
            if relation_value is None:
                result[relation_name] = None
            elif hasattr(relation_value, "to_array"):
                # Single model
                result[relation_name] = relation_value.to_array()
            elif hasattr(relation_value, "__iter__"):
                # Collection of models
                serialized_items = []
                for item in relation_value:
                    if hasattr(item, "to_array"):
                        serialized_items.append(item.to_array())
                    else:
                        serialized_items.append(item)
                result[relation_name] = serialized_items
            else:
                result[relation_name] = relation_value

        return result

    def relations_to_dict(self) -> Dict[str, Any]:
        """Convert relationships to dictionary."""
        return self._serialize_relations()

    def without_relations(self) -> Dict[str, Any]:
        """Get model data without relationships."""
        # This will use the parent to_array but exclude relations
        data = super().to_array() if hasattr(super(), "to_array") else {}

        # Remove any relationship data
        if hasattr(self, "_relations"):
            for relation_name in self._relations.keys():
                data.pop(relation_name, None)

        return data

    def with_relations(self, *relations) -> "HasRelationships":
        """Include specific relations in serialization."""
        # This would be used to control which relations are serialized
        clone = (
            self._clone_for_visibility()
            if hasattr(self, "_clone_for_visibility")
            else self
        )
        clone.__dict__["_serialize_relations_only"] = set(relations)
        return clone

    def without_relation(self, *relations) -> "HasRelationships":
        """Exclude specific relations from serialization."""
        clone = (
            self._clone_for_visibility()
            if hasattr(self, "_clone_for_visibility")
            else self
        )
        clone.__dict__["_serialize_relations_except"] = set(relations)
        return clone

    # ===== Helper Methods =====

    def get_relationship_names(self) -> List[str]:
        """Get all relationship names defined on the model."""
        relationships = []

        for attr_name in dir(self.__class__):
            attr = getattr(self.__class__, attr_name)
            if hasattr(attr, "__call__") and hasattr(attr, "get_related"):
                relationships.append(attr_name)

        return relationships

    def has_relationship(self, relation: str) -> bool:
        """Check if a relationship is defined on the model."""
        return relation in self.get_relationship_names()

    def refresh_relation(self, relation: str) -> "HasRelationships":
        """Refresh a specific relationship."""
        if self.has_relationship(relation):
            # Remove the cached relationship
            self.unset_relation(relation)
            # Access it again to reload it
            getattr(self, relation)

        return self

    def refresh_relations(self, *relations) -> "HasRelationships":
        """Refresh multiple relationships."""
        if not relations:
            # Refresh all loaded relations
            relations = (
                list(self._relations.keys()) if hasattr(self, "_relations") else []
            )

        for relation in relations:
            self.refresh_relation(relation)

        return self
