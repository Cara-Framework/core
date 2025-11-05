"""
EagerRelations - Simple eager loading component
"""


class EagerRelations:
    """Complete eager relations management with all required attributes."""

    def __init__(self):
        self.relations = []
        self.eagers = []  # 🔧 For compatibility
        self.nested_eagers = {}  # 🔧 For nested eager loading
        self.eager_constraints = {}  # 🔧 For eager loading constraints
        self.eager_counts = []  # 🔧 For counting eager loads
        self.callback_eagers = {}  # 🔧 For callback-based eager loading

    def register(self, *relations):
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

            Log.error(f"Error in register: {str(e)}")
            raise e
        return self

    def with_relation(self, relation: str):
        """Add a relation to eager load."""
        self.relations.append(relation)
        self.eagers.append(relation)

        # Handle nested relations (e.g., "posts.comments")
        if "." in relation:
            parts = relation.split(".")
            self.nested_eagers[parts[0]] = parts[1:]

        return self

    def with_count(self, relation: str):
        """Add a relation to count."""
        self.eager_counts.append(relation)
        return self

    def with_constraint(self, relation: str, constraint):
        """Add constraint to eager loading."""
        self.eager_constraints[relation] = constraint
        return self

    def with_callback(self, relation: str, callback):
        """Add callback for eager loading."""
        self.callback_eagers[relation] = callback
        return self

    def get_relations(self):
        """Get all relations to load."""
        return self.relations.copy()

    def get_eagers(self):
        """Get eager relations (alias for compatibility)."""
        return self.eagers.copy()

    def get_nested_eagers(self):
        """Get nested eager relations."""
        return self.nested_eagers.copy()

    def get_eager_counts(self):
        """Get relations to count."""
        return self.eager_counts.copy()

    def get_constraints(self):
        """Get eager loading constraints."""
        return self.eager_constraints.copy()

    def has_relations(self):
        """Check if there are relations to load."""
        return len(self.relations) > 0

    def has_eagers(self):
        """Check if there are eager relations (alias for compatibility)."""
        return len(self.eagers) > 0

    def has_nested_eagers(self):
        """Check if there are nested eager relations."""
        return len(self.nested_eagers) > 0

    def has_counts(self):
        """Check if there are relations to count."""
        return len(self.eager_counts) > 0

    def reset(self):
        """Reset all relations."""
        self.relations = []
        self.eagers = []
        self.nested_eagers = {}
        self.eager_constraints = {}
        self.eager_counts = []
        self.callback_eagers = {}
        return self
