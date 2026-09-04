from __future__ import annotations

from cara.support import Collection

from .BaseRelationship import BaseRelationship


class HasMany(BaseRelationship):
    """
    Has Many Relationship Class.

    Works as both decorator and property (Laravel-style).
    When accessed as property, returns self (relationship instance).
    """

    def __call__(self, func):
        """Decorator: Store the function and return self."""
        self._func = func
        return self

    def _resolve(self, instance):
        """Laravel behavior: ``model.posts`` is a Collection, not a builder."""
        return (
            self.get_builder()
            .where(
                self.foreign_key,
                instance.__attributes__[self.local_key],
            )
            .get()
        )

    def register_related(self, key, model, collection):
        model.add_relation(
            {key: collection.get(getattr(model, self.local_key)) or Collection()}
        )

    def map_related(self, related_result):
        return related_result.group_by(self.foreign_key)

    def get_related(self, query, relation, eagers=None, callback=None):
        eagers = eagers or []
        builder = self.get_builder().with_(eagers)

        if callback:
            callback(builder)
        if isinstance(relation, Collection):
            return builder.where_in(
                f"{builder.get_table_name()}.{self.foreign_key}",
                Collection(relation._get_value(self.local_key)).unique(),
            ).get()

        return builder.where(
            f"{builder.get_table_name()}.{self.foreign_key}",
            getattr(relation, self.local_key),
        ).get()
