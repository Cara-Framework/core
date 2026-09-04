from __future__ import annotations

from cara.support import Collection

from .BaseRelationship import BaseRelationship


class HasOne(BaseRelationship):
    """
    Has One Relationship.

    Works as both decorator and property (Laravel-style).
    """

    def __call__(self, func):
        """Decorator: Store the function and return self."""
        self._func = func
        return self

    def _resolve(self, instance):
        """Laravel behavior: ``model.phone`` is a Model instance or None."""
        return (
            self.get_builder()
            .where(
                self.foreign_key,
                instance.__attributes__[self.local_key],
            )
            .first()
        )

    def __init__(self, fn, foreign_key=None, local_key=None):
        if isinstance(fn, str):
            # When called as @has_one("foreign_key", "local_key")
            self.fn = None
            self.foreign_key = fn
            self.local_key = foreign_key
        else:
            self.fn = fn
            self.local_key = local_key or "id"
            self.foreign_key = foreign_key

    def get_related(self, query, relation, eagers=(), callback=None):
        """
        Gets the relation needed between the relation and the related builder. If the relation is a
        collection then will need to pluck out all the keys from the collection and fetch from the
        related builder. If relation is just a Model then we can just call the model based on the
        value of the related builders primary key.

        Args:
            relation (Model|Collection):

        Returns:
            Model|Collection
        """
        builder = self.get_builder().with_(eagers)

        if callback:
            callback(builder)

        if isinstance(relation, Collection):
            return builder.where_in(
                f"{builder.get_table_name()}.{self.foreign_key}",
                Collection(relation._get_value(self.local_key)).unique(),
            ).get()
        else:
            return builder.where(
                f"{builder.get_table_name()}.{self.foreign_key}",
                getattr(relation, self.local_key),
            ).first()

    def register_related(self, key, model, collection):
        related = collection.where(
            self.foreign_key, getattr(model, self.local_key)
        ).first()

        model.add_relation({key: related or None})

    def map_related(self, related_result):
        return related_result

    def detach(self, current_model, related_record):
        return related_record.update({self.foreign_key: None})
