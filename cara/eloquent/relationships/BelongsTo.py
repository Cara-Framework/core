from __future__ import annotations

from cara.support import Collection

from .BaseRelationship import BaseRelationship


class BelongsTo(BaseRelationship):
    """
    Belongs To Relationship.

    Works as both decorator and property (Laravel-style).
    """

    def __call__(self, func):
        """Decorator: Store the function and return self."""
        self._func = func
        return self

    def _resolve(self, instance):
        """Laravel behavior: ``model.user`` is a Model instance or None.

        A NULL foreign key short-circuits before the query — there is no
        related record to look for.
        """
        local_value = instance.__attributes__.get(self.local_key)
        if local_value is None:
            return None

        return self.get_builder().where(self.foreign_key, local_value).first()

    def __init__(self, fn, local_key=None, foreign_key=None):
        if isinstance(fn, str):
            self.fn = None
            self.local_key = fn or "id"
            self.foreign_key = local_key
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
        local_value = getattr(model, self.local_key, None)
        if local_value is None:
            # A nullable foreign key has no related record.  The lazy-loading
            # path already returns None without querying; eager loading must
            # preserve the same contract instead of attempting to use None as
            # an index on an empty, list-backed Collection.
            model.add_relation({key: None})
            return

        related = collection.get(local_value, None)

        model.add_relation({key: related[0] if related else None})

    def map_related(self, related_result):
        return related_result.group_by(self.foreign_key)

    def attach(self, current_model, related_record):
        foreign_key_value = getattr(related_record, self.foreign_key)
        if not current_model.is_created():
            current_model.fill({self.local_key: foreign_key_value})
            return current_model.create(current_model.all_attributes(), cast=True)

        return current_model.update({self.local_key: foreign_key_value})

    def detach(self, current_model, related_record):
        return current_model.update({self.local_key: None})

    def relate(self, related_record):
        local_value = related_record.__attributes__.get(self.local_key)
        return (
            self.get_builder()
            .where(
                self.foreign_key,
                local_value,
            )
            ._set_creates_related({self.foreign_key: local_value})
        )
