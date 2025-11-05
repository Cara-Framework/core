from cara.support.Collection import Collection

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
    
    def __get__(self, instance, owner):
        """Property access: Return Collection like Laravel (execute query automatically)."""
        if instance is None:
            # Accessed from class, return self for eager loading
            return self
        # Accessed from instance, execute query and return Collection (Laravel-style)
        return self.apply_query(self.get_builder(), instance)

    def apply_query(self, foreign, owner):
        """
        Apply the query and return a dictionary to be hydrated.

        Arguments:
            foreign {oject} -- The relationship object
            owner {object} -- The current model oject.

        Returns:
            dict -- A dictionary of data which will be hydrated.
        """
        result = foreign.where(
            self.foreign_key,
            owner.__attributes__[self.local_key],
        ).get()

        return result

    def set_keys(self, owner, attribute):
        self.local_key = self.local_key or "id"
        self.foreign_key = self.foreign_key or f"{attribute}_id"
        return self

    def register_related(self, key, model, collection):
        model.add_relation(
            {key: collection.get(getattr(model, self.local_key)) or Collection()}
        )

    def map_related(self, related_result):
        return related_result.group_by(self.foreign_key)

    def attach(self, current_model, related_record):
        local_key_value = getattr(current_model, self.local_key)
        if not related_record.is_created():
            related_record.fill({self.foreign_key: local_key_value})
            return related_record.create(related_record.all_attributes(), cast=True)

        return related_record.update({self.foreign_key: local_key_value})

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
