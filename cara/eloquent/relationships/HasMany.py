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
        """Property access: return eager-loaded Collection or lazy-load and cache.

        Laravel behavior: accessing $model->posts returns a Collection directly,
        not a QueryBuilder. The result is cached in _relations for subsequent access.
        """
        if instance is None:
            return self

        func = getattr(self, '_func', None) or getattr(self, 'fn', None)
        attr_name = func.__name__ if func and hasattr(func, '__name__') else None

        # Return cached relation if already loaded (eager or previous lazy load)
        if attr_name:
            relations = getattr(instance, '_relations', None)
            if relations is not None and attr_name in relations:
                return relations[attr_name]

        # Lazy load: execute query and cache the result (Laravel behavior)
        builder = self.get_builder()
        result = builder.where(
            self.foreign_key,
            instance.__attributes__[self.local_key],
        ).get()

        # Cache in _relations so subsequent access doesn't re-query
        if attr_name:
            if not hasattr(instance, '_relations') or instance._relations is None:
                instance.__dict__.setdefault('_relations', {})
            instance._relations[attr_name] = result

        return result

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

    def query_has(self, current_query_builder, method="where_exists"):
        """Exists-correlated subquery: `WHERE EXISTS (SELECT ... FROM related
        WHERE related.foreign_key = parent.local_key)`. Used by ``has`` /
        ``doesnt_have`` for boolean relation filtering.
        """
        related_builder = self.get_builder()
        getattr(current_query_builder, method)(
            related_builder.where_column(
                f"{related_builder.get_table_name()}.{self.foreign_key}",
                f"{current_query_builder.get_table_name()}.{self.local_key}",
            )
        )
        return related_builder

    def query_where_exists(self, builder, callback, method="where_exists"):
        """Same shape as ``query_has`` but invokes the caller's callback so
        they can add extra constraints. Used by ``where_has`` family.
        """
        query = self.get_builder()
        getattr(builder, method)(
            callback(
                query.where_column(
                    f"{query.get_table_name()}.{self.foreign_key}",
                    f"{builder.get_table_name()}.{self.local_key}",
                )
            )
        )
        return query

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

    # ===== Aggregate Subquery Support (withCount, withSum, withAvg, withMin, withMax) =====
    #
    # Laravel-style correlated subqueries: SELECT (SELECT COUNT(*) FROM <related>
    # WHERE <related>.<foreign_key> = <parent>.<local_key>) AS <alias>, ...
    # ``callback`` (if given) receives the inner subquery for extra constraints.

    def _aggregate_subquery(self, builder, alias, agg_fn, callback):
        related_table = self.get_builder().get_table_name()
        if not builder._columns:
            builder = builder.select("*")
        # Laravel parity: build the correlated subquery from the RELATED
        # model's own query (so its global scopes apply, not the parent's).
        def _make_sub(_unused_new):
            sub = self.get_builder()
            return (
                agg_fn(sub)
                .where_column(
                    f"{related_table}.{self.foreign_key}",
                    f"{builder.get_table_name()}.{self.local_key}",
                )
                .when(callback, lambda qq: callback(qq))
            )
        return builder.add_select(alias, _make_sub)

    def _alias_base(self, relation_name):
        """Laravel uses the relation name for alias (e.g. ``images_count``).
        Fall back to the related table name when no relation name is given."""
        return relation_name or self.get_builder().get_table_name()

    def get_with_count_query(self, builder, callback=None, relation_name=None):
        base = self._alias_base(relation_name)
        return self._aggregate_subquery(
            builder,
            f"{base}_count",
            lambda q: q.count("*", dry=True),
            callback,
        )

    def get_with_sum_query(self, builder, column, callback=None, relation_name=None):
        base = self._alias_base(relation_name)
        related_table = self.get_builder().get_table_name()
        return self._aggregate_subquery(
            builder,
            f"{base}_{column}_sum",
            lambda q: q.sum(f"{related_table}.{column}", dry=True),
            callback,
        )

    def get_with_avg_query(self, builder, column, callback=None, relation_name=None):
        base = self._alias_base(relation_name)
        related_table = self.get_builder().get_table_name()
        return self._aggregate_subquery(
            builder,
            f"{base}_{column}_avg",
            lambda q: q.avg(f"{related_table}.{column}", dry=True),
            callback,
        )

    def get_with_min_query(self, builder, column, callback=None, relation_name=None):
        base = self._alias_base(relation_name)
        related_table = self.get_builder().get_table_name()
        return self._aggregate_subquery(
            builder,
            f"{base}_{column}_min",
            lambda q: q.min(f"{related_table}.{column}", dry=True),
            callback,
        )

    def get_with_max_query(self, builder, column, callback=None, relation_name=None):
        base = self._alias_base(relation_name)
        related_table = self.get_builder().get_table_name()
        return self._aggregate_subquery(
            builder,
            f"{base}_{column}_max",
            lambda q: q.max(f"{related_table}.{column}", dry=True),
            callback,
        )
