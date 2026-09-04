from __future__ import annotations


class BaseRelationship:
    def __init__(self, fn, local_key=None, foreign_key=None):
        if isinstance(fn, str):
            self.fn = None
            self.foreign_key = fn
            self.local_key = local_key
        else:
            self.fn = fn
            self.local_key = local_key
            self.foreign_key = foreign_key

    def __set_name__(self, cls, name):
        """
        This method is called right after the decorator is registered.

        At this point we finally have access to the model cls

        Arguments:
            name {object} -- The model class.
        """

    def __call__(self, fn=None, *args, **kwargs):
        """
        This method is called when the decorator contains arguments.

        When you do something like this:

        @belongs_to('id', 'user_id').

        In this case, the {fn} argument will be the callable.
        """
        if callable(fn):
            self.fn = fn

        return self

    def get_builder(self):
        """Get query builder for the related model.

        IMPORTANT: Always return a FRESH builder instance to avoid
        query condition accumulation across multiple calls.
        """
        # Get the related model class from the decorated function
        func = getattr(self, "_func", None) or getattr(self, "fn", None)
        if func:
            # Call the function to get the model class
            related_model = func(self)
            if related_model:
                # ALWAYS create a NEW query builder (don't cache)
                return related_model.query()

        raise AttributeError("Cannot get builder: related model not found")

    def __get__(self, instance, owner):
        """Property access: return the eager-loaded value, or lazy-load and cache.

        Laravel behavior: accessing ``model.relation`` returns a Model, a
        Collection or ``None`` — never a QueryBuilder. The result is cached in
        ``_relations`` so subsequent access does not re-query.

        The cache lookup, the strict lazy-load guard and the write-back are the
        same for every relationship kind; only the query differs, and each
        subclass supplies that through :meth:`_resolve`.
        """
        if instance is None:
            return self

        func = getattr(self, "_func", None) or getattr(self, "fn", None)
        attr_name = func.__name__ if func and hasattr(func, "__name__") else None

        # Return cached relation if already loaded (eager or previous lazy load)
        if attr_name:
            relations = getattr(instance, "_relations", None)
            if relations is not None and attr_name in relations:
                return relations[attr_name]

        # Strict lazy-load guard (opt-in, off by default): raise if this
        # un-eager-loaded relation is accessed on a collection-hydrated model.
        guard = getattr(instance, "_guard_against_lazy_load", None)
        if attr_name and callable(guard):
            guard(attr_name)

        result = self._resolve(instance)

        # Cache in _relations so subsequent access doesn't re-query
        if attr_name:
            if not hasattr(instance, "_relations") or instance._relations is None:
                instance.__dict__.setdefault("_relations", {})
            instance._relations[attr_name] = result

        return result

    def _resolve(self, instance):
        """Run this relationship's lazy-load query for ``instance``."""
        klass = self.__class__.__name__
        raise NotImplementedError(
            f"{klass} relationship does not implement the '_resolve' method"
        )

    def __getattr__(self, attribute):
        # Use _func if set by decorator, otherwise fn. Read via __dict__:
        # getattr(self, ...) would re-enter __getattr__ for the probe
        # names when they're unset (string-form init) and recurse forever.
        func = self.__dict__.get("_func") or self.__dict__.get("fn")
        if func:
            relationship = func(self)()
            return getattr(relationship.builder, attribute)
        raise AttributeError(
            f"'{type(self).__name__}' object has no attribute '{attribute}'"
        )

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

    def joins(self, builder, clause=None):
        """Helper method for adding join clauses to a relationship."""
        other_table = self.get_builder().get_table_name()
        local_table = builder.get_table_name()
        return builder.join(
            other_table,
            f"{local_table}.{self.local_key}",
            "=",
            f"{other_table}.{self.foreign_key}",
            clause=clause,
        )

    # ===== Aggregate Subquery Support (withCount, withSum, withAvg, withMin, withMax) =====
    #
    # Laravel-style correlated subqueries: SELECT (SELECT COUNT(*) FROM <related>
    # WHERE <related>.<foreign_key> = <parent>.<local_key>) AS <alias>, ...
    # ``callback`` (if given) receives the inner subquery for extra constraints.
    #
    # The key layout differs per relationship kind but the correlation is
    # written the same way: HasOne/HasMany store the parent's key in
    # ``local_key`` and the child's pointer in ``foreign_key``; BelongsTo
    # stores the FK in ``local_key`` and the related row identifies itself
    # via ``foreign_key`` (usually ``id``). Either way the predicate is
    # ``<related>.<foreign_key> = <parent>.<local_key>``.

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
        """Adds a clause to the query to get the record count of the relationship."""
        base = self._alias_base(relation_name)
        return self._aggregate_subquery(
            builder,
            f"{base}_count",
            lambda q: q.count("*", dry=True),
            callback,
        )

    def get_with_sum_query(self, builder, column, callback=None, relation_name=None):
        """Adds a clause to the query to get the sum of a column in the relationship."""
        base = self._alias_base(relation_name)
        related_table = self.get_builder().get_table_name()
        return self._aggregate_subquery(
            builder,
            f"{base}_{column}_sum",
            lambda q: q.sum(f"{related_table}.{column}", dry=True),
            callback,
        )

    def get_with_avg_query(self, builder, column, callback=None, relation_name=None):
        """Adds a clause to the query to get the average of a column in the relationship."""
        base = self._alias_base(relation_name)
        related_table = self.get_builder().get_table_name()
        return self._aggregate_subquery(
            builder,
            f"{base}_{column}_avg",
            lambda q: q.avg(f"{related_table}.{column}", dry=True),
            callback,
        )

    def get_with_min_query(self, builder, column, callback=None, relation_name=None):
        """Adds a clause to the query to get the minimum of a column in the relationship."""
        base = self._alias_base(relation_name)
        related_table = self.get_builder().get_table_name()
        return self._aggregate_subquery(
            builder,
            f"{base}_{column}_min",
            lambda q: q.min(f"{related_table}.{column}", dry=True),
            callback,
        )

    def get_with_max_query(self, builder, column, callback=None, relation_name=None):
        """Adds a clause to the query to get the maximum of a column in the relationship."""
        base = self._alias_base(relation_name)
        related_table = self.get_builder().get_table_name()
        return self._aggregate_subquery(
            builder,
            f"{base}_{column}_max",
            lambda q: q.max(f"{related_table}.{column}", dry=True),
            callback,
        )

    def attach(self, current_model, related_record):
        """Link a related model to the current model.

        The child-owns-the-key layout (HasOne / HasMany): stamp the parent's
        ``local_key`` value onto the related record's ``foreign_key``.
        ``BelongsTo`` overrides this — there the PARENT holds the key.
        """
        local_key_value = getattr(current_model, self.local_key)
        if not related_record.is_created():
            related_record.fill({self.foreign_key: local_key_value})
            return related_record.create(related_record.all_attributes(), cast=True)

        return related_record.update({self.foreign_key: local_key_value})

    def get_related(self, query, relation, eagers=None, callback=None):
        klass = self.__class__.__name__
        raise NotImplementedError(
            f"{klass} relationship does not implement the 'get_related' method"
        )

    def relate(self, related_record):
        klass = self.__class__.__name__
        raise NotImplementedError(
            f"{klass} relationship does not implement the 'relate' method"
        )

    def detach(self, current_model, related_record):
        """Unlink a related model from the current model."""
        klass = self.__class__.__name__
        raise NotImplementedError(
            f"{klass} relationship does not implement the 'detach' method"
        )

    def attach_related(self, current_model, related_record):
        """Link a related model to the current model."""
        klass = self.__class__.__name__
        raise NotImplementedError(
            f"{klass} relationship does not implement the 'attach_related' method"
        )

    def map_related(self, related_result):
        klass = self.__class__.__name__
        raise NotImplementedError(
            f"{klass} relationship does not implement the 'map_related' method"
        )
