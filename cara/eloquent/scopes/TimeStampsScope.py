from __future__ import annotations

from cara.eloquent.expressions import UpdateQueryExpression

from .BaseScope import BaseScope


class TimeStampsScope(BaseScope):
    """Global scope class to add soft deleting to models."""

    def on_boot(self, builder):
        builder.set_global_scope(
            "_timestamps",
            self.set_timestamp_create,
            action="insert",
        )

        # bulk_create runs its own scope action; without it, bulk-inserted
        # rows never get created_at/updated_at auto-filled.
        builder.set_global_scope(
            "_timestamps_bulk",
            self.set_timestamp_bulk_create,
            action="bulk_create",
        )

        builder.set_global_scope(
            "_timestamp_update",
            self.set_timestamp_update,
            action="update",
        )

    def on_remove(self, builder):
        """No cleanup needed when timestamps scope is removed."""

    @staticmethod
    def _stamp(model, column, now):
        """Render ``now`` for ``column``, keeping the instant intact.

        The value handed to the cast MUST stay tz-aware: ``DateTimeCast``
        refuses a naive write (``NaiveDateTimeException``). Stringifying
        first with ``to_datetime_string()`` dropped the offset — once read
        back in APP_TIMEZONE, which stamped a Europe/Madrid product's
        created_at two hours in the past; today it would be refused.

        Without a cast the value goes straight into the builder, so it is
        rendered ISO-8601 WITH the offset: a naive literal into a TIMESTAMPTZ
        column is resolved by PostgreSQL against the session ``TimeZone``
        GUC, which the framework does not assert.
        """
        if column in model.__casts__:
            return model._set_cast_attribute(column, now)

        return now.to_iso8601_string()

    def _timestamp_values(self, model):
        """Compute (created_at, updated_at) values through the cast system.

        Both columns are rendered from the SAME aware instant. Deriving
        updated_at from the already-cast created_at value fed a naive UTC
        string back through ``DateTimeCast.set``, shifting updated_at by the
        app-timezone offset a second time.
        """
        now = model.get_new_date()

        return (
            self._stamp(model, model.date_created_at, now),
            self._stamp(model, model.date_updated_at, now),
        )

    def set_timestamp_create(self, builder):
        if not builder._model.__timestamps__:
            return builder

        model = builder._model
        timestamp_value, updated_timestamp_value = self._timestamp_values(model)

        # The same rule as ``set_timestamp_bulk_create``: a row whose writer
        # STATES when it was born (a store that keeps its own clock) keeps
        # that instant; only an absent/None column is stamped now.
        if builder._creates.get(model.date_created_at) is None:
            builder._creates[model.date_created_at] = timestamp_value
        if builder._creates.get(model.date_updated_at) is None:
            builder._creates[model.date_updated_at] = updated_timestamp_value

    def set_timestamp_bulk_create(self, builder):
        if not builder._model.__timestamps__:
            return builder

        model = builder._model
        timestamp_value, updated_timestamp_value = self._timestamp_values(model)

        # ``_creates`` is a list of canonicalized rows here. Explicitly
        # provided timestamps (seeders, bulk imports) are respected —
        # only absent/None columns are stamped.
        for row in builder._creates:
            if row.get(model.date_created_at) is None:
                row[model.date_created_at] = timestamp_value
            if row.get(model.date_updated_at) is None:
                row[model.date_updated_at] = updated_timestamp_value

    def set_timestamp_update(self, builder):
        if not builder._model.__timestamps__:
            return builder

        for update in builder._updates:
            # A dict update names its columns as keys; a counter update
            # (increment / decrement) names ONE column as a string — where
            # ``in`` is a substring test, so ``updated_count`` read as "this
            # write already stamps ``updated``" and the row kept its old
            # timestamp.
            column = update.column
            named = column if isinstance(column, dict) else (column,)
            if builder._model.date_updated_at in named:
                return

        # Use model's cast system for timestamp values
        model = builder._model
        timestamp_value = self._stamp(model, model.date_updated_at, model.get_new_date())

        builder._updates += (
            UpdateQueryExpression({model.date_updated_at: timestamp_value}),
        )
