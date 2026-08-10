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

        The value handed to the cast MUST stay tz-aware. Stringifying first
        with ``to_datetime_string()`` dropped the offset, and
        ``DateTimeCast.set`` then re-read that naive string as APP_TIMEZONE
        (its documented contract for product-supplied naive input) — so a
        product on Europe/Madrid stamped every created_at/updated_at two
        hours in the past, an instant that never happened. Passing the aware
        ``pendulum.DateTime`` takes the offset-preserving branch instead.

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

        builder._creates.update(
            {
                model.date_updated_at: updated_timestamp_value,
                model.date_created_at: timestamp_value,
            }
        )

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
            if builder._model.date_updated_at in update.column:
                return

        # Use model's cast system for timestamp values
        model = builder._model
        timestamp_value = self._stamp(model, model.date_updated_at, model.get_new_date())

        builder._updates += (
            UpdateQueryExpression({model.date_updated_at: timestamp_value}),
        )
