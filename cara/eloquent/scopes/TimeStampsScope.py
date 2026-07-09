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

        # bulk_create runs its own scope action (same registration split
        # UUIDPrimaryKeyScope uses) — without it, bulk-inserted rows never
        # got created_at/updated_at auto-filled.
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

    def _timestamp_values(self, model):
        """Compute (created_at, updated_at) values through the cast system."""
        timestamp_value = model.get_new_date().to_datetime_string()

        # Apply cast to timestamp values if casts are defined
        if model.date_created_at in model.__casts__:
            timestamp_value = model._set_cast_attribute(
                model.date_created_at, timestamp_value
            )

        updated_timestamp_value = timestamp_value
        if model.date_updated_at in model.__casts__:
            updated_timestamp_value = model._set_cast_attribute(
                model.date_updated_at, timestamp_value
            )

        return timestamp_value, updated_timestamp_value

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
        # provided timestamps (seeders, scrape imports) are respected —
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
        timestamp_value = model.get_new_date().to_datetime_string()

        # Apply cast to timestamp value if cast is defined
        if model.date_updated_at in model.__casts__:
            timestamp_value = model._set_cast_attribute(
                model.date_updated_at, timestamp_value
            )

        builder._updates += (
            UpdateQueryExpression({model.date_updated_at: timestamp_value}),
        )
