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

        builder.set_global_scope(
            "_timestamp_update",
            self.set_timestamp_update,
            action="update",
        )

    def on_remove(self, builder):
        pass

    def set_timestamp(owner_cls, query):
        # Use UTC timestamp instead of "now" to avoid database timezone issues
        import pendulum

        owner_cls.updated_at = pendulum.now("UTC").to_datetime_string()

    def set_timestamp_create(self, builder):
        if not builder._model.__timestamps__:
            return builder

        # Use model's cast system for timestamp values
        model = builder._model
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

        builder._creates.update(
            {
                model.date_updated_at: updated_timestamp_value,
                model.date_created_at: timestamp_value,
            }
        )

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
