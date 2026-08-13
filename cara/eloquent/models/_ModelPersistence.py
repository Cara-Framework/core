"""Initialization, identity, lifecycle and core persistence composed into ``Model``."""

from __future__ import annotations

import inspect
import logging
from typing import Any, Self

from inflection import tableize, underscore

from cara.decorators.Events import _get_model_events
from cara.exceptions import ModelNotFoundException
from cara.facades import DB, Log

from ..query import QueryBuilder

_logger = logging.getLogger("cara.eloquent.models")
Model: type


def _bind_model(model_type: type) -> None:
    global Model
    Model = model_type


def _model_get_primary_key(cls) -> str:
    """Get the primary key column name."""
    return cls.__primary_key__


def _model_get_primary_key_type(self) -> str:
    """
    Gets the primary key column type.

    Returns:
        mixed
    """
    return self.__primary_key_type__


def _model_get_primary_key_value(self) -> Any:
    """
    Gets the primary key value.

    Raises:
        AttributeError: Raises attribute error if the model does not have an
            attribute with the primary key.

    Returns:
        str|int
    """
    try:
        return getattr(self, self.get_primary_key())
    except AttributeError:
        name = self.__class__.__name__
        raise AttributeError(
            f"class '{name}' has no attribute {self.get_primary_key()}. Did you set the primary key correctly on the model using the __primary_key__ attribute?"
        )


def _model_get_foreign_key(self) -> str:
    """
    Gets the foreign key based on this model name.

    Args:
        relationship (str): The relationship name.

    Returns:
        str
    """
    return underscore(f"{self.__class__.__name__}_{self.get_primary_key()}")


def _model_get_builder(self) -> Any:
    if hasattr(self, "builder"):
        return self.builder

    self.builder = QueryBuilder(
        connection=self.__connection__,
        table=self.get_table_name(),
        connection_details=self.get_connection_details(),
        model=self,
        scopes=self._scopes.get(self.__class__),
        dry=self.__dry__,
    )

    return self.builder


def _model_get_selects(self) -> list[str]:
    return self.__selects__


def _model_get_columns(cls) -> list[str]:
    row = cls.first()
    return list(row.__attributes__.keys()) if row else []


def _model_get_connection_details(self) -> dict[str, Any]:

    return DB.get_connection_details()


def _model_boot(self) -> None:
    if not self._booted:
        self.observe_events(self, "booting")
        for base_class in inspect.getmro(self.__class__):
            class_name = base_class.__name__

            if class_name.startswith("Makes"):
                getattr(self, f"boot_{class_name}")(self.get_builder())
            elif (
                base_class != Model
                and issubclass(base_class, Model)
                and "__fillable__" in base_class.__dict__
                and "__guarded__" in base_class.__dict__
            ):
                raise AttributeError(
                    f"{type(self).__name__} must specify either __fillable__ or __guarded__ properties, but not both."
                )

        self._booted = True
        self.observe_events(self, "booted")

        self.append_passthrough(list(self.get_builder()._macros.keys()))

        # Persist the global scopes + macros that the ``boot_Makes*`` hooks
        # wired onto the boot-time builder back onto the model instance.
        # ``QueryBuilder.__init__`` seeds every NEW builder from
        # ``model._global_scopes`` / ``model._macros``, so this snapshot is
        # what guarantees a model whose cached builder is later REBUILT
        # still carries its soft-delete / timestamp / tenant / uuid scopes.
        #
        # Why the cache is lost: ``get_builder`` stashes the builder via
        # ``self.builder = ...``, but ``Model.__setattr__`` routes any
        # non-underscore attribute into ``__dirty_attributes__`` — which
        # ``save()`` / ``create()`` ``.clear()`` and ``get_dirty_attributes()``
        # ``.pop("builder")``. So the very first ``create()``/``save()``
        # throws away the scoped builder, and the next ``get_builder()``
        # would otherwise hand back a SCOPE-LESS builder — making
        # ``instance.delete()`` hard-delete instead of soft-delete (the bug
        # pinned by tests/integration/test_soft_delete_contract.py). Seeding
        # from the model snapshot makes scope wiring survive the cache loss.
        booted = self.get_builder()
        self._global_scopes = {
            action: dict(scopes) for action, scopes in booted._global_scopes.items()
        }
        self._macros = dict(booted._macros)


def _model_append_passthrough(self, passthrough) -> Self:
    self.__passthrough__.update(passthrough)
    return self


def _model_get_model_events(self):
    """
    Get cached model events or discover them if not cached.

    Returns:
        dict: Mapping of event names to list of listener methods
    """
    if self._model_events is None:
        self._model_events = _get_model_events(self.__class__)
    return self._model_events


def _model_fire_model_event(self, event_name: str) -> bool:
    """
    Fire a model event and return whether it should continue.

    Args:
        event_name (str): The name of the event to fire

    Returns:
        bool: True if operation should continue, False if cancelled
    """
    events = self._get_model_events()

    if event_name not in events:
        return True  # No listeners, continue

    # Fire all listeners for this event
    for listener_method in events[event_name]:
        try:
            # Call the listener method (bound method call)
            result = listener_method(self)

            # If any listener returns False, cancel the operation
            if result is False:
                return False

        except Exception as e:
            # Log error but don't stop other listeners

            Log.error(
                "Model event error in %s: %s",
                listener_method.__name__,
                e,
                exc_info=True,
            )

    return True  # All listeners passed, continue


def _model_save(self, **kwargs: Any) -> bool:
    """Save the model to the database.

    Laravel-style save method with full event lifecycle.
    Fires appropriate events: creating/updating -> saving -> created/updated -> saved

    Returns:
        True if successful, False if cancelled by event or error occurred
    """
    # Determine if this is a new record or existing one
    is_new_record = not self.is_created()

    # Fire pre-save events
    if is_new_record:
        # Fire creating event - can cancel operation
        if not self._fire_model_event("creating"):
            return False
    else:
        # Fire updating event - can cancel operation
        if not self._fire_model_event("updating"):
            return False

    # Fire saving event - can cancel operation
    if not self._fire_model_event("saving"):
        return False

    try:
        # Perform the actual save operation
        if is_new_record:
            # Create new record
            result = self.__class__.create(self.all_attributes(), **kwargs)
            if result:
                # Copy created record's attributes back to this instance
                self.__attributes__.update(result.__attributes__)
                self.__original_attributes__.update(result.__original_attributes__)
                self.__dirty_attributes__.clear()
        else:
            # Update existing record
            # Snapshot before resolving the builder. ``get_builder()`` may
            # rebuild its cache through ``self.builder = ...`` which is
            # tracked as a dirty attribute by the model's magic setter.
            # Passing the live dirty dict would then leak that QueryBuilder
            # object into the SQL payload once guarded columns are allowed.
            updates = dict(self.get_dirty_attributes())
            if updates:
                # ``__setattr__`` already applied the SET cast when it
                # populated ``__dirty_attributes__`` (via
                # HasAttributes._set_cast_attribute). Letting update()
                # re-cast would DOUBLE-cast and corrupt non-idempotent
                # casts — e.g. DateTimeCast.set on a non-UTC APP_TIMEZONE
                # re-shifts the timestamp on every save. Cast exactly once,
                # at the __setattr__ boundary. (Direct .update({...}) /
                # .create({...}) callers keep casting — they never went
                # through __setattr__.)
                kwargs.setdefault("cast", False)
                result = (
                    self.get_builder()
                    .where(self.get_primary_key(), self.get_primary_key_value())
                    .update(
                        updates,
                        ignore_mass_assignment=True,
                        **kwargs,
                    )
                )
                if result:
                    # Merge dirty attributes into main attributes
                    self.__attributes__.update(self.__dirty_attributes__)
                    self.__original_attributes__.update(self.__dirty_attributes__)
                    self.__dirty_attributes__.clear()
            else:
                result = True  # No changes to save

        if not result:
            return False

        # Fire post-save events (these cannot cancel the operation)
        if is_new_record:
            self._fire_model_event("created")
        else:
            self._fire_model_event("updated")

        self._fire_model_event("saved")

        # Touch parent models if configured
        if hasattr(self, "__touches__") and self.__touches__:
            self._touch_parents()

        return True

    except Exception as e:
        Log.error("Save operation failed: %s", e, exc_info=True)
        return False


def _model_delete(self, **kwargs: Any) -> bool:
    """Delete the model from the database.

    Laravel-style delete method with event lifecycle.
    Fires: deleting -> deleted

    Returns:
        True if successful, False if cancelled by event or error occurred
    """
    # Fire deleting event - can cancel operation
    if not self._fire_model_event("deleting"):
        return False

    try:
        # Perform the actual delete operation
        result = (
            self.get_builder()
            .where(self.get_primary_key(), self.get_primary_key_value())
            .delete(**kwargs)
        )

        if result:
            # Fire deleted event (cannot cancel)
            self._fire_model_event("deleted")
            return True
        else:
            return False

    except Exception as e:
        Log.error("Delete operation failed: %s", e, exc_info=True)
        return False


def _model_touch_parents(self):
    """Touch parent models listed in __touches__."""
    for relation_name in self.__touches__:
        related = getattr(self, relation_name, None)
        if related and hasattr(related, "touch"):
            related.touch()


def _model_touch(self) -> None:
    """Update the model's updated_at timestamp."""
    # Get the timestamp column name
    timestamp_col = "updated_at"
    if (
        hasattr(self, "__timestamps__")
        and self.__timestamps__
        and isinstance(self.__timestamps__, (list, tuple))
    ):
        timestamp_col = (
            self.__timestamps__[1] if len(self.__timestamps__) > 1 else "updated_at"
        )

    # Get the current datetime in the appropriate format
    current_time = self.get_new_datetime_string()

    # Framework-managed write: ``updated_at`` is never in __fillable__,
    # so without ignore_mass_assignment the filter strips it and the
    # UPDATE silently becomes a no-op (same failure SoftDeleteScope's
    # _restore fixed).
    self.update({timestamp_col: current_time}, ignore_mass_assignment=True)

    # Also update the local attribute
    self.__attributes__[timestamp_col] = current_time
    self.__original_attributes__[timestamp_col] = current_time


def _model_get_table_name(cls) -> str:
    """Get the table name, deriving from class name via Laravel's tableize rules."""
    return cls.__table__ or tableize(cls.__name__)


def _model_table(cls, table) -> str:
    """
    Gets the table name.

    Returns:
        str
    """
    cls.__table__ = table
    return cls


def _model_find(
    cls: type[Model],
    record_id: Any | list[Any] | tuple,
    query: bool = False,
) -> Model | list[Model] | QueryBuilder | None:
    """Find a row by the primary key ID.

    Args:
        record_id: The primary key value (int, string) or list of IDs
        query: If True, return the QueryBuilder instead of executing

    Returns:
        A Model instance, Collection of models, QueryBuilder, or None if not found
    """
    if isinstance(record_id, (list, tuple)):
        if not record_id:
            return cls.new_collection([]) if not query else cls().get_builder()
        builder = cls().where_in(cls.get_primary_key(), record_id)
    else:
        builder = cls().where(cls.get_primary_key(), record_id)

    if query:
        return builder

    if isinstance(record_id, (list, tuple)):
        return builder.get()

    return builder.first()


def _model_find_or_fail(
    cls: type[Model],
    record_id: Any | list[Any] | tuple,
    query: bool = False,
) -> Model | list[Model] | QueryBuilder:
    """Find a row by the primary key ID or raise ModelNotFoundException.

    Args:
        record_id: The primary key value or list of IDs
        query: If True, return the QueryBuilder instead of executing

    Returns:
        A Model instance or Collection of models

    Raises:
        ModelNotFoundException: If no model is found
    """
    result = cls.find(record_id, query)

    if not result:
        raise ModelNotFoundException(f"{cls.__name__} with ID {record_id} not found")

    return result


def _model_prevent_lazy_loading(cls, prevent: bool = True) -> None:
    """Enable/disable the strict lazy-load guard process-wide (Laravel parity).

    OFF by default. Turn ON in dev/test bootstrap to convert an
    accidental N+1 lazy-load (a relationship accessed without a prior
    ``.with_(...)`` on a collection-hydrated model) into a loud
    ``LazyLoadingViolation`` instead of a silent extra query. Set on the
    base ``Model`` so the policy applies to every model uniformly.
    """
    Model._prevent_lazy_loading = bool(prevent)


def _model_mark_from_collection(self) -> None:
    """Tag this instance as having come from a multi-row (collection) fetch.

    Set by the query builder for ``get()``/``all()`` results — the rows
    where N+1 lazy-loading actually matters. ``find()``/``first()``
    single-instance loads are NOT tagged, so they never trip the guard.
    """
    self.__dict__["_from_collection"] = True
