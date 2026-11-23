"""Relationship and guarded bulk-write operations composed into ``Model``."""

from __future__ import annotations

import logging
from typing import Any, Self

from cara.exceptions import InvalidArgumentException

_logger = logging.getLogger("cara.eloquent.models")
Model: type


def _bind_model(model_type: type) -> None:
    global Model
    Model = model_type


def _model_set_appends(self, appends) -> Self:
    """
    Get the attributes that should be converted to dates.

    :rtype: list
    """
    self.__appends__ += appends
    return self


def _model_save_many(self, relation, relating_records):
    if isinstance(relating_records, Model):
        raise InvalidArgumentException(
            "Saving many records requires an iterable like a collection or a list of models and not a Model object. To attach a model, use the 'attach' method."
        )

    for related_record in relating_records:
        self.attach(relation, related_record)


def _model_detach_many(self, relation, relating_records):
    if isinstance(relating_records, Model):
        raise InvalidArgumentException(
            "Detaching many records requires an iterable like a collection or a list of models and not a Model object. To detach a model, use the 'detach' method."
        )

    related = getattr(self.__class__, relation)
    for related_record in relating_records:
        if not related_record.is_created():
            related_record = related_record.create(related_record.all_attributes())
        else:
            related_record.save()

        related.detach(self, related_record)


def _model_related(self, relation):
    related = getattr(self.__class__, relation)
    return related.relate(self)


def _model_get_related(self, relation):
    if hasattr(self, "_relations") and relation in self._relations:
        return self._relations[relation]
    return getattr(self.__class__, relation)


def _model_attach(self, relation, related_record):
    related = getattr(self.__class__, relation)
    return related.attach(self, related_record)


def _model_detach(self, relation, related_record):
    related = getattr(self.__class__, relation)

    if not related_record.is_created():
        related_record = related_record.create(related_record.all_attributes())
    else:
        related_record.save()

    return related.detach(self, related_record)


def _model_save_quietly(self):
    """
    This method calls the save method on a model without firing the saved & saving observer
    events. Saved/Saving are toggled back on once save_quietly has been ran.

    Instead of calling:

    User().save(...)

    you can use this:

    User.save_quietly(...)
    """
    # Muted for this execution context only (``ObservesEvents.muted_events``).
    # It used to flip the CLASS flag ``__has_events__``, which also silenced
    # every concurrent save of the class on other threads and tasks. The
    # context manager restores the mute even when ``save`` raises.
    with self.__class__.muted_events():
        return self.save()


def _model_delete_quietly(self):
    """This method calls the delete method on a model without firing the delete & deleting observer events.
    Instead of calling:

    User().delete(...)

    you can use this:

    User.delete_quietly(...)

    Returns:
        self
    """
    # Muted for this execution context only — see ``_model_save_quietly``.
    with self.__class__.muted_events():
        return self.__class__.where(
            self.get_primary_key(),
            self.get_primary_key_value(),
        ).delete()


def _model_attach_related(self, relation, related_record):
    return self.attach(relation, related_record)


def _model_filter_fillable(cls, dictionary: dict[str, Any]) -> dict[str, Any]:
    """
    Filters provided dictionary to only include fields specified in the model's __fillable__
    property.

    Passed dictionary is not mutated.
    """
    if cls.__fillable__ != ["*"]:
        dictionary = {x: dictionary[x] for x in cls.__fillable__ if x in dictionary}
    return dictionary


def _model_filter_mass_assignment(cls, dictionary: dict[str, Any]) -> dict[str, Any]:
    """
    Filters the provided dictionary in preparation for a mass-assignment operation.

    Wrapper around filter_fillable() & filter_guarded(). Passed dictionary is not mutated.
    """
    return cls.filter_guarded(cls.filter_fillable(dictionary))


def _model_filter_guarded(cls, dictionary: dict[str, Any]) -> dict[str, Any]:
    """
    Filters provided dictionary to exclude fields specified in the model's __guarded__ property.

    Passed dictionary is not mutated.
    """
    if cls.__guarded__ == ["*"]:
        # If all fields are guarded, all data should be filtered
        return {}
    guarded = {_column_spelling(name) for name in cls.__guarded__}
    return {f: dictionary[f] for f in dictionary if _column_spelling(f) not in guarded}


def _column_spelling(key: Any) -> Any:
    """``key`` as the column it writes, the way ``TenantScope`` recognises a
    column "under any spelling": the grammar drops a ``table.`` qualifier and
    the quote characters, so ``{"users.is_admin": 1}`` writes ``is_admin``.
    The deny-list compared exact keys, and the qualified spelling slipped
    past ``__guarded__ = ["is_admin"]``. Case-folded too: SQLite folds even a
    quoted identifier, and refusing one key too many is the safe direction
    for a deny-list."""
    if not isinstance(key, str):
        return key
    return key.rsplit(".", 1)[-1].strip().strip('"`[]').strip().casefold()


def _model_upsert(
    cls,
    values: list[dict[str, Any]],
    unique_by: list[str],
    update: list[str] | None = None,
    cast: bool = True,
):
    """
    Insert new records or update existing ones.

    Args:
        values: List of dictionaries with data to insert/update
        unique_by: List of column names that determine uniqueness
        update: List of column names to update on conflict (if None, updates all except unique_by)
        cast: Whether to apply model casts

    Returns:
        Number of affected rows

    Example:
        Receipt.upsert([
            {"receipt_id": "123", "status": "processed", "amount": 100},
            {"receipt_id": "124", "status": "pending", "amount": 200}
        ], unique_by=["receipt_id"], update=["status", "amount"])
    """
    # Create instance and call through passthrough mechanism
    instance = cls()

    # Use get_builder() to avoid boot() cycle and directly call upsert
    builder = instance.get_builder()
    return builder.upsert(
        values=values,
        unique_by=unique_by,
        update=update,
        cast=cast,
    )
