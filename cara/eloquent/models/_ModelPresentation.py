"""Hydration, casting and presentation composed into ``Model``."""

from __future__ import annotations

import contextlib
import copy
import logging
from datetime import date as datetimedate
from datetime import datetime
from datetime import time as datetimetime
from typing import Any, Self

from cara.eloquent.Integrity import is_unique_violation
from cara.exceptions import LazyLoadingViolation, ModelNotFoundException
from cara.facades import DB
from cara.support import Collection, json_dumps

from ..casts import cast_registry as enhanced_registry
from ..query import QueryBuilder

_logger = logging.getLogger("cara.eloquent.models")
Model: type


def _bind_model(model_type: type) -> None:
    global Model
    Model = model_type


def _model_guard_against_lazy_load(self, relation: str) -> None:
    """Raise ``LazyLoadingViolation`` for an accidental lazy-load — IF armed.

    No-op (zero behaviour change) unless ALL hold:
      * the guard is explicitly enabled, AND
      * this instance came from a collection fetch (eager was the
        intended path), AND
      * the relation is not already loaded (eager-loaded relations are
        cached in ``_relations`` and never reach here).
    """
    if not Model._prevent_lazy_loading:
        return
    if not self.__dict__.get("_from_collection", False):
        # Single-instance find()/first() — lazy-loading one related
        # record is fine; only collection rows risk N+1.
        return
    if relation in self.__dict__.get("_relations", {}):
        return

    raise LazyLoadingViolation(
        f"Attempted to lazy-load relation '{relation}' on "
        f"{self.__class__.__name__} while strict lazy-loading is enabled. "
        f"Eager-load it with .with_('{relation}') to avoid an N+1 query."
    )


def _model_is_loaded(self) -> bool:
    return bool(self.__attributes__)


def _model_is_created(self) -> bool:
    return self.get_primary_key() in self.__attributes__


def _model_hydrate(cls, result, relations=None) -> Any:
    """
    Takes a result and loads it into a model.

    Args:
        result ([type]): [description]
        relations (dict, optional): [description]. Defaults to {}.

    Returns:
        [type]: [description]
    """

    relations = relations or {}

    if result is None:
        return None

    if isinstance(result, (list, tuple)):
        response = []
        for element in result:
            response.append(cls.hydrate(element))
        return cls.new_collection(response)

    elif isinstance(result, dict):
        model = cls()
        dic = {}
        for key, value in result.items():
            if key in model.get_dates() and value:
                value = model.get_new_date(value)
            dic.update({key: value})

        model.observe_events(model, "hydrating")
        model.__attributes__.update(dic or {})
        model.__original_attributes__.update(dic or {})
        model.add_relation(relations)
        model.observe_events(model, "hydrated")
        return model

    elif hasattr(result, "serialize"):
        model = cls()
        model.__attributes__.update(result.serialize())
        model.__original_attributes__.update(result.serialize())
        return model
    else:
        model = cls()
        model.observe_events(model, "hydrating")
        model.__attributes__.update(dict(result))
        model.__original_attributes__.update(dict(result))
        model.observe_events(model, "hydrated")
        return model


def _model_fill(self, attributes) -> Self:
    self.__attributes__.update(attributes)
    return self


def _model_fill_original(self, attributes) -> Self:
    self.__original_attributes__.update(attributes)
    return self


def _model_new_collection(cls, data) -> Any:
    """
    Takes a result and puts it into a new collection. This is designed to be able to be
    overidden by the user.

    Args:
        data (list|dict): Could be any data type but will be loaded directly into a collection.

    Returns:
        Collection
    """
    return Collection(data)


def _model_create(
    cls: type[Model],
    dictionary: dict[str, Any] | None = None,
    query: bool = False,
    cast: bool = True,
    **kwargs: Any,
) -> Model | QueryBuilder:
    """Create a new record in the database.

    Args:
        dictionary: Attributes for the new record
        query: If True, return the QueryBuilder instead of executing
        cast: Whether to cast attribute values
        **kwargs: Additional options passed to the query builder

    Returns:
        A new Model instance, or a QueryBuilder if query=True
    """
    if query:
        return cls().get_builder().create(dictionary, query=True, cast=cast, **kwargs)

    return cls().get_builder().create(dictionary, cast=cast, **kwargs)


def _model_cast_value(cls, attribute: str, value: Any):
    """
    Given an attribute name and a value, casts the value using the model's registered caster.

    If no registered caster exists, returns the unmodified value.

    Supports parametrized casts like:
    - "datetime:YYYY-MM-DD HH:mm:ss"
    - "array:int"
    - "hash:bcrypt"
    """
    cast_definition = cls.__casts__.get(attribute)
    if not cast_definition:
        return value

    if value is None:
        return None

    # Use new cast registry system

    cast_instance = enhanced_registry.get_cast_instance(cast_definition)
    if cast_instance:
        return cast_instance.set(value)

    return value


def _model_cast_values(cls, dictionary: dict[str, Any]) -> dict[str, Any]:
    """
    Runs provided dictionary through all model casters and returns the result.

    Does not mutate the passed dictionary.
    """
    if not dictionary:
        return {}
    return {x: cls.cast_value(x, dictionary[x]) for x in dictionary}


def _model_fresh(self) -> Any:
    """Return a newly-loaded instance of the same record (Laravel parity)."""
    return (
        self.get_builder()
        .where(
            self.get_primary_key(),
            self.get_primary_key_value(),
        )
        .first()
    )


def _model_refresh(self) -> Self:
    """Reload the model's attributes from the database in place.

    Laravel parity — unlike :meth:`fresh`, this mutates ``self`` and
    returns ``self`` so callers can chain or ignore the return value.
    Raises ``ModelNotFoundException`` if the record no longer exists.
    """
    reloaded = self.fresh()
    if reloaded is None:
        raise ModelNotFoundException(
            f"Cannot refresh {self.__class__.__name__}: record "
            f"{self.get_primary_key()}={self.get_primary_key_value()!r} "
            "was not found."
        )

    reloaded_attrs = getattr(reloaded, "__attributes__", {})
    self.__attributes__ = dict(reloaded_attrs)
    self.__original_attributes__ = dict(reloaded_attrs)
    self.__dirty_attributes__ = {}
    # Clear any cached relationship data so accessors reload fresh.
    self._relations = {}
    self._relationships = {}
    return self


def _model_serialize(self, exclude=None, include=None) -> dict[str, Any]:
    """
    Convert the model instance to a domain dictionary.

    Exact numeric types stay exact here; ``to_json`` owns wire encoding.

    Args:
        exclude (list, optional): Attributes to exclude from serialization
        include (list, optional): Only these attributes will be included

    Returns:
        dict: The model attributes after declared casts and date formatting.
    """
    # Get all attributes
    data = self.__attributes__.copy()
    data.update(self.__dirty_attributes__)

    # Remove builder if present
    if "builder" in data:
        del data["builder"]

    # Apply exclude/include filters
    if include:
        data = {k: v for k, v in data.items() if k in include}
    if exclude:
        data = {k: v for k, v in data.items() if k not in exclude}

    # Apply __visible__ whitelist (if set, only these keys are exposed)
    visible = getattr(self, "__visible__", [])
    if visible:
        data = {k: v for k, v in data.items() if k in visible}

    # Apply hidden attributes
    hidden = getattr(self, "__hidden__", [])
    for hidden_key in hidden:
        data.pop(hidden_key, None)

    # Apply casts to all attributes that have them
    for key, value in data.items():
        if value is not None and key in self.__casts__:
            try:
                # Use the proper cast system
                data[key] = self._cast_attribute(key, value)
            except Exception:
                _logger.warning("cast failed for attribute %s", key, exc_info=True)

    # Normalize temporal values for the array representation. Decimal stays
    # Decimal until a real JSON boundary, where ``json_dumps`` emits its
    # exact digits as a string.
    for key, value in data.items():
        if value is not None:
            final_value = data[key]
            if isinstance(final_value, datetime):
                data[key] = final_value.isoformat()
            elif isinstance(final_value, datetimetime):
                data[key] = final_value.strftime("%H:%M:%S")
            elif isinstance(final_value, datetimedate):
                data[key] = final_value.strftime("%Y-%m-%d")

    # Add relationships - Laravel way: use serialize() for proper casting
    relations_dict = getattr(self, "_relations", {})
    for relation_name, relation_value in relations_dict.items():
        if relation_value is None:
            data[relation_name] = None
        elif isinstance(relation_value, list):
            # Collection of models

            data[relation_name] = Collection(relation_value).serialize()
        elif hasattr(relation_value, "serialize"):
            # Single model - use serialize() for proper decimal casting
            data[relation_name] = relation_value.serialize()
        else:
            # Raw value
            data[relation_name] = relation_value

    # Add appends (computed attributes)
    appends = getattr(self, "__appends__", [])
    for append_name in appends:
        with contextlib.suppress(AttributeError):
            data[append_name] = getattr(self, append_name)

    return data


def _model_to_array(self, exclude=None, include=None) -> dict[str, Any]:
    """
    Laravel-style alias for serialize().

    Returns:
        dict: Same as serialize()
    """
    return self.serialize(exclude=exclude, include=include)


def _model_to_json(self, **kwargs) -> str:
    """
    Convert the model instance to JSON.
    Laravel-style method with options.

    Args:
        **kwargs: Additional arguments passed to json.dumps()

    Returns:
        str: JSON representation of the model
    """
    return json_dumps(self.to_array(), **kwargs)


def _model_make_hidden(self, *attributes):
    """
    Make the given attributes hidden for serialization.
    Returns a new instance with updated hidden attributes.

    Args:
        *attributes: Attribute names to hide

    Returns:
        Model: New model instance with updated visibility
    """
    clone = self._clone_for_visibility()

    for attr in attributes:
        if attr not in clone.__hidden__:
            clone.__hidden__.append(attr)

    return clone


def _model_make_visible(self, *attributes):
    """
    Make the given hidden attributes visible for serialization.
    Returns a new instance with updated visible attributes.

    Args:
        *attributes: Attribute names to make visible

    Returns:
        Model: New model instance with updated visibility
    """
    clone = self._clone_for_visibility()

    # Remove from hidden list
    clone.__hidden__ = [attr for attr in clone.__hidden__ if attr not in attributes]

    return clone


def _model_set_hidden(self, hidden) -> Self:
    """
    Set the hidden attributes for the model.

    Args:
        hidden (list): List of attributes to hide

    Returns:
        Model: Self for method chaining
    """
    self.__hidden__ = list(hidden) if hidden else []
    return self


def _model_set_visible(self, visible) -> Self:
    """
    Set the visible attributes for the model.

    Args:
        visible (list): List of attributes to show

    Returns:
        Model: Self for method chaining
    """
    self.__visible__ = list(visible) if visible else []
    return self


def _model_append(self, *attributes) -> Self:
    """
    Add attributes to the append list.

    Args:
        *attributes: Attribute names to append

    Returns:
        Model: Self for method chaining
    """
    for attr in attributes:
        if attr not in self.__appends__:
            self.__appends__.append(attr)
    return self


def _model_except_keys(self, *keys):
    """
    Return a new model instance excluding specified keys from serialization.
    Compatible with Collection.except_keys() for relationship serialization.

    Args:
        *keys: Attribute names to exclude

    Returns:
        Model: New model instance with updated hidden attributes
    """
    return self.make_hidden(*keys)


def _model_without_timestamps(self):
    """
    Return a version of this model without timestamp fields.

    Returns:
        Model: New model instance without timestamps
    """
    clone = self._clone_for_visibility()

    timestamp_fields = [self.date_created_at, self.date_updated_at]
    for field in timestamp_fields:
        if field not in clone.__hidden__:
            clone.__hidden__.append(field)

    return clone


def _model_clone_for_visibility(self):
    """Create a shallow clone for visibility modifications."""
    clone = copy.copy(self)
    clone.__hidden__ = list(self.__hidden__)
    clone.__visible__ = list(self.__visible__)
    clone.__appends__ = list(self.__appends__)
    return clone


def _model_first_or_create(cls, wheres, creates: dict | None = None) -> Any:
    """
    Get the first record matching the attributes or create it.

    Race-safe: a concurrent caller can insert the same row between
    the SELECT below and the INSERT. If a UNIQUE constraint backs
    the ``wheres`` columns, the racing INSERT raises a driver-
    level UniqueViolation. Without the catch-and-re-query below,
    the loser of the race surfaces that as an unhandled
    ``IntegrityError`` even though semantically the operation
    succeeded (the row IS there now — just inserted by the other
    request). The fix asks ``cara.eloquent.Integrity`` — the single
    classifier — and re-runs the SELECT to return the row the winning
    side inserted.

    Without a UNIQUE constraint there's no atomic guard — two
    concurrent callers DO each insert a row. ``first_or_create``
    cannot prevent that at the application layer; callers who
    need true uniqueness must back ``wheres`` with a DB-level
    UNIQUE index.

    Returns:
        Model
    """
    if creates is None:
        creates = {}
    self = cls()
    record = self.where(wheres).first()
    total = {}
    total.update(creates)
    total.update(wheres)
    if record:
        return record
    try:
        # The INSERT must own a transaction boundary. Under a caller's open
        # transaction this is a savepoint; standalone it is a short outer
        # transaction. PostgreSQL aborts the current transaction scope after
        # 23505, so the recovery SELECT below is usable only after this scope
        # has rolled back.
        with DB.transaction(getattr(cls, "__connection__", None)):
            return self.create(total, id_key=cls.get_primary_key())
    except Exception as exc:
        if not is_unique_violation(exc):
            raise
        # Concurrent insert won the race — re-query and return
        # the row they inserted. If it's STILL not there
        # (race against a delete, RLS hiding, etc.), bubble the
        # original IntegrityError so the caller sees the real
        # failure instead of a misleading None.
        again = cls().where(wheres).first()
        if again is not None:
            return again
        raise
