"""Persistence helpers, serialization and attribute/date access composed into ``Model``."""

from __future__ import annotations

import logging
from datetime import date as datetimedate
from datetime import datetime
from datetime import time as datetimetime
from typing import Any

import pendulum

from cara.eloquent.Integrity import is_unique_violation
from cara.exceptions import EncryptionException
from cara.facades import DB
from cara.support import Collection

from ..casts import cast_registry as enhanced_registry
from ..casts.DateTimeCast import DateTimeCast

_logger = logging.getLogger("cara.eloquent.models")
Model: type


def _bind_model(model_type: type) -> None:
    global Model
    Model = model_type


def _model_first_or_new(cls, wheres, values: dict | None = None) -> Any:
    """
    Laravel-style firstOrNew.
    Get the first record matching the attributes, or a new (unpersisted)
    instance hydrated with the merged attributes.

    Returns:
        Model
    """
    if values is None:
        values = {}
    self = cls()
    record = self.where(wheres).first()
    if record is not None:
        return record
    total = {}
    total.update(values)
    total.update(wheres)
    instance = cls()
    instance.fill(total)
    return instance


def _model_update_or_create(cls, wheres, updates) -> Any:
    """Upsert: update if a matching row exists, else create.

    ONE round trip when the row exists: the UPDATE runs first and hands the
    row it wrote back (``RETURNING``). It used to be three — a SELECT to
    look, the UPDATE, a SELECT to read the result — although the UPDATE's own
    answer says both whether a row was there and what it now holds. A missing
    row costs what it did: the UPDATE that matches nothing, the INSERT, the
    read-back.

    The race is ``first_or_create``'s: the INSERT can lose to a concurrent
    inserter. The loser catches the UNIQUE violation and applies its UPDATE
    again (the merge semantics of an upsert), so both racing requests
    converge on a single row with the latest payload. If the row vanished
    again (a race against a delete) that UPDATE matches nothing and the
    original error surfaces.
    """
    total = {}
    total.update(updates)
    total.update(wheres)

    record = _update_matching(cls, wheres, total)
    if record is not None:
        return record
    try:
        # A unique-race loser must roll back its failed INSERT before the
        # UPDATE can reuse the surrounding PostgreSQL session. Nested
        # callers get a savepoint from this boundary.
        with DB.transaction(getattr(cls, "__connection__", None)):
            return cls.create(total, id_key=cls.get_primary_key()).fresh()
    except Exception as exc:
        if not is_unique_violation(exc):
            raise
        record = _update_matching(cls, wheres, total)
        if record is None:
            raise
        return record


def _update_matching(cls, wheres, total) -> Any:
    """UPDATE the rows matching ``wheres`` and answer with the first row
    written, as a loaded model — or None when there is no such row.

    A fresh instance per statement: a model query's ``update`` fills the
    instance it ran on, and a filled instance reads as a LOADED one, whose
    next ``update`` is keyed to its primary key instead of ``wheres``.
    """
    # A model that reads a chosen column list (``__selects__``) is handed
    # back in that shape, which only its own SELECT produces.
    selects = bool(cls().get_selects())
    rows = (
        cls()
        .where(wheres)
        .update(total, returning=(cls.get_primary_key(),) if selects else ("*",))
    )
    if not isinstance(rows, list):
        # No statement ran: mass assignment left nothing to write (or a dry
        # run). Whether the row exists is then the SELECT's to say.
        return cls().where(wheres).first()
    if not rows:
        return None
    if selects:
        return cls().where(wheres).first()
    return cls().get_builder().prepare_result(dict(rows[0]))


def _model_truncate(cls, foreign_keys=False) -> None:
    """
    Laravel-style truncate method.
    Truncate the table associated with the model.

    Arguments:
        foreign_keys {bool} -- Whether to disable foreign key constraints (default: {False})

    Returns:
        int -- Number of affected rows
    """
    return cls().get_builder().truncate(foreign_keys)


def _model_query(cls) -> Any:
    """
    Laravel-style query method.
    Begin querying the model.

    Returns:
        QueryBuilder -- A new query builder instance
    """
    return cls().get_builder()


def _model_relations_to_dict(self):
    """
    Converts a models relationships to a dictionary.

    Returns:
        [type]: [description]
    """
    new_dic = {}
    for key, value in self._relations.items():
        if value == {}:
            new_dic.update({key: {}})
        else:
            if value is None:
                new_dic.update({key: {}})
                continue
            elif isinstance(value, list):
                value = Collection(value).serialize()
            elif isinstance(value, dict):
                pass
            else:
                value = value.serialize()

            new_dic.update({key: value})

    return new_dic


def _model_getattr(self, attribute):
    """
    Magic method that is called when an attribute does not exist on the model.

    Args:
        attribute (string): the name of the attribute being accessed or called.

    Returns:
        mixed: Could be anything that a method can return.
    """

    # An accessor (decorated with @accessor, or only named like one) first;
    # the class's own ``__dict__``, never a lookup that could recurse here.
    accessor_method = type(self).__dict__.get(f"get_{attribute}_attribute")
    if accessor_method is not None:
        # Dirty first, then stored, then None for virtual attributes.
        return accessor_method(self, self.get_raw_attribute(attribute))

    # A column read is the hot path of every walk over rows: the two stores are
    # read once each, and the date list is built only for an uncast column.
    state = self.__dict__
    dirty = state.get("__dirty_attributes__")
    if dirty is not None and attribute in dirty:
        return self.get_dirty_value(attribute)

    stored = state.get("__attributes__")
    if stored is not None and attribute in stored:
        value = self.get_value(attribute)
        # A cast owns its column, whatever its date type.
        if attribute in self.__casts__ or attribute not in self.get_dates():
            return value
        return self.get_new_date(value) if value else None

    if attribute in self.__passthrough__:
        # Special warning for common dict-style usage mistake
        if attribute == "get":

            def method(*args, **kwargs):
                # Check if this looks like dict-style access
                if len(args) >= 1 and isinstance(args[0], str) and len(args) <= 2:
                    attr_name = args[0]
                    default_value = args[1] if len(args) == 2 else None

                    # This looks like user.get('id', 'default') - common mistake!
                    raise AttributeError(
                        f"❌ Model dict-style access error!\n"
                        f"You tried: model.get('{attr_name}', {repr(default_value)})\n"
                        f"✅ Use instead: getattr(model, '{attr_name}', {repr(default_value)})\n"
                        f"   or simply: model.{attr_name}\n"
                        f"\n"
                        f"🧠 Remember: Cara Models are not dictionaries!\n"
                        f"   - model.attribute     ← ✅ Correct\n"
                        f"   - model.get('attr')   ← ❌ Wrong (calls QueryBuilder.get)\n"
                        f"   - getattr(model, 'attr', default) ← ✅ Correct with default"
                    )

                # Not dict-style, pass to QueryBuilder
                return getattr(self.get_builder(), attribute)(*args, **kwargs)

            return method
        else:

            def method(*args, **kwargs):
                return getattr(self.get_builder(), attribute)(*args, **kwargs)

            return method

    if attribute in self.__dict__.get("_relations", {}):
        return self.__dict__["_relations"][attribute]

    if attribute in self.__dict__.get("_relationships", {}):
        return self.__dict__["_relationships"][attribute]

    if attribute not in self.__dict__:
        name = self.__class__.__name__

        raise AttributeError(f"class model '{name}' has no attribute {attribute}")

    return None


def _model_only(self, attributes: list) -> dict:
    if isinstance(attributes, str):
        attributes = [attributes]
    results: dict[str, Any] = {}
    for attribute in attributes:
        if " as " in attribute:
            attribute, alias = attribute.split(" as ")
            alias = alias.strip()
            attribute = attribute.strip()
        else:
            alias = attribute.strip()
            attribute = attribute.strip()

        results[alias] = self.get_raw_attribute(attribute)

    return results


def _model_setattr(self, attribute, value):
    # Check for @mutator decorated methods first
    mutator_method_name = f"set_{attribute}_attribute"
    # Use direct __dict__ access to avoid recursion
    if mutator_method_name in self.__class__.__dict__:
        mutator_method = self.__class__.__dict__[mutator_method_name]
        if hasattr(mutator_method, "_is_mutator"):
            value = mutator_method(self, value)

    # A cast owns its column's write. An uncast DATE column (``__dates__``,
    # created_at/updated_at) takes the same instant rule as ``DateTimeCast``:
    # an aware value is stored as its UTC instant, a naive one is refused —
    # never re-read in ``app.timezone`` (DOCTRINE §7). Running both used to
    # re-parse the cast's output and drop its microseconds.
    if attribute in self.__casts__:
        value = self._set_cast_attribute(attribute, value)
    else:
        value = type(self).cast_value(attribute, value)

    try:
        if not attribute.startswith("_"):
            self.__dict__["__dirty_attributes__"].update({attribute: value})
        else:
            self.__dict__[attribute] = value
    except KeyError:
        # `__dirty_attributes__` has not been initialized yet (can happen
        # during parent ``__init__`` before our ``__init__`` runs line
        # ``self.__dirty_attributes__ = {}``). Silently dropping the value
        # — as the previous implementation did — lost writes. Instead, we
        # bootstrap the dict and retry so no attribute is lost.
        if not attribute.startswith("_"):
            self.__dict__.setdefault("__dirty_attributes__", {})[attribute] = value
        else:
            self.__dict__[attribute] = value


def _model_get_raw_attribute(self, attribute):
    """Read an attribute's stored value: PENDING first, then persisted.

    This is the one owner of that precedence. Reads go through
    ``self.__dict__`` rather than attribute access so the method stays
    usable from ``__getattr__`` without recursing.

    Pre-fix this read ``__attributes__`` alone while every write lands in
    ``__dirty_attributes__`` (``Model.__setattr__``), so it could not see
    an unsaved value: ``m.foo = 5`` followed by ``m.get_attribute("foo")``
    answered ``None`` — the public read API disagreed with ``m.foo`` on
    the same model. ``__getattr__`` had the precedence right and spelled
    it out three separate times; those copies now call here.

    Args:
        attribute (string): The attribute to fetch

    Returns:
        mixed: Any value an attribute can be.
    """
    dirty = self.__dict__.get("__dirty_attributes__")
    if dirty is not None and attribute in dirty:
        return dirty[attribute]
    stored = self.__dict__.get("__attributes__")
    if stored is not None:
        return stored.get(attribute)
    return None


def _model_is_dirty(self, *attributes: str) -> bool:
    """Return ``True`` if the model (or specific attributes) has unsaved changes.

    Mirrors Laravel's ``isDirty`` signature — with no arguments, returns
    ``True`` if any attribute is dirty; with one or more names, returns
    ``True`` only if at least one of the named attributes is dirty.
    """
    if not self.__dirty_attributes__:
        return False
    if not attributes:
        return True
    return any(name in self.__dirty_attributes__ for name in attributes)


def _model_is_clean(self, *attributes: str) -> bool:
    """Inverse of :meth:`is_dirty` — Laravel parity."""
    return not self.is_dirty(*attributes)


def _model_get_original(self, key):
    return self.__original_attributes__.get(key)


def _model_get_dirty_attributes(self):
    if "builder" in self.__dirty_attributes__:
        self.__dirty_attributes__.pop("builder")
    return self.__dirty_attributes__ or {}


def _model_seal_context(cls, attribute: str, row: Any) -> str | None:
    """The AEAD context binding ``attribute`` to its row, or ``None`` when
    the column is not declared in ``__seal_context__``.

    Fails closed: a sealed column whose row lacks one of its binding fields
    raises instead of sealing (or opening) without the binding.
    """
    fields = (getattr(cls, "__seal_context__", None) or {}).get(attribute)
    if not fields:
        return None
    cast_instance = enhanced_registry.get_cast_instance(cls.__casts__.get(attribute))
    if not getattr(cast_instance, "binds_row", False):
        raise EncryptionException(
            f"{cls.__name__}.{attribute} is in __seal_context__ but its cast "
            "does not bind a row (use encrypted / encrypted_json)"
        )
    table = getattr(cls, "__table__", None) or cls.__name__
    parts = [f"{table}.{attribute}"]
    for field in fields:
        value = row.get(field) if row is not None else None
        if value is None or value == "":
            raise EncryptionException(
                f"{table}.{attribute} is sealed to {field}; the row has none"
            )
        parts.append(f"{field}={value}")
    return "|".join(parts)


def _model_seal_row(self) -> dict:
    """This instance's raw identity: persisted values under pending ones."""
    return {
        **self.__dict__.get("__attributes__", {}),
        **self.__dict__.get("__dirty_attributes__", {}),
    }


def _cast_get(self, attribute, value):
    cast_instance = enhanced_registry.get_cast_instance(self.__casts__[attribute])
    if not cast_instance:
        return value
    # The row is built only for a column the model seals: every other cast
    # read (an integer, a date) used to copy the model's whole attribute map
    # first — one dict of every column per attribute read.
    if not (getattr(type(self), "__seal_context__", None) or {}).get(attribute):
        return cast_instance.get(value)
    context = _model_seal_context(type(self), attribute, _model_seal_row(self))
    if context is not None:
        return cast_instance.get(value, context)
    return cast_instance.get(value)


def _model_get_value(self, attribute):
    """Get attribute value with cast applied."""
    value = self.__attributes__[attribute]
    if attribute in self.__casts__:
        return _cast_get(self, attribute, value)
    return value


def _model_get_dirty_value(self, attribute):
    """Get dirty attribute value with cast applied."""
    value = self.__dirty_attributes__[attribute]
    if attribute in self.__casts__:
        return _cast_get(self, attribute, value)
    return value


def _model_all_attributes(self):
    attributes = {**self.__attributes__, **self.get_dirty_attributes()}
    for key, value in list(attributes.items()):
        if key in self.__casts__:
            attributes[key] = self._cast_attribute(key, value)

    return attributes


def _model_delete_attribute(self, key):
    if key in self.__attributes__:
        del self.__attributes__[key]
        return True

    return False


def _model_get_cast_map(self):
    return {**self.__internal_cast_map__, **self.__cast_map__}


def _model_cast_attribute(self, attribute, value):
    # An undeclared attribute has no cast — return it as it is stored.
    # This used to ``KeyError``, which only stayed hidden because
    # ``get_raw_attribute`` answered ``None`` for every unsaved value and
    # ``get_attribute`` short-circuits on ``None``: the moment reads saw
    # pending writes, every uncast attribute raised.
    if attribute not in self.__casts__:
        return value

    cast_method = self.__casts__[attribute]
    cast_map = self.get_cast_map()

    if value is None:
        return None

    if attribute in (getattr(self, "__seal_context__", None) or {}):
        return _cast_get(self, attribute, value)

    if isinstance(cast_method, str):
        # Handle parametrized casts
        if ":" in cast_method:
            cast_type, cast_params = cast_method.split(":", 1)

            if cast_type in cast_map:
                if cast_type == "datetime":
                    parts = cast_params.split(",")
                    format_str = parts[0] if parts else None
                    timezone = parts[1].strip() if len(parts) > 1 else "UTC"
                    return cast_map[cast_type](format_str, timezone).get(value)
                elif cast_type == "decimal":
                    precision = int(cast_params) if cast_params.isdigit() else 2
                    return cast_map[cast_type](precision).get(value)
                # array / hash / other single-param casts share the same path
                return cast_map[cast_type](cast_params).get(value)

        elif cast_method in cast_map:
            return cast_map[cast_method]().get(value)

    return cast_method(value)


def _model_getitem(self, attribute):
    return getattr(self, attribute)


def _model_get_dates(self):
    """
    Get the attributes that should be converted to dates.

    :rtype: list
    """
    return type(self).date_columns()


def _model_date_columns(cls) -> list[str]:
    """The columns that are dates: ``__dates__``, created_at and updated_at."""
    return [*cls.__dates__, cls.date_created_at, cls.date_updated_at]


def _model_get_new_date(self, _datetime=None):
    """
    Get the attributes that should be converted to dates.

    :rtype: list
    """

    if not _datetime:
        return pendulum.now("UTC")
    elif isinstance(_datetime, str):
        return pendulum.parse(_datetime, tz="UTC")
    elif isinstance(_datetime, datetime):
        return pendulum.instance(_datetime, tz="UTC")
    elif isinstance(_datetime, datetimedate):
        return pendulum.datetime(
            _datetime.year,
            _datetime.month,
            _datetime.day,
            tz="UTC",
        )
    elif isinstance(_datetime, datetimetime):
        return pendulum.parse(
            f"{_datetime.hour}:{_datetime.minute}:{_datetime.second}",
            tz="UTC",
        )

    return pendulum.instance(_datetime, tz="UTC")


def _model_get_new_datetime_string(self, _datetime=None):
    """
    The UTC wall string a date column is WRITTEN as: now when no value is
    given, else the instant ``_datetime`` names. It is a WRITE value (a soft
    delete's ``deleted_at``, an update of a date column), so it keeps the
    microseconds (``DateTimeCast.sql_instant``) and follows the one write
    rule (``DateTimeCast.instant``): a naive value raises
    ``NaiveDateTimeException`` instead of being read as UTC.

    :rtype: str
    """
    if _datetime is None:
        return DateTimeCast.sql_instant(pendulum.now("UTC"))
    return DateTimeCast.sql_instant(DateTimeCast.instant(_datetime))


def _model_get_new_serialized_date(self, _datetime):
    """
    Get the attributes that should be converted to dates.

    :rtype: list
    """
    return self.get_new_date(_datetime).to_datetime_string()
