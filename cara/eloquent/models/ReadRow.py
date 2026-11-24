"""Rows of a model read for reading only: ``QueryBuilder.get_rows``."""

from __future__ import annotations

from typing import Any

from cara.exceptions import EncryptionException

from ..casts import cast_registry
from ._ModelData import _model_get_new_date


class ReadRow:
    """One database row standing in for its model where rows are only READ.

    A walk over a whole table paid for a model per row: every column of the
    table hydrated into two stores, and a method call per column read. A read
    row holds the columns it was asked for, casts one on its first read and is
    a plain attribute after that. It IS its model's class (``isinstance``, the
    properties and the constants all hold), never saved, never written to.

    A column the read did not ask for RAISES, and ``getattr(row, name, None)``
    raises with it: read as absent, a cost would silently be no cost.
    """

    _read_row: dict[str, Any]
    _read_columns: frozenset[str] = frozenset()
    _read_accessors: dict[str, Any] = {}
    _read_plan: dict[str, Any] = {}

    def __getattr__(self, attribute: str) -> Any:
        state = self.__dict__
        row = state.get("_read_row", {})
        if attribute not in row:
            if attribute in type(self)._read_columns:
                raise LookupError(
                    f"{type(self).__name__}.{attribute} was not read: name it in get_rows()"
                )
            raise AttributeError(
                f"class model '{type(self).__name__}' has no attribute {attribute}"
            )
        value = row[attribute]
        plan = type(self)._read_plan
        if attribute not in plan:
            plan[attribute] = self._reader(attribute)
        reader = plan[attribute]
        if reader is not None:
            value = (
                reader(self, value)
                if attribute in self._read_accessors
                else reader(value)
            )
        state[attribute] = value
        return value

    def __setattr__(self, attribute: str, value: Any) -> None:
        raise TypeError(f"{type(self).__name__} is a read row; load the model to write")

    def _reader(self, attribute: str) -> Any:
        """What turns a column's stored value into the one a hydrated model
        answers with (``Model.__getattr__``); None where they are the same."""
        cls = type(self)
        accessor = cls._read_accessors.get(attribute)
        if accessor is not None:
            return accessor
        definition = cls.__casts__.get(attribute)
        if definition is None:
            return _date if attribute in cls.date_columns() else None
        if (getattr(cls, "__seal_context__", None) or {}).get(attribute):
            raise EncryptionException(
                f"{cls.__name__}.{attribute} is sealed to its row; load the model"
            )
        cast = cast_registry.get_cast_instance(definition)
        return cast.get if cast else None


def _date(value: Any) -> Any:
    return _model_get_new_date(None, value) if value else None


def _columns(model: type) -> frozenset[str]:
    """Every column ``model`` can have: its declared schema's (``fields``), and
    the ones it only names — a schema is not obliged to repeat a cast."""
    schema = getattr(model, "fields", None)
    declared = schema.fget(model()) if isinstance(schema, property) else ()
    return frozenset(
        {
            *(str(field.name) for field in declared if field.name is not None),
            *model.__casts__,
            *(model.__fillable__ or ()),
            *model.date_columns(),
            model.__primary_key__,
        }
    )


_ROW_TYPES: dict[type, type] = {}


def read_row_type(model: type) -> type:
    """``model``'s read-row class, made once."""
    row_type = _ROW_TYPES.get(model)
    if row_type is None:
        accessors = {
            name[4:-10]: method
            for name, method in model.__dict__.items()
            if name.startswith("get_") and name.endswith("_attribute")
        }
        row_type = _ROW_TYPES[model] = type(model)(
            f"{model.__name__}ReadRow",
            (ReadRow, model),
            {
                "__module__": __name__,
                "_read_columns": _columns(model),
                "_read_accessors": accessors,
                "_read_plan": {},
            },
        )
    return row_type


def read_rows(model: type, rows: Any) -> list[Any]:
    """Each driver row as ``model``'s read row."""
    row_type = read_row_type(model)
    read: list[Any] = []
    for row in rows or ():
        instance = object.__new__(row_type)
        instance.__dict__["_read_row"] = row
        read.append(instance)
    return read


__all__ = ["ReadRow", "read_row_type", "read_rows"]
