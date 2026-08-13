"""Named registry of a resource's filterable fields.

A ``TreeSchema`` is what a surface declares ONCE, server-side: the
fields a filter tree may reference, each fully typed (see
``TreeField``). Everything else derives from it —

* request validation (the ``filter_tree:<name>`` rule)
* SQL compilation (``compile_tree``)
* the generated frontend schema (``describe()`` → OpenAPI
  ``x-filter-schema`` → the dashboard's generated filter catalog)

Schemas register by name at import time so the validation rule can look
them up from a plain rule string. Re-registering a name OVERWRITES the
entry on purpose: dev servers hot-reload modules, and a stale schema
object surviving a reload would validate against yesterday's fields.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from cara.exceptions import FilterSchemaError

from .TreeField import TreeField

# Structural caps. Depth is fixed by the wire grammar itself (root →
# group → condition, nothing deeper); these bound breadth.
MAX_ROOT_NODES = 12
MAX_GROUP_CHILDREN = 12
MAX_CONDITIONS = 24


class TreeSchema:
    """The filterable-field vocabulary of one resource."""

    def __init__(
        self,
        name: str,
        fields: Iterable[TreeField],
        *,
        max_root_nodes: int = MAX_ROOT_NODES,
        max_group_children: int = MAX_GROUP_CHILDREN,
        max_conditions: int = MAX_CONDITIONS,
    ) -> None:
        if not name or not isinstance(name, str):
            raise FilterSchemaError("A tree schema needs a non-empty string name.")
        self.name = name
        self.max_root_nodes = int(max_root_nodes)
        self.max_group_children = int(max_group_children)
        self.max_conditions = int(max_conditions)
        self._fields: dict[str, TreeField] = {}
        for field in fields:
            if field.id in self._fields:
                raise FilterSchemaError(
                    f"Schema {name!r} declares field {field.id!r} twice."
                )
            self._fields[field.id] = field
        if not self._fields:
            raise FilterSchemaError(f"Schema {name!r} declares no fields.")

    def field(self, field_id: str) -> TreeField | None:
        """The field declaration behind ``field_id``, or ``None``."""
        return self._fields.get(field_id)

    def fields(self) -> tuple[TreeField, ...]:
        """All fields in declaration order (the frontend's display order)."""
        return tuple(self._fields.values())

    def describe(self) -> dict[str, Any]:
        """JSON-serialisable schema spec for the generated frontend catalog."""
        return {
            "name": self.name,
            "fields": [field.describe() for field in self._fields.values()],
        }

    def __repr__(self) -> str:  # pragma: no cover - debugging sugar
        return f"<TreeSchema name={self.name!r} fields={len(self._fields)}>"


# ── registry ────────────────────────────────────────────────────────

_REGISTRY: dict[str, TreeSchema] = {}


def register_tree_schema(schema: TreeSchema) -> TreeSchema:
    """Register (or hot-reload-replace) a schema under its name."""
    _REGISTRY[schema.name] = schema
    return schema


def tree_schema(name: str) -> TreeSchema | None:
    """Look a schema up by name (``None`` when unregistered)."""
    return _REGISTRY.get(name)


__all__ = [
    "MAX_CONDITIONS",
    "MAX_GROUP_CHILDREN",
    "MAX_ROOT_NODES",
    "TreeSchema",
    "register_tree_schema",
    "tree_schema",
]
