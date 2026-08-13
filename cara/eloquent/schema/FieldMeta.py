"""Canonical definition of ``FieldMeta``."""

from __future__ import annotations

import inspect

from .Blueprint import Blueprint
from .FieldProxy import FieldProxy


class FieldMeta(type):
    """Metaclass that automatically creates field methods from Blueprint."""

    def __new__(cls, name, bases, attrs):
        # Get all Blueprint methods that create columns
        blueprint_methods = [
            method_name
            for method_name, method in inspect.getmembers(
                Blueprint, predicate=inspect.isfunction
            )
            if not method_name.startswith("_")
            and method_name
            not in [
                "to_sql",
                "execute",
                "default",
                "nullable",
                "comment",
                "after",
                "unique",
                "partial_unique",
                "check",
                "index",
                "fulltext",
                "primary",
                "add_foreign",
                "foreign",
                "foreign_id",
                "foreign_uuid",
                "foreign_id_for",
                "references",
                "on",
                "on_delete",
                "on_update",
                "soft_deletes",
                "table_comment",
                "rename",
                "drop_column",
                "drop_index",
                "change",
                "drop_unique",
                "drop_primary",
                "drop_foreign",
                "morphs",
                "unsigned",
            ]
        ]

        # Create proxy methods for each Blueprint method
        for method_name in blueprint_methods:
            attrs[method_name] = staticmethod(FieldProxy(method_name))

        return super().__new__(cls, name, bases, attrs)
