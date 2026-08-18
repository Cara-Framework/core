"""Standalone scalar and composite foreign-key call discovery."""

from __future__ import annotations

import ast


def _extract_separate_foreign_key_definition(
    self, call_node: ast.Call
) -> dict | None:
    """Extract ``field.foreign(...).references(...).on(...)`` metadata."""

    field_name = None
    name = None
    references = None
    on_table = None
    on_delete = None
    on_delete_columns = None
    on_update = None

    current = call_node
    while isinstance(current, ast.Call) and isinstance(current.func, ast.Attribute):
        method_name = current.func.attr
        if method_name == "foreign" and current.args:
            field_name = self._foreign_key_arg(current.args[0])
            if len(current.args) > 1 and isinstance(current.args[1], ast.Constant):
                name = current.args[1].value
            for keyword in current.keywords:
                if keyword.arg == "name" and isinstance(keyword.value, ast.Constant):
                    name = keyword.value.value
        elif method_name == "references" and current.args:
            references = self._foreign_key_arg(current.args[0])
        elif method_name == "on" and current.args:
            if isinstance(current.args[0], ast.Constant):
                on_table = current.args[0].value
        elif method_name in ("on_delete", "onDelete") and current.args:
            if isinstance(current.args[0], ast.Constant):
                on_delete = current.args[0].value
            if len(current.args) > 1:
                on_delete_columns = self._foreign_key_arg(current.args[1])
            for keyword in current.keywords:
                if keyword.arg == "columns":
                    on_delete_columns = self._foreign_key_arg(keyword.value)
        elif (
            method_name in ("on_update", "onUpdate")
            and current.args
            and isinstance(current.args[0], ast.Constant)
        ):
            on_update = current.args[0].value
        current = current.func.value

    is_composite = isinstance(field_name, list)
    if is_composite and (
        not on_table
        or not isinstance(references, list)
        or len(references) != len(field_name)
    ):
        return None
    if not is_composite and not (field_name and on_table):
        return None

    return {
        "composite": is_composite,
        "field": field_name,
        "name": name,
        "references": references if is_composite else references or "id",
        "on": on_table,
        "on_delete": on_delete,
        **(
            {"on_delete_columns": on_delete_columns}
            if on_delete_columns
            else {}
        ),
        "on_update": on_update,
    }
