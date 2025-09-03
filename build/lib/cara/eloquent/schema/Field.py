"""Field definitions for Eloquent models using Blueprint."""

import inspect

from .Blueprint import Blueprint


class FieldDefinition:
    """Represents a field definition that uses Blueprint methods."""

    def __init__(self, blueprint_method, *args, **kwargs):
        self.blueprint_method = blueprint_method
        self.args = args
        self.kwargs = kwargs
        self._modifiers = []

    def nullable(self):
        """Make field nullable."""
        self._modifiers.append("nullable")
        return self

    def default(self, value):
        """Set default value."""
        self._modifiers.append(("default", value))
        return self

    def to_blueprint_call(self, field_name):
        """Convert field definition to Blueprint method call string."""
        # Build base method call
        args_str = f'"{field_name}"'
        if self.args:
            args_str += ", " + ", ".join(str(arg) for arg in self.args)

        # Add keyword arguments
        if self.kwargs:
            kwargs_str = ", ".join(f"{k}={v}" for k, v in self.kwargs.items())
            args_str += ", " + kwargs_str

        call = f"table.{self.blueprint_method}({args_str})"

        # Add modifiers
        for modifier in self._modifiers:
            if isinstance(modifier, tuple):
                method_name, value = modifier
                if isinstance(value, str):
                    call += f'.{method_name}("{value}")'
                elif isinstance(value, bool):
                    call += f".{method_name}({str(value)})"
                else:
                    call += f".{method_name}({value})"
            else:
                call += f".{modifier}()"

        return call


class FieldProxy:
    """Proxy that wraps Blueprint methods for field definitions."""

    def __init__(self, method_name):
        self.method_name = method_name

    def __call__(self, *args, **kwargs):
        """Create FieldDefinition when called."""
        # Extract modifier kwargs
        nullable = kwargs.pop("nullable", False)
        default = kwargs.pop("default", None)

        # Create field definition
        field = FieldDefinition(self.method_name, *args, **kwargs)

        # Apply modifiers
        if nullable:
            field.nullable()
        if default is not None:
            field.default(default)

        return field


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


class Field(metaclass=FieldMeta):
    """Field factory that automatically proxies all Blueprint column methods."""

    pass
