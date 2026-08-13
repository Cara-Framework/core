"""Canonical definition of ``FieldProxy``."""

from __future__ import annotations

from .FieldDefinition import FieldDefinition


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
