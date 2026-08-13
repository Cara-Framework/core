"""
HTTP Input Management Module.

This module provides classes for managing HTTP request input data in the Cara framework,
implementing input handling with support for nested data, arrays, and dot notation
access.
"""

from __future__ import annotations

from typing import Any


class Input:
    """
    Single input value representation.

    This class represents a single input value from an HTTP request, providing access to both the
    input name and its value.
    """

    def __init__(self, name: str, value: Any):
        """
        Initialize an input.

        Args:
            name: Input name
            value: Input value
        """
        self.name = name
        self.value = value

    def __str__(self) -> str:
        """String representation of input."""
        return f"{self.name}={self.value}"

    def __repr__(self) -> str:
        """Debug representation of input."""
        return f"Input(name='{self.name}', value={repr(self.value)})"
