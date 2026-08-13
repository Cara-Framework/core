"""
HTTP Header Management Module.

This module provides classes for managing HTTP headers in the Cara framework, implementing case-
insensitive header handling, header bags, and proper header formatting for ASGI compatibility.
"""

from __future__ import annotations


class Header:
    """
    Single HTTP header representation.

    This class represents a single HTTP header with proper name and value handling. It ensures
    header names are case-insensitive and values are properly encoded.
    """

    def __init__(self, name: str, value: str):
        """
        Initialize a header.

        Args:
            name: Header name
            value: Header value
        """
        self.name = name
        self.value = value

    def __str__(self) -> str:
        """String representation of header."""
        return f"{self.name}: {self.value}"

    def __repr__(self) -> str:
        """Debug representation of header."""
        return f"Header(name='{self.name}', value='{self.value}')"
