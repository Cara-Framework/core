"""
Storage Interface for the Cara framework.

This module defines the contract that any storage driver must implement, specifying required methods
for storage operations.
"""

from typing import Protocol, Any


class Storage(Protocol):
    """Contract that any storage driver must implement."""

    def put(self, key: str, data: bytes) -> None:
        """Store raw bytes under `key`."""

    def get(self, key: str) -> bytes:
        """
        Retrieve bytes for `key`.

        Raise KeyNotFoundException if not found.
        """

    def delete(self, key: str) -> bool:
        """
        Delete `key`.

        Return True if deleted, False if not found.
        """

    def exists(self, key: str) -> bool:
        """Return True if `key` exists, False otherwise."""
