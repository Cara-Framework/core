"""
Collection Cast Types for Cara ORM

Handles arrays, lists, and Cara Collection objects.
"""

import json
from typing import Optional

from .base import BaseCast


class ArrayCast(BaseCast):
    """Cast to/from Python arrays with JSON storage."""

    def __init__(self, item_cast: Optional[str] = None):
        self.item_cast = item_cast

    def get(self, value):
        """Get as Python list."""
        if value is None:
            return []

        if isinstance(value, list):
            return value

        if isinstance(value, str):
            try:
                parsed = json.loads(value)
                if not isinstance(parsed, list):
                    return []
                return parsed
            except (ValueError, TypeError):
                return []

        return []

    def set(self, value):
        """Set as JSON string."""
        if value is None:
            return "[]"

        if not isinstance(value, list):
            return "[]"

        return json.dumps(value, default=str)


class CollectionCast(BaseCast):
    """Cast for Cara Collection objects."""

    def get(self, value):
        """Get as Collection object."""
        # Import here to avoid circular imports
        try:
            from cara.support.Collection import Collection
        except ImportError:
            # Fallback to list if Collection not available
            return self._get_as_list(value)

        if value is None:
            return Collection([])

        if hasattr(value, "__class__") and value.__class__.__name__ == "Collection":
            return value

        if isinstance(value, list):
            return Collection(value)

        if isinstance(value, str):
            try:
                parsed = json.loads(value)
                if isinstance(parsed, list):
                    return Collection(parsed)
            except (ValueError, TypeError):
                pass

        return Collection([])

    def _get_as_list(self, value):
        """Fallback to list if Collection not available."""
        if value is None:
            return []

        if isinstance(value, list):
            return value

        if isinstance(value, str):
            try:
                parsed = json.loads(value)
                if isinstance(parsed, list):
                    return parsed
            except (ValueError, TypeError):
                pass

        return []

    def set(self, value):
        """Set from Collection or list."""
        if value is None:
            return "[]"

        # Handle Collection objects
        if hasattr(value, "to_list"):
            return json.dumps(value.to_list(), default=str)

        if isinstance(value, list):
            return json.dumps(value, default=str)

        return "[]"
