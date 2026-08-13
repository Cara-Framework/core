"""Canonical definition of ``ArrayCast``."""

from __future__ import annotations

import json

from .BaseCast import BaseCast
from .Collections import _write_list


class ArrayCast(BaseCast):
    """Cast to/from Python arrays with JSON storage."""

    def __init__(self, item_cast: str | None = None):
        self.item_cast = item_cast

    def get(self, value):
        """Get as Python list."""
        if value is None:
            return None

        if isinstance(value, list):
            return value

        if isinstance(value, str):
            try:
                parsed = json.loads(value)
                if not isinstance(parsed, list):
                    return None
                return parsed
            except ValueError, TypeError:
                return None

        return None

    def set(self, value):
        """Preserve NULL, encode lists, and reject every other shape."""
        return _write_list(value, cast_name="ArrayCast")
