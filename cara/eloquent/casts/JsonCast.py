"""Canonical definition of ``JsonCast``."""

from __future__ import annotations

import json

from cara.support import json_dumps

from .BaseCast import BaseCast


class JsonCast(BaseCast):
    """Cast to/from JSON."""

    def get(self, value):
        """Get as parsed JSON."""
        if value is None:
            return None
        if isinstance(value, str):
            # Empty/whitespace strings are null-equivalent — they
            # come back as NULL from many DB schemas via empty-string
            # default. Treat them as None instead of failing JSON
            # parse and silently producing None anyway.
            if not value.strip():
                return None
            try:
                return json.loads(value)
            except ValueError, TypeError:
                return None
        return value

    def set(self, value):
        """Set as JSON string.

        Empty string is null-equivalent — previously ``set("")`` fell
        through to ``json.dumps("")`` and produced the literal JSON
        string ``'""'``. The next ``get()`` then returned the empty
        string instead of ``None``, breaking ``if obj.field is None``
        checks all over the call site. Now empty becomes NULL.
        """
        if value is None:
            return None
        if isinstance(value, str) and not value.strip():
            return None

        if isinstance(value, str):
            try:
                json.loads(value)
                return value
            except ValueError, TypeError:
                return json_dumps(value)
        return json_dumps(value)
