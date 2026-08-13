"""Canonical definition of ``CollectionCast``."""

from __future__ import annotations

import json

from cara.support import Collection

from .BaseCast import BaseCast
from .Collections import _write_list


class CollectionCast(BaseCast):
    """Cast for Cara Collection objects."""

    def get(self, value):
        """Get as Collection object."""
        # Import here to avoid circular imports

        if value is None:
            return None

        if hasattr(value, "__class__") and value.__class__.__name__ == "Collection":
            return value

        if isinstance(value, list):
            return Collection(value)

        if isinstance(value, str):
            try:
                parsed = json.loads(value)
                if isinstance(parsed, list):
                    return Collection(parsed)
            except ValueError, TypeError:
                pass

        return None

    def _get_as_list(self, value):
        """Fallback to list if Collection not available."""
        if value is None:
            return None

        if isinstance(value, list):
            return value

        if isinstance(value, str):
            try:
                parsed = json.loads(value)
                if isinstance(parsed, list):
                    return parsed
            except ValueError, TypeError:
                pass

        return None

    def set(self, value):
        """Preserve NULL, encode collections/lists, reject other shapes."""
        # ``Collection`` publishes ``to_array()`` (which runs ``serialize()``
        # so nested models become dicts). The probe here used to be
        # ``hasattr(value, "to_list")`` — a method cara's ``Collection`` has
        # never had — so the one input type this cast is named for fell all
        # the way through to the terminal ``return "[]"`` and the write was
        # lost without a sound.
        unwrapper = getattr(value, "to_array", None)
        if callable(unwrapper):
            value = unwrapper()

        return _write_list(value, cast_name="CollectionCast")
