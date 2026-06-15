"""
Collection Cast Types for Cara ORM

Handles arrays, lists, and Cara Collection objects.
"""

from __future__ import annotations

import json

from .base import BaseCast


class ArrayCast(BaseCast):
    """Cast to/from Python arrays with JSON storage."""

    def __init__(self, item_cast: str | None = None):
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
        """Set as JSON string.

        ``None`` is preserved as ``None`` (SQL NULL). Pre-fix the
        cast returned the literal ``"[]"`` string for ``None``,
        causing NULL drift on nullable array columns:
        ``WHERE col IS NULL`` queries then missed every row written
        through this path while ``col = '[]'::jsonb`` matched them
        all — a silent split between "no value" and "empty value"
        that broke facet aggregation, sitemap filters, and any
        downstream predicate that branched on NULL-ness.

        ``get(None)`` still returns ``[]`` on purpose — callers
        iterate the read-side result without guards. The hazard
        was the write-side coercion, not the read-side fallback.

        Non-list inputs (a dict, a number, a string passed where a
        list was expected) are a caller bug. Historically the cast
        silently swallowed them and stored ``"[]"`` with no signal;
        the fix keeps the graceful ``"[]"`` fallback for backwards
        compatibility but logs a warning so ops can see the
        dropped write in observability.
        """
        if value is None:
            return None

        if not isinstance(value, list):
            # Caller bug — dropping a non-list to ``"[]"`` is data
            # loss. Log so the bug surfaces; preserve the legacy
            # return value so existing callers don't break.
            try:
                from cara.facades import Log

                Log.warning("ArrayCast: dropped %s input (repr=%s); expected list — storing as '[]'", type(value).__name__, value, category='cast.array')
            except Exception:
                # Facade not bound (unit-test boot order, etc.) —
                # fall back to stdlib logging so the warning still
                # lands in test capture and any plain Python harness.
                import logging

                logging.getLogger("cara.cast.array").warning(
                    "ArrayCast: dropped %s input (repr=%r); "
                    "expected list — storing as '[]'",
                    type(value).__name__,
                    value,
                )
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
