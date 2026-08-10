"""
Collection Cast Types for Cara ORM

Handles arrays, lists, and Cara Collection objects.
"""

from __future__ import annotations

import json

from .base import BaseCast


def _write_list(value, *, cast_name: str, category: str):
    """The write-side coercion shared by every list-shaped cast in this file.

    It lives in one place because it was fixed in one place and not the
    other. ``ArrayCast.set`` learned to preserve ``None`` as SQL NULL;
    ``CollectionCast.set``, registered right beside it, kept returning the
    literal ``"[]"`` — so a nullable column written through the "collection"
    cast still split "no value" from "empty value", ``WHERE col IS NULL``
    still missed every such row, and ``col = '[]'::jsonb`` still matched them
    all. A copy has no way to learn that the original changed.

    ``None`` → ``None``: unknown stays unknown.
    Non-list → ``"[]"`` and a WARNING: the value is a caller bug and the
    graceful fallback is the historical contract, but dropping a write in
    total silence is not — ops must be able to see it.
    """
    if value is None:
        return None

    if not isinstance(value, list):
        try:
            from cara.facades import Log

            Log.warning(
                "%s: dropped %s input (repr=%s); expected list — storing as '[]'",
                cast_name,
                type(value).__name__,
                value,
                category=category,
            )
        except Exception:
            # Facade not bound (unit-test boot order, etc.) — fall back to
            # stdlib logging so the warning still lands in test capture and
            # any plain Python harness.
            import logging

            logging.getLogger(f"cara.{category}").warning(
                "%s: dropped %s input (repr=%r); expected list — storing as '[]'",
                cast_name,
                type(value).__name__,
                value,
            )
        return "[]"

    return json.dumps(value, default=str)


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
            except ValueError, TypeError:
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
        return _write_list(value, cast_name="ArrayCast", category="cast.array")


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
            except ValueError, TypeError:
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
            except ValueError, TypeError:
                pass

        return []

    def set(self, value):
        """Set from Collection or list.

        Shares ``ArrayCast``'s write rule, so ``None`` is SQL NULL here too.
        This cast used to return the literal ``"[]"`` for ``None`` — the
        exact NULL drift ``ArrayCast.set`` documents as fixed, sitting
        unfixed in the same file — and dropped a non-list write with no log
        at all.

        ``get(None)`` still returns ``Collection([])``: as with
        ``ArrayCast``, the read-side fallback is sanctioned and the hazard
        was write-side only.
        """
        # ``Collection`` publishes ``to_array()`` (which runs ``serialize()``
        # so nested models become dicts). The probe here used to be
        # ``hasattr(value, "to_list")`` — a method cara's ``Collection`` has
        # never had — so the one input type this cast is named for fell all
        # the way through to the terminal ``return "[]"`` and the write was
        # lost without a sound. ``to_list`` is kept for foreign
        # collection-likes that do publish it.
        for unwrap in ("to_array", "to_list"):
            unwrapper = getattr(value, unwrap, None)
            if callable(unwrapper):
                value = unwrapper()
                break

        return _write_list(value, cast_name="CollectionCast", category="cast.collection")
