"""Canonical definition of ``UUIDCast``."""

from __future__ import annotations

import uuid

from .BaseCast import BaseCast


class UUIDCast(BaseCast):
    """Cast for UUID values with validation."""

    def get(self, value):
        """Get as string UUID."""
        if value is None:
            return None

        # Validate UUID format
        try:
            uuid_obj = uuid.UUID(str(value))
            return str(uuid_obj)
        except ValueError, TypeError, ImportError:
            return None

    def set(self, value):
        """Set UUID with validation."""
        if value is None:
            return None

        try:
            if hasattr(value, "__class__") and value.__class__.__name__ == "UUID":
                return str(value)

            uuid_obj = uuid.UUID(str(value))
            return str(uuid_obj)
        except ValueError, TypeError, ImportError:
            return None
