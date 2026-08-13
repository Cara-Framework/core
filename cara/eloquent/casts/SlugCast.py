"""Canonical definition of ``SlugCast``."""

from __future__ import annotations

import re

from .BaseCast import BaseCast


class SlugCast(BaseCast):
    """Cast for URL-friendly slug generation."""

    def get(self, value):
        """Get as URL-friendly slug."""
        if value is None:
            return None

        # Convert to lowercase and replace spaces/special chars with hyphens
        slug = str(value).lower()
        slug = re.sub(r"[^\w\s-]", "", slug)  # Remove special chars
        slug = re.sub(r"[\s_-]+", "-", slug)  # Replace spaces/underscores with hyphens
        slug = slug.strip("-")  # Remove leading/trailing hyphens

        return slug if slug else None

    def set(self, value):
        """Set with slug generation."""
        return self.get(value)
