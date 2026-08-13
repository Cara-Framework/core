"""Canonical definition of ``URLCast``."""

from __future__ import annotations

from .BaseCast import BaseCast


class URLCast(BaseCast):
    """Cast for URL validation and normalization."""

    def get(self, value):
        """Get normalized URL."""
        if value is None:
            return None

        url = str(value).strip()

        # Basic URL validation and normalization
        if not url.startswith(("http://", "https://")):
            if url.startswith("//"):
                url = f"https:{url}"
            elif url and not url.startswith(("ftp://", "file://")):
                url = f"https://{url}"

        return url if url else None

    def set(self, value):
        """Set with URL validation."""
        return self.get(value)
