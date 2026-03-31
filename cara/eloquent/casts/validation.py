"""
Validation Cast Types for Cara ORM

Provides validation and normalization for common data formats.
"""

import re

from .base import BaseCast


class EmailCast(BaseCast):
    """Cast for email validation and normalization."""

    # Basic email regex pattern
    EMAIL_PATTERN = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")

    def get(self, value):
        """Get normalized email."""
        if value is None:
            return None

        email = str(value).strip().lower()

        # Basic email validation
        if self.EMAIL_PATTERN.match(email):
            return email

        return None

    def set(self, value):
        """Set with email validation."""
        return self.get(value)


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
                url = "https:" + url
            elif url and not url.startswith(("ftp://", "file://")):
                url = "https://" + url

        return url if url else None

    def set(self, value):
        """Set with URL validation."""
        return self.get(value)


class UUIDCast(BaseCast):
    """Cast for UUID values with validation."""

    def get(self, value):
        """Get as string UUID."""
        if value is None:
            return None

        # Validate UUID format
        try:
            import uuid

            uuid_obj = uuid.UUID(str(value))
            return str(uuid_obj)
        except (ValueError, TypeError, ImportError):
            return None

    def set(self, value):
        """Set UUID with validation."""
        if value is None:
            return None

        try:
            import uuid

            if hasattr(value, "__class__") and value.__class__.__name__ == "UUID":
                return str(value)

            uuid_obj = uuid.UUID(str(value))
            return str(uuid_obj)
        except (ValueError, TypeError, ImportError):
            return None


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


class PhoneCast(BaseCast):
    """Cast for phone number normalization."""

    def get(self, value):
        """Get normalized phone number."""
        if value is None:
            return None

        # Remove all non-digit characters except +
        phone = re.sub(r"[^\d+]", "", str(value))

        # Basic phone validation (at least 10 digits)
        if len(phone.replace("+", "")) >= 10:
            return phone

        return None

    def set(self, value):
        """Set with phone normalization."""
        return self.get(value)
