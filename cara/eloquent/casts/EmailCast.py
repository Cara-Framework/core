"""Canonical definition of ``EmailCast``."""

from __future__ import annotations

import re

from .BaseCast import BaseCast


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
