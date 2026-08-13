"""Canonical definition of ``PhoneCast``."""

from __future__ import annotations

import re

from .BaseCast import BaseCast


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
