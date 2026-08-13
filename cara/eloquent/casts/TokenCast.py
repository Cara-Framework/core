"""Canonical definition of ``TokenCast``."""

from __future__ import annotations

import secrets
from typing import Any

from cara.exceptions import InvalidArgumentException

from .BaseCast import BaseCast


class TokenCast(BaseCast):
    """Cast for generating and validating tokens."""

    def __init__(self, length: int = 32):
        if length <= 0:
            raise InvalidArgumentException("Token length must be positive")
        self.length = length

    def get(self, value: Any) -> Any:
        """Return token as-is."""
        return value

    def set(self, value: Any) -> str:
        """Generate a token when the value is None, otherwise coerce to str."""
        if value is None:
            return self._generate_token()
        return str(value)

    def _generate_token(self) -> str:
        return secrets.token_urlsafe(self.length)
