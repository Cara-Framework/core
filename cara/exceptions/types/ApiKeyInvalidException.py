"""ApiKeyInvalidException."""

from __future__ import annotations

from .AuthenticationException import AuthenticationException


class ApiKeyInvalidException(AuthenticationException):
    """
    Exception raised when an API key is invalid or not found.
    """

    pass
