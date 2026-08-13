"""TokenInvalidException."""

from __future__ import annotations

from .AuthenticationException import AuthenticationException


class TokenInvalidException(AuthenticationException):
    """
    Exception raised when an authentication token is invalid.
    """

    pass
