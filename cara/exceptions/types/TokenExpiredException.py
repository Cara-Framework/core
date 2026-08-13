"""TokenExpiredException."""

from __future__ import annotations

from .AuthenticationException import AuthenticationException


class TokenExpiredException(AuthenticationException):
    """
    Exception raised when an authentication token has expired.
    """

    pass
