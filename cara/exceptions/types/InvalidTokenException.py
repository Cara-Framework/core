"""InvalidTokenException."""

from __future__ import annotations

from .AuthenticationException import AuthenticationException


class InvalidTokenException(AuthenticationException):
    """Thrown when a JWT or session token is invalid."""

    pass
