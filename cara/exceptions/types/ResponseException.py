"""ResponseException."""

from __future__ import annotations

from .CaraException import CaraException


class ResponseException(CaraException):
    """Thrown if there's a failure writing to the response stream."""

    pass
