"""UnsupportedMediaTypeException."""

from __future__ import annotations

from .HttpException import HttpException


class UnsupportedMediaTypeException(HttpException):
    """Thrown when a non-empty request body uses an unsupported media type."""

    status_code = 415
    error_type = "unsupported_media_type"
