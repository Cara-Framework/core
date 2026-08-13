"""BadRequestException."""

from __future__ import annotations

from .HttpException import HttpException


class BadRequestException(HttpException):
    """Thrown when the request is malformed (HTTP 400)."""

    status_code = 400
    error_type = "bad_request"
