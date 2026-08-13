"""PayloadTooLargeException."""

from __future__ import annotations

from .HttpException import HttpException


class PayloadTooLargeException(HttpException):
    """Thrown when a request body exceeds its allowed size (HTTP 413)."""

    status_code = 413
    error_type = "payload_too_large"
