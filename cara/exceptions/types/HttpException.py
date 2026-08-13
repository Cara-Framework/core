"""HttpException."""

from __future__ import annotations

from typing import Any

from .CaraException import CaraException


class HttpException(CaraException):
    """
    Base for custom HTTP exceptions.

    Simple HTTP exception that users can easily extend.

    Usage:
        # Basic usage
        raise HttpException("The request could not be completed")

        # With status code
        raise HttpException("Not found", status_code=404)

        # With extra data
        raise HttpException("Payment failed", status_code=422, gateway="stripe")

        # Create custom exception class
        class PaymentException(HttpException):
            status_code = 402
    """

    is_http_exception = True
    status_code = 500
    error_type = "http_error"

    def __init__(
        self, message: str = "An error occurred", status_code: int | None = None, **kwargs
    ):
        super().__init__(message)
        # Use provided status_code or fall back to class attribute
        if status_code is not None:
            self.status_code = status_code
        # Set any additional attributes
        for key, value in kwargs.items():
            setattr(self, key, value)

    def to_dict(self) -> dict[str, Any]:
        """Convert exception to dictionary for JSON response.

        Canonical error shape: ``{error, type, ...optional context}``.

        ``type`` is the machine-readable discriminator clients branch on.
        Every framework-raised error includes it so clients never need to
        classify human-readable copy.
        """
        result: dict[str, Any] = {
            "error": str(self),
            "type": self.error_type,
        }

        # Add any extra attributes that don't start with underscore.
        # ``type`` was already set above; never let a subclass override
        # it via __dict__ (would defeat the canonical-shape guarantee).
        for key, value in self.__dict__.items():
            if not key.startswith("_") and key not in [
                "args",
                "status_code",
                "is_http_exception",
                "type",
            ]:
                result[key] = value

        return result
