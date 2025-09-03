"""
HTTP Exception Type for the Cara framework.

This module defines exception types related to HTTP operations.
"""

from typing import Any, Dict

from .base import CaraException


class HttpException(CaraException):
    """
    Base for custom HTTP exceptions.

    Simple HTTP exception that users can easily extend.

    Usage:
        # Basic usage
        raise HttpException("Something went wrong")

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

    def __init__(
        self, message: str = "An error occurred", status_code: int = None, **kwargs
    ):
        super().__init__(message)
        # Use provided status_code or fall back to class attribute
        if status_code is not None:
            self.status_code = status_code
        # Set any additional attributes
        for key, value in kwargs.items():
            setattr(self, key, value)

    def to_dict(self) -> Dict[str, Any]:
        """Convert exception to dictionary for JSON response."""
        result = {
            "error": str(self),
        }

        # Add any extra attributes that don't start with underscore
        for key, value in self.__dict__.items():
            if not key.startswith("_") and key not in [
                "args",
                "status_code",
                "is_http_exception",
            ]:
                result[key] = value

        return result


class BadRequestException(HttpException):
    """Thrown when the request is malformed (HTTP 400)."""

    status_code = 400


class RouteNotFoundException(HttpException):
    """Thrown when no route matches a request path."""

    status_code = 404


class MethodNotAllowedException(HttpException):
    """Thrown when the route exists but the HTTP method is not permitted."""

    status_code = 405


class RouteMiddlewareNotFoundException(CaraException):
    """Thrown if a route's middleware alias cannot be resolved."""

    pass


class ResponseException(CaraException):
    """Thrown if there's a failure writing to the response stream."""

    pass
