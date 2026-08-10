"""
HTTP Exception Type for the Cara framework.

This module defines exception types related to HTTP operations.
"""

from __future__ import annotations

from typing import Any

from .Base import CaraException


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


class BadRequestException(HttpException):
    """Thrown when the request is malformed (HTTP 400)."""

    status_code = 400
    error_type = "bad_request"


class PayloadTooLargeException(HttpException):
    """Thrown when a request body exceeds its allowed size (HTTP 413)."""

    status_code = 413
    error_type = "payload_too_large"


class RouteNotFoundException(HttpException):
    """Thrown when no route matches a request path."""

    status_code = 404
    error_type = "not_found"


class MethodNotAllowedException(HttpException):
    """Thrown when the route exists but the HTTP method is not permitted.

    Per RFC 9110 §15.5.6 ("405 Method Not Allowed"): the origin server
    MUST generate an ``Allow`` header field in a 405 response containing
    a list of the target resource's currently supported methods.

    The caller (``Router.find``) passes the allowed-method list via the
    ``allowed`` kwarg. The default exception handler reads it back via
    ``getattr(exception, "allowed", None)`` and emits the ``Allow``
    header on the response. Pre-fix the kwarg didn't exist and the
    framework's 405 responses violated the RFC — load balancers and
    fetch clients (which rely on the header to discover supported
    methods) had no signal beyond the human-readable message.
    """

    status_code = 405
    error_type = "method_not_allowed"


class InvalidCursor(HttpException, ValueError):
    """A pagination cursor is malformed, tampered with, or belongs to another query.

    A tampered cursor is bad CLIENT input, so it answers 422 like any
    other validation failure. It used to be a bare ``ValueError`` living
    in ``cara.http.Cursor``, outside the taxonomy and therefore without a
    ``status_code`` — ``get_status_code`` fell through to "default to 500
    for unknown exceptions", so ``QueryBuilder.cursor_paginate`` turned an
    edited query string into a 500 with an ERROR-level traceback: a client
    fault recorded as a server fault, burning the error budget and paging
    oncall. Both products had to restate the translation themselves.

    Also a ``ValueError`` — a malformed cursor IS a value error, and the
    call sites that catch ``(InvalidCursor, TypeError, ValueError)`` around
    cursor decoding stay correct either way.
    """

    status_code = 422
    error_type = "validation_error"


class Http404Exception(CaraException):
    """
    Exception for HTTP 404 errors.
    HTTP 404 Not Found.
    """

    is_http_exception = True
    status_code = 404


class ResponseException(CaraException):
    """Thrown if there's a failure writing to the response stream."""

    pass


class ServiceUnavailableException(HttpException):
    """Thrown when a dependency the request needs is temporarily down.

    Distinct from 500: the server itself isn't faulting, an upstream
    is. Clients can retry with backoff. ``retry_after`` (seconds) is
    surfaced both in the JSON envelope and the ``Retry-After`` header
    so callers don't have to parse the body to know when to come back.
    """

    status_code = 503
    error_type = "service_unavailable"

    def __init__(
        self,
        message: str = "Service temporarily unavailable",
        retry_after: int | None = None,
        **kwargs,
    ):
        super().__init__(message, **kwargs)
        if retry_after is not None:
            self.retry_after = retry_after


__all__ = [
    "BadRequestException",
    "Http404Exception",
    "HttpException",
    "InvalidCursor",
    "MethodNotAllowedException",
    "PayloadTooLargeException",
    "ResponseException",
    "RouteNotFoundException",
    "ServiceUnavailableException",
]
