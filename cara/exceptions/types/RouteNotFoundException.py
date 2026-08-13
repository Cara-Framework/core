"""RouteNotFoundException."""

from __future__ import annotations

from .HttpException import HttpException


class RouteNotFoundException(HttpException):
    """Thrown when no route matches a request path."""

    status_code = 404
    error_type = "not_found"
