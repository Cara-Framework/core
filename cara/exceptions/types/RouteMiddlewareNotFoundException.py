"""RouteMiddlewareNotFoundException."""

from __future__ import annotations

from cara.exceptions.types.CaraException import CaraException


class RouteMiddlewareNotFoundException(CaraException):
    """Route middleware not found exception."""

    pass
