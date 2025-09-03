"""
Routing package exceptions.
"""

from cara.exceptions.types.base import CaraException


class RouteException(CaraException):
    """A generic route-related exception (compile errors, etc.)."""

    pass


class RouteRegistrationException(CaraException):
    """Exception during route registration."""

    pass


class RouteMiddlewareNotFoundException(CaraException):
    """Route middleware not found exception."""

    pass


# RouteNotFoundException moved to cara.exceptions.types.http
# Import it from there to avoid duplication
