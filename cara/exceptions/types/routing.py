"""
Routing package exceptions.
"""

from __future__ import annotations

from cara.exceptions.types.Base import CaraException


class RouteException(CaraException):
    """A generic route-related exception (compile errors, etc.)."""

    pass


class RouteMiddlewareNotFoundException(CaraException):
    """Route middleware not found exception."""

    pass


# RouteNotFoundException moved to cara.exceptions.types.http
# Import it from there to avoid duplication.
#
# ``RouteRegistrationException`` moved to ``cara.exceptions.types.application``,
# which is where its ``ControllerMethodNotFoundException`` subclass already
# lived. Both classes existed: the barrel bound the plain one from here while
# ``ControllerMethodNotFoundException`` descended from the richer one there, so
# the two were disjoint and ``RouteResolver`` had to name both in one ``except``
# clause to catch its own errors — and ``Application.boot``'s
# ``isinstance(e, RouteRegistrationException)`` missed every missing-controller-
# method failure, reporting it as a generic startup error.


__all__ = [
    "RouteException",
    "RouteMiddlewareNotFoundException",
]
