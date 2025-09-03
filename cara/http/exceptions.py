"""
Http package exceptions.
"""

from cara.exceptions.types.base import CaraException

class InvalidHTTPStatusCode(CaraException):
    """Raised when someone tries to send an invalid numeric HTTP status code."""

    pass

