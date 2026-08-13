"""
Invalid HTTP status-code exception.
"""

from __future__ import annotations

from cara.exceptions import CaraException


class InvalidHTTPStatusCode(CaraException):
    """Raised when someone tries to send an invalid numeric HTTP status code."""

    pass
