"""Http404Exception."""

from __future__ import annotations

from .CaraException import CaraException


class Http404Exception(CaraException):
    """
    Exception for HTTP 404 errors.
    HTTP 404 Not Found.
    """

    is_http_exception = True
    status_code = 404
