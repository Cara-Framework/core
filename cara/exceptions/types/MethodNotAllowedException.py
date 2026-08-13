"""MethodNotAllowedException."""

from __future__ import annotations

from .HttpException import HttpException


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
