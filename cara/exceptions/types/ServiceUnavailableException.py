"""ServiceUnavailableException."""

from __future__ import annotations

from .HttpException import HttpException


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
