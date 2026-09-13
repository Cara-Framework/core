"""TooManyRequestsException."""

from __future__ import annotations

from typing import Any

from .CaraException import CaraException


class TooManyRequestsException(CaraException):
    """A rate limit refused the request — HTTP 429.

    Raised, not answered in place, so a throttled request leaves by the one
    error path every other refusal uses: the canonical envelope, CORS headers
    on the error response (a browser that cannot read ``Retry-After`` retries
    blind), and ``Retry-After`` lifted from ``retry_after``.
    ``response_headers`` carries the IETF ``RateLimit`` / ``RateLimit-Policy``
    pair describing the policy that refused it.
    """

    is_http_exception = True
    status_code = 429

    def __init__(
        self,
        message: str = "Too Many Requests",
        *,
        retry_after: int,
        response_headers: dict[str, str] | None = None,
    ):
        super().__init__(message)
        self.retry_after = int(retry_after)
        self.response_headers = dict(response_headers or {})

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": str(self),
            "type": "rate_limit_exceeded",
            "retry_after": self.retry_after,
        }
