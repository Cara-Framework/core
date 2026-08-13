"""AccountLockedException."""

from __future__ import annotations

from typing import Any

from .CaraException import CaraException


class AccountLockedException(CaraException):
    """Account temporarily locked (e.g. too many failed login attempts).

    HTTP 429 Too-Many-Requests rather than 401/403: the credentials may be
    correct — the lock is a brute-force policy, not an authorization
    decision. ``retry_after_seconds`` carries the remaining window.
    """

    is_http_exception = True
    status_code = 429

    def __init__(
        self, message: str = "Account temporarily locked", retry_after_seconds: int = 0
    ):
        super().__init__(message)
        self.retry_after_seconds = int(retry_after_seconds)

    def to_dict(self) -> dict[str, Any]:
        body: dict[str, Any] = {"error": str(self), "type": "rate_limit_exceeded"}
        if self.retry_after_seconds > 0:
            body["retry_after"] = self.retry_after_seconds
        return body
