"""Authentication service error."""

from __future__ import annotations

from .ServiceException import ServiceException


class Unauthorized(ServiceException):
    """401 — missing or bad credentials; same token as Cara's ``AuthenticationException``; envelope ``type`` = ``authentication_error``."""

    status_code = 401
    do_not_retry: bool = True

    def __init__(self, message: str = "Unauthorized", **extra) -> None:
        super().__init__(message, **extra)

    def to_dict(self) -> dict:
        """The error envelope body this exception renders."""
        return {"error": self.message, "type": "authentication_error"}


__all__ = ["Unauthorized"]
