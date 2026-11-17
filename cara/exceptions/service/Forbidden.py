"""Authorization service error."""

from __future__ import annotations

from .ServiceException import ServiceException


class Forbidden(ServiceException):
    """403 — authenticated but not allowed; same token as Cara's ``AuthorizationException``; envelope ``type`` = ``authorization_error``."""

    status_code = 403
    do_not_retry: bool = True

    def __init__(self, message: str = "Forbidden", **extra) -> None:
        super().__init__(message, **extra)

    def to_dict(self) -> dict:
        """The error envelope body this exception renders."""
        return {"error": self.message, "type": "authorization_error"}


__all__ = ["Forbidden"]
