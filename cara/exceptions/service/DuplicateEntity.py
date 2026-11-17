"""Unique-key collision service error."""

from __future__ import annotations

from .ServiceException import ServiceException


class DuplicateEntity(ServiceException):
    """422 — unique-key collision, told apart from ``ValidationFailed`` by its token; envelope ``type`` = ``duplicate_entity``."""

    status_code = 422
    do_not_retry: bool = True

    def __init__(self, message: str = "Duplicate entry", **extra) -> None:
        super().__init__(message, **extra)

    def to_dict(self) -> dict:
        """The error envelope body this exception renders."""
        return {"error": self.message, "type": "duplicate_entity"}


__all__ = ["DuplicateEntity"]
