"""Stale-write / domain-conflict service error."""

from __future__ import annotations

from .ServiceException import ServiceException


class ConflictException(ServiceException):
    """409 — stale write or optimistic-concurrency miss; envelope ``type`` = ``conflict``."""

    status_code = 409
    do_not_retry: bool = True

    def __init__(self, message: str = "Conflict", **extra) -> None:
        super().__init__(message, **extra)

    def to_dict(self) -> dict:
        """The error envelope body this exception renders."""
        return {"error": self.message, "type": "conflict"}


__all__ = ["ConflictException"]
