"""Missing-entity service error."""

from __future__ import annotations

from .ServiceException import ServiceException


class EntityNotFound(ServiceException):
    """404 — the requested entity does not exist; envelope ``type`` = ``not_found``."""

    status_code = 404
    do_not_retry: bool = True

    def __init__(self, message: str = "Not found", **extra) -> None:
        super().__init__(message, **extra)

    def to_dict(self) -> dict:
        """The error envelope body this exception renders."""
        return {"error": self.message, "type": "not_found"}


__all__ = ["EntityNotFound"]
