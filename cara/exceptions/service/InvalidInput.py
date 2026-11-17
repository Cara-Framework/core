"""Malformed-input service error."""

from __future__ import annotations

from .ServiceException import ServiceException


class InvalidInput(ServiceException, ValueError):
    """400 — malformed arguments / missing required field; envelope ``type`` = ``invalid_input``.

    Distinct from the 422 ``validation_error`` (field-level rule misses) and
    from the generic ``request_error`` catch-all. Also a ``ValueError``: it IS
    a bad value, and code guarding a parse with ``except ValueError`` must
    see it as one.
    """

    status_code = 400
    do_not_retry: bool = True

    def __init__(self, message: str = "Bad request", **extra) -> None:
        super().__init__(message, **extra)

    def to_dict(self) -> dict:
        """The error envelope body this exception renders."""
        return {"error": self.message, "type": "invalid_input"}


__all__ = ["InvalidInput"]
