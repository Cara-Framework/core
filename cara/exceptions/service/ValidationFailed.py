"""Service-layer validation error with the canonical 422 envelope."""

from __future__ import annotations

from .ServiceException import ServiceException


class ValidationFailed(ServiceException):
    """422 — service-layer validation miss; envelope ``type`` = ``validation_error``.

    Raised from the SERVICE / repository layer for a domain-rule miss.
    Request-field validation raises ``ValidationException`` (``FormRequest``);
    both serialise to the SAME 422 envelope, so clients switch on ``type``
    identically — the split is only about where the check lives.

    Accepts a bare human message or a ``{field: [msg, ...]}`` dict from the
    validator: the dict lands on ``self.errors`` and its first message becomes
    ``self.message`` for ``str(exc)`` callers.
    """

    status_code = 422
    do_not_retry: bool = True

    def __init__(self, message="Validation failed", **extra) -> None:
        errors: dict = {}
        if isinstance(message, dict):
            errors = {
                str(key): (
                    list(value) if isinstance(value, (list, tuple)) else [str(value)]
                )
                for key, value in message.items()
            }
            message = next(
                (
                    str(field_errors[0])
                    for field_errors in errors.values()
                    if field_errors
                ),
                "Validation failed",
            )
        super().__init__(message, **extra)
        self.errors = errors

    @classmethod
    def generic(cls, message: str) -> ValidationFailed:
        """Create a validation error on the generic ``_`` field."""
        return cls({"_": [message]})

    @classmethod
    def field(cls, field_name: str, message: str) -> ValidationFailed:
        """Create a validation error on a specific field."""
        return cls({field_name: [message]})

    def to_dict(self) -> dict:
        """The error envelope body; mirrors ``ValidationException.to_dict``."""
        response: dict = {"error": self.message, "type": "validation_error"}
        if self.errors:
            response["errors"] = self.errors
            response["meta"] = {
                "total_errors": sum(len(value) for value in self.errors.values()),
                "failed_fields": [key for key, value in self.errors.items() if value],
            }
        return response


__all__ = ["ValidationFailed"]
