"""ModelNotFoundException."""

from __future__ import annotations

from .ModelException import ModelException


class ModelNotFoundException(ModelException):
    """
    Thrown when an ORM query (e.g. findOrFail) does not locate a record.

    Should map to HTTP 404 in the global handler.
    """

    is_http_exception = True
    status_code = 404

    def __init__(self, message: str = "Not found"):
        # A bare model miss must still serialize as a precise 404.
        super().__init__(message)

    def to_dict(self) -> dict:
        """Emit the same ``not_found`` type token that the service-layer
        ``EntityNotFound`` uses, so clients keying on ``type`` get a
        consistent discriminator regardless of which layer raised the 404.
        """
        return {"error": str(self) or "Not found", "type": "not_found"}
