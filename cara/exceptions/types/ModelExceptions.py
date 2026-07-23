"""Model / ORM-related exceptions."""

from __future__ import annotations

from .Base import CaraException


class ModelException(CaraException):
    """Base for all Eloquent/ORM exceptions."""

    pass


class DriverNotFoundException(ModelException):
    """Thrown when a database driver cannot be found."""

    pass


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


class QueryException(ModelException):
    """Thrown when a SQL query fails (syntax, constraint, etc.)."""

    pass


class MultipleRecordsFoundException(ModelException):
    """Thrown when a "firstOrFail" style query unexpectedly returns many records."""

    pass


class LazyLoadingViolation(ModelException):
    """Thrown when an accidental lazy-load is caught by the strict guard.

    Opt-in via ``Model.prevent_lazy_loading()`` (OFF by default). When
    enabled, accessing an un-eager-loaded relationship on a model that
    came from a multi-row fetch raises this instead of silently issuing
    an N+1 query — surfacing the missing ``.with_(...)`` in dev/test
    before it reaches production.
    """

    pass


class InvalidArgumentException(ModelException, ValueError):
    """Generic invalid-argument exception.

    Also subclasses the builtin ``ValueError``: an invalid argument IS a
    value error, and a large body of callers + tests catch these with
    ``pytest.raises(ValueError)`` / ``except ValueError``. The framework
    raises ``InvalidArgumentException`` (for a precise, catchable type)
    from spots that historically raised a bare ``ValueError`` — making it
    a ``ValueError`` subclass keeps every one of those call sites working
    whether they catch the specific type or the builtin. MRO is well-formed
    (``ModelException`` → ``CaraException`` → ``Exception`` ← ``ValueError``).
    """

    pass


__all__ = [
    "ModelException",
    "DriverNotFoundException",
    "ModelNotFoundException",
    "QueryException",
    "MultipleRecordsFoundException",
    "LazyLoadingViolation",
    "InvalidArgumentException",
]
