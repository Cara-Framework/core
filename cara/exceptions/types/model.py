"""Model / ORM-related exceptions."""

from .base import CaraException


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


class QueryException(ModelException):
    """Thrown when a SQL query fails (syntax, constraint, etc.)."""

    pass


class MultipleRecordsFoundException(ModelException):
    """Thrown when a "firstOrFail" style query unexpectedly returns many records."""

    pass


class InvalidArgumentException(ModelException):
    """Generic invalid-argument exception within the ORM layer."""

    pass
