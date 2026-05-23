"""ORM / Eloquent-style exceptions."""

from .base import CaraException


class ORMException(CaraException):
    """Base for ORM-related errors."""

    pass


class DriverNotFoundException(ORMException):
    """Exception raised when a database driver is not found."""

    pass


class ModelNotFoundException(ORMException):
    """
    Exception raised when a model is not found.
    HTTP 404 Not Found.
    """

    is_http_exception = True
    status_code = 404


class HTTP404Exception(CaraException):
    """
    Exception for HTTP 404 errors.
    HTTP 404 Not Found.
    """

    is_http_exception = True
    status_code = 404


class ConnectionNotRegisteredException(ORMException):
    """Exception raised when a database connection is not registered."""

    pass


class QueryException(ORMException):
    """Exception raised when a database query fails."""

    pass


class MultipleRecordsFoundException(ORMException):
    """Exception raised when multiple records are found when expecting one."""

    pass


class InvalidArgumentException(ORMException):
    """Exception raised when an invalid argument is provided."""

    pass


class DatabaseUnavailableException(ORMException):
    """Postgres is unreachable, connection was refused, or the pool was
    exhausted before a slot could be acquired.

    Distinct from ``QueryException`` (a bad query) — this is the
    "the database isn't answering" path. Maps to HTTP 503 so callers
    (and load balancers) can distinguish it from a 500 application
    fault and retry without alarming oncall.
    """

    is_http_exception = True
    status_code = 503

    def __init__(
        self,
        message: str = "Database temporarily unavailable",
        retry_after: int | None = None,
    ):
        super().__init__(message)
        if retry_after is not None:
            self.retry_after = retry_after


__all__ = [
    "ORMException",
    "DriverNotFoundException",
    "ModelNotFoundException",
    "HTTP404Exception",
    "ConnectionNotRegisteredException",
    "QueryException",
    "MultipleRecordsFoundException",
    "InvalidArgumentException",
    "DatabaseUnavailableException",
]
