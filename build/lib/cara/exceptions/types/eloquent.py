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
