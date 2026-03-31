"""Driver-related exceptions (queue drivers, logging drivers, etc.)."""

from .base import CaraException


class DriverException(CaraException):
    """Base for all driver-related exceptions."""

    pass


class DriverNotFoundException(DriverException):
    """Thrown when a driver class cannot be found."""

    pass


class DriverLibraryNotFoundException(DriverException):
    """Thrown when a native driver library is missing on the system."""

    pass


class QueueException(DriverException):
    """Generic queue driver failure (Redis down, etc.)."""

    pass
