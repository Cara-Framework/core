"""Container / dependency injection-related exceptions."""

from .base import CaraException


class ContainerException(CaraException):
    """Base for container errors."""

    pass


class MissingContainerBindingException(ContainerException):
    """Thrown when an optional binding was expected but not found."""

    pass


class GenericContainerException(ContainerException):
    """Generic container exception."""

    pass


class StrictContainerException(ContainerException):
    """Thrown when strict container rules are violated."""

    pass
