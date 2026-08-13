"""MissingContainerBindingException."""

from __future__ import annotations

from .ContainerException import ContainerException


class MissingContainerBindingException(ContainerException):
    """Thrown when an optional binding was expected but not found."""

    pass
