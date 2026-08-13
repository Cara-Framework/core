"""StrictContainerException."""

from __future__ import annotations

from .ContainerException import ContainerException


class StrictContainerException(ContainerException):
    """Thrown when strict container rules are violated."""

    pass
