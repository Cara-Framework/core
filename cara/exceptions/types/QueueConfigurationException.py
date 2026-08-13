"""QueueConfigurationException."""

from __future__ import annotations

from .CaraException import CaraException


class QueueConfigurationException(CaraException):
    """Raised when the 'queue' configuration is missing or invalid."""

    pass
