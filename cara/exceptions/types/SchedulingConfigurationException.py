"""SchedulingConfigurationException."""

from __future__ import annotations

from .CaraException import CaraException


class SchedulingConfigurationException(CaraException):
    """Raised when the 'scheduling' configuration is missing or invalid."""

    pass
