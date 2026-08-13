"""BroadcastingConfigurationException."""

from __future__ import annotations

from .BroadcastingException import BroadcastingException


class BroadcastingConfigurationException(BroadcastingException):
    """Exception thrown when broadcasting configuration is invalid."""

    pass
