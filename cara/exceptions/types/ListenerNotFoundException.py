"""ListenerNotFoundException."""

from __future__ import annotations

from .CaraException import CaraException


class ListenerNotFoundException(CaraException):
    """Thrown if you attempt to dispatch an event with no registered listeners."""

    pass
