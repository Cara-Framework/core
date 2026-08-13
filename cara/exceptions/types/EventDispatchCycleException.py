"""EventDispatchCycleException."""

from __future__ import annotations

from .CaraException import CaraException


class EventDispatchCycleException(CaraException):
    """Thrown when a listener re-dispatches an event already in flight.

    Without this guard, a listener that fires the same event it is
    handling — directly or transitively — recurses until the Python
    stack overflows. The dispatcher tracks the chain of in-flight
    event names per asyncio task and raises before recursing back into
    one that is already on the stack.
    """

    pass
