"""Typed refusal from a thread lane whose every thread is busy."""

from __future__ import annotations

from cara.exceptions import CaraException


class ThreadLaneBusy(CaraException):
    """Every thread of a :class:`~cara.context.ThreadLane.ThreadLane` is taken.

    Raised before any work starts, so nothing needs undoing: the caller
    maps it to its own "busy, retry shortly" answer.
    """

    def __init__(self, lane: str, size: int) -> None:
        super().__init__(f"thread lane {lane!r} is at capacity ({size} running)")
        self.lane = lane
        self.size = size


__all__ = ["ThreadLaneBusy"]
