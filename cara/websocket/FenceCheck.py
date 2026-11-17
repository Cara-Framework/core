"""One question ``AuthorizationFence`` re-asks of an open socket."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

__all__ = ["FenceCheck"]


@dataclass(frozen=True)
class FenceCheck:
    """One question the fence re-asks, and how a "no" closes the socket.

    Attributes:
        name: Short label for the log line (``"session"``, ``"membership"``).
        ask: Zero-argument coroutine factory; truthy means still allowed.
        close_code: The WebSocket close code a "no" sends (4006, 4007, ...).
        reason: The close reason a "no" sends.
    """

    name: str
    ask: Callable[[], Awaitable[Any]]
    close_code: int
    reason: str
