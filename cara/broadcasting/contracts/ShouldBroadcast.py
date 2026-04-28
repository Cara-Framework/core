"""
ShouldBroadcast contract.

The interface a domain event implements to opt into broadcasting.
Mirrors Laravel's ``Illuminate\\Contracts\\Broadcasting\\ShouldBroadcast``
plus the optional ``broadcastUnless`` / ``except_socket_id`` /
``broadcastVia`` extensions used in real-world Laravel codebases.

Channel return values may be:
- a single string (``"deals"``)
- a single Channel instance (``PrivateChannel("user.42")``)
- a list / tuple containing any mix of the above

The Broadcasting manager flattens to canonical wire-form strings
before dispatch. Callers don't need to think about Channel vs string
when implementing their event.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Sequence, Union

# Forward-imported lazily to avoid a circular import — the Channel
# module has no other deps but ShouldBroadcast lives in `contracts`
# which loads first.
ChannelLike = Union[str, "object"]


class ShouldBroadcast(ABC):
    """Interface for events that should be broadcast over WebSocket."""

    @abstractmethod
    def broadcast_on(self) -> Union[ChannelLike, Sequence[ChannelLike]]:
        """Channel(s) the event should broadcast on. May return a
        string, a ``Channel`` instance, or a list/tuple of either."""

    @abstractmethod
    def broadcast_as(self) -> str:
        """Wire-side event name (e.g. ``"price.updated"``). Defaults
        to the class name when ``BroadcastEvent`` is the base."""

    def broadcast_with(self) -> Dict[str, Any]:
        """Payload dict broadcast to subscribers. Defaults to empty."""
        return {}

    def broadcast_when(self) -> bool:
        """Gate broadcasting on a runtime condition. Return ``False``
        to skip the broadcast entirely (event is logged at debug)."""
        return True

    def broadcast_unless(self) -> bool:
        """Mirror of ``broadcast_when``. Return ``True`` to *skip*
        broadcasting. Provided for readability — having both lets the
        gate read naturally regardless of which way the condition is
        phrased."""
        return False

    def broadcast_via(self) -> Optional[str]:
        """Driver name override. Return ``None`` to use the default
        broadcaster, or a registered driver name (``"redis"``,
        ``"log"``, ...) to route this event to a specific transport."""
        return None

    def except_socket_id(self) -> Optional[str]:
        """Connection ``socket_id`` to skip when delivering this
        event — the "don't echo back to sender" pattern. Set this to
        the value of the request's ``X-Socket-Id`` header so the
        connection that triggered the event doesn't receive its own
        broadcast back."""
        return None
