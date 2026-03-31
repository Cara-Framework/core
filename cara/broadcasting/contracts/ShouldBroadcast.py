"""
ShouldBroadcast Interface.

Interface for events that should be broadcast over WebSocket.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Union


class ShouldBroadcast(ABC):
    """Interface for events that should be broadcast over WebSocket."""

    @abstractmethod
    def broadcast_on(self) -> Union[str, List[str]]:
        """Get the channels the event should broadcast on."""
        pass

    @abstractmethod
    def broadcast_as(self) -> str:
        """Get the broadcast event name."""
        pass

    def broadcast_with(self) -> Dict[str, Any]:
        """Get the data to broadcast with the event."""
        return {}

    def broadcast_when(self) -> bool:
        """Determine if the event should be broadcast."""
        return True
