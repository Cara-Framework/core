"""
BroadcastEvent Class.

Base broadcast event class.
"""

from typing import Any, Dict, List, Optional, Union

from cara.broadcasting.contracts import ShouldBroadcast


class BroadcastEvent(ShouldBroadcast):
    """Base broadcast event class."""

    def __init__(
        self,
        channels: Union[str, List[str]],
        event_name: Optional[str] = None,
        data: Optional[Dict[str, Any]] = None,
    ):
        self.channels = self._normalize_channels(channels)
        self.event_name = event_name or self.__class__.__name__
        self.data = data or {}

    def _normalize_channels(self, channels: Union[str, List[str]]) -> List[str]:
        """Convert various channel formats to list of strings."""
        if isinstance(channels, str):
            return [channels]
        elif isinstance(channels, list):
            return channels
        else:
            raise ValueError(f"Invalid channels type: {type(channels)}")

    def broadcast_on(self) -> List[str]:
        return self.channels

    def broadcast_as(self) -> str:
        return self.event_name

    def broadcast_with(self) -> Dict[str, Any]:
        return self.data
