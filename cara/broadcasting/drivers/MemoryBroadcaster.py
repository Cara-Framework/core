"""
Memory Broadcasting Driver.

In-memory broadcaster for single-process WebSocket broadcasting.
Implements Laravel-style Broadcaster interface.
"""

from typing import Any, Dict, List, Union

from cara.broadcasting.ConnectionManager import ConnectionManager
from cara.broadcasting.contracts.Broadcaster import Broadcaster


class MemoryBroadcaster(ConnectionManager, Broadcaster):
    """Memory broadcasting driver - single process only."""

    driver_name = "memory"

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)

    # Broadcaster interface implementation
    async def broadcast(
        self, channels: Union[str, List[str]], event: str, data: Dict[str, Any]
    ):
        """Broadcast an event to one or more channels."""
        if isinstance(channels, str):
            channels = [channels]

        for channel in channels:
            await super().broadcast_to_channel(channel, event, data)

    # All other methods inherited from ConnectionManager
    # No additional implementation needed for memory driver
