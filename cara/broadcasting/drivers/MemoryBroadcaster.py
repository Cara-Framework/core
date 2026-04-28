"""
Memory broadcasting driver — single-process broadcaster.

Backed entirely by the in-process state in ``ConnectionManager``.
Suitable for tests and dev mode; in production with multiple workers
you want the Redis driver so events fan out across processes.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from cara.broadcasting.ConnectionManager import ConnectionManager
from cara.broadcasting.contracts.Broadcaster import Broadcaster


class MemoryBroadcaster(ConnectionManager, Broadcaster):
    """In-memory broadcaster — every method is a thin wrapper over
    ``ConnectionManager`` since there's no transport layer to add."""

    driver_name = "memory"

    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__(config)

    async def broadcast(
        self,
        channels: Union[str, List[str]],
        event: str,
        data: Dict[str, Any],
        *,
        except_socket_id: Optional[str] = None,
    ) -> None:
        if isinstance(channels, str):
            channels = [channels]
        for channel in channels:
            await self.broadcast_to_channel(
                channel, event, data, except_socket_id=except_socket_id
            )

    async def broadcast_to_user(
        self,
        user_id: str,
        event: str,
        data: Dict[str, Any],
        *,
        except_socket_id: Optional[str] = None,
    ) -> None:
        await self.broadcast_to_user_local(
            user_id, event, data, except_socket_id=except_socket_id
        )
