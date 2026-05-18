"""
Null broadcasting driver — every operation is a no-op.

Used as the default in CLI / migration / queue worker contexts where
broadcasting is meaningless. Implements the full Broadcaster contract
so apps can swap it in without changing call sites.
"""

from __future__ import annotations

from typing import Any

from cara.broadcasting.contracts.Broadcaster import Broadcaster


class NullBroadcaster(Broadcaster):
    """No-op broadcaster."""

    driver_name = "null"

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config

    async def broadcast(
        self,
        channels: str | list[str],
        event: str,
        data: dict[str, Any],
        *,
        except_socket_id: str | None = None,
    ) -> None:
        return

    async def add_connection(
        self,
        connection_id: str,
        websocket: Any,
        user_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        return

    async def remove_connection(self, connection_id: str) -> None:
        return

    async def subscribe(self, connection_id: str, channel: str) -> bool:
        return True

    async def unsubscribe(self, connection_id: str, channel: str) -> bool:
        return True

    async def broadcast_to_user(
        self,
        user_id: str,
        event: str,
        data: dict[str, Any],
        *,
        except_socket_id: str | None = None,
    ) -> None:
        return

    def get_connection_count(self) -> int:
        return 0

    def get_channel_subscribers(self, channel: str) -> list[str]:
        return []

    def get_stats(self) -> dict[str, Any]:
        return {
            "driver": "null",
            "connections": 0,
            "channels": 0,
            "description": "Null driver — all operations are no-ops",
        }
