"""
Broadcaster contract — interface every broadcasting driver implements.

Mirrors Laravel's ``Illuminate\\Contracts\\Broadcasting\\Broadcaster``.
The Broadcasting manager only ever talks to the driver through this
contract; concrete drivers (Memory, Redis, Log, Null) implement it.

Note that ``broadcast`` and ``broadcast_to_user`` accept a keyword-only
``except_socket_id`` parameter — the connection whose public socket
id matches will be skipped on delivery. Used to avoid echoing an
event back to the connection that triggered it.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union


class Broadcaster(ABC):
    """Driver interface."""

    @abstractmethod
    async def broadcast(
        self,
        channels: Union[str, List[str]],
        event: str,
        data: Dict[str, Any],
        *,
        except_socket_id: Optional[str] = None,
    ) -> None:
        """Fan out ``event`` to ``channels``. Skip the connection
        whose public socket id matches ``except_socket_id``."""

    @abstractmethod
    async def add_connection(
        self,
        connection_id: str,
        websocket: Any,
        user_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None: ...

    @abstractmethod
    async def remove_connection(self, connection_id: str) -> None: ...

    @abstractmethod
    async def subscribe(self, connection_id: str, channel: str) -> bool: ...

    @abstractmethod
    async def unsubscribe(self, connection_id: str, channel: str) -> bool: ...

    @abstractmethod
    async def broadcast_to_user(
        self,
        user_id: str,
        event: str,
        data: Dict[str, Any],
        *,
        except_socket_id: Optional[str] = None,
    ) -> None: ...

    @abstractmethod
    def get_connection_count(self) -> int: ...

    @abstractmethod
    def get_channel_subscribers(self, channel: str) -> List[str]: ...

    @abstractmethod
    def get_stats(self) -> Dict[str, Any]: ...

    async def cleanup(self) -> None:
        """Tear-down hook — override if the driver holds external
        resources (Redis connections, background tasks, etc.). The
        application's shutdown sequence calls this on every registered
        driver."""
