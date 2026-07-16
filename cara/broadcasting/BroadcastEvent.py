"""
BroadcastEvent — convenience base class for broadcasting events.

Subclass this when you want to broadcast a payload without writing
a full ``ShouldBroadcast`` boilerplate. Most broadcastable events
extend this directly.

Channel arguments accept strings *and* ``Channel`` instances (and
lists of either). Wire-form normalization happens at dispatch time
so subclasses can declare channels however reads naturally.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from cara.broadcasting.Channel import Channel, channel_name
from cara.broadcasting.contracts import ShouldBroadcast

ChannelLike = str | Channel


class BroadcastEvent(ShouldBroadcast):
    """Reasonable defaults for the ``ShouldBroadcast`` contract.

    Subclasses typically override ``broadcast_with`` to shape the
    payload, optionally ``broadcast_when`` to gate firing, and pass
    channels + event name through ``__init__``::

        class ThingUpdated(BroadcastEvent):
            def __init__(self, record_id: int, value: float):
                super().__init__(
                    channels=[f"record.{record_id}"],
                    event_name="record.updated",
                )
                self._payload = {"record_id": record_id, "value": value}

            def broadcast_with(self) -> dict:
                return self._payload
    """

    def __init__(
        self,
        channels: ChannelLike | Sequence[ChannelLike],
        event_name: str | None = None,
        data: dict[str, Any] | None = None,
    ) -> None:
        self.channels: list[str] = self._normalize_channels(channels)
        self.event_name: str = event_name or self.__class__.__name__
        self.data: dict[str, Any] = data or {}
        # Consumers set this to the value of the inbound HTTP request's
        # X-Socket-Id header to avoid echoing the event back to the
        # connection that triggered it.
        self._except_socket_id: str | None = None
        self._broadcast_via: str | None = None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _normalize_channels(
        channels: ChannelLike | Sequence[ChannelLike],
    ) -> list[str]:
        """Flatten a string / Channel / list-of-either into a list of
        canonical wire-form strings."""
        if isinstance(channels, (str, Channel)):
            return [channel_name(channels)]
        if isinstance(channels, (list, tuple)):
            return [channel_name(c) for c in channels]
        raise TypeError(
            f"channels must be str, Channel, or sequence of either; got {type(channels).__name__}"
        )

    # ------------------------------------------------------------------
    # Fluent setters — chainable for readability at dispatch site.
    # ------------------------------------------------------------------
    def to_others(self, socket_id: str | None) -> BroadcastEvent:
        """Skip the connection identified by ``socket_id`` when
        fanning this event out. Returns self so chains read naturally::

            event = OrderCreated(...).to_others(request.header("X-Socket-Id"))
            await broadcast_event_async(event)
        """
        self._except_socket_id = socket_id
        return self

    def via(self, driver: str | None) -> BroadcastEvent:
        """Pin this event to a specific broadcasting driver."""
        self._broadcast_via = driver
        return self

    # ------------------------------------------------------------------
    # ShouldBroadcast contract
    # ------------------------------------------------------------------
    def broadcast_on(self) -> list[str]:
        return self.channels

    def broadcast_as(self) -> str:
        return self.event_name

    def broadcast_with(self) -> dict[str, Any]:
        return self.data

    def except_socket_id(self) -> str | None:
        return self._except_socket_id

    def broadcast_via(self) -> str | None:
        return self._broadcast_via
