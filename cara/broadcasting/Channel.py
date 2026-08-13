"""
Channel value-objects for broadcasting.

Laravel's ``Illuminate\\Broadcasting\\Channel`` / ``PrivateChannel`` /
``PresenceChannel`` give events a typed way to declare which channel
they fan out on. The string form ``"private-user.123"`` carries the
auth requirement implicitly — the prefix tells the WebSocket layer
"this needs an auth callback to allow subscription".

Application broadcasts use Channel objects. Raw strings exist only at the
WebSocket ingress, where :func:`channel_from_wire` validates and converts the
wire name before it enters the broadcasting core.

Conventions
-----------
- ``Channel("updates")``        → ``"updates"`` (public, no auth required)
- ``PrivateChannel("user.123")`` → ``"private-user.123"`` (auth required)
- ``PresenceChannel("room.42")`` → ``"presence-room.42"`` (auth + identity)

The prefixes are how the framework recognises auth-gated channels at
subscribe time — see ``ChannelRegistry.authorize`` / Socket-layer
subscribe path.
"""

from __future__ import annotations

from cara.exceptions import InvalidArgumentException

_AUTH_PREFIXES = ("private-", "presence-")
_MAX_CHANNEL_NAME_LENGTH = 200


class Channel:
    """Public broadcasting channel — no authorization needed."""

    prefix: str = ""

    def __init__(self, name: str) -> None:
        if (
            not isinstance(name, str)
            or not name
            or name != name.strip()
            or any(char.isspace() or ord(char) < 32 for char in name)
            or len(name) > _MAX_CHANNEL_NAME_LENGTH
        ):
            raise InvalidArgumentException(
                "Channel name must be a non-empty, whitespace-free string no "
                f"longer than {_MAX_CHANNEL_NAME_LENGTH} characters, got {name!r}"
            )
        if name.startswith(_AUTH_PREFIXES):
            raise InvalidArgumentException(
                "Channel constructors require a bare name; use the matching "
                "PrivateChannel or PresenceChannel type instead of a wire prefix."
            )
        self.name = name

    @property
    def full_name(self) -> str:
        """Channel name as it appears on the wire."""
        return f"{self.prefix}-{self.name}" if self.prefix else self.name

    def __str__(self) -> str:
        return self.full_name

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.name!r})"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Channel):
            return self.full_name == other.full_name
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self.full_name)


def channel_name(value: Channel) -> str:
    """Return the canonical wire name of a typed channel."""
    if not isinstance(value, Channel):
        raise TypeError(f"Expected Channel, got {type(value).__name__}")
    return value.full_name


def channel_from_wire(value: object) -> Channel:
    """Validate one client-supplied wire name and recover its channel type."""
    from .PresenceChannel import (
        PresenceChannel,  # local: cycle with cara.broadcasting.PresenceChannel
    )
    from .PrivateChannel import (
        PrivateChannel,  # local: cycle with cara.broadcasting.PrivateChannel
    )

    if not isinstance(value, str):
        raise InvalidArgumentException(
            f"Wire channel name must be a string, got {type(value).__name__}"
        )
    if value.startswith("presence-"):
        return PresenceChannel(value[len("presence-") :])
    if value.startswith("private-"):
        return PrivateChannel(value[len("private-") :])
    return Channel(value)


__all__ = [
    "Channel",
    "channel_from_wire",
    "channel_name",
]
