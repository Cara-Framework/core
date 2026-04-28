"""
Channel value-objects for broadcasting.

Laravel's ``Illuminate\\Broadcasting\\Channel`` / ``PrivateChannel`` /
``PresenceChannel`` give events a typed way to declare which channel
they fan out on. The string form ``"private-user.123"`` carries the
auth requirement implicitly — the prefix tells the WebSocket layer
"this needs an auth callback to allow subscription".

Cara's broadcasting subsystem accepts plain strings *and* Channel
objects. Strings are passed through unchanged for backwards
compatibility with plain channel names like ``"deals"`` or
``"products.live"``; Channel objects are flattened to their canonical
``str(channel)`` form at dispatch time.

Conventions
-----------
- ``Channel("deals")``           → ``"deals"`` (public, no auth required)
- ``PrivateChannel("user.123")`` → ``"private-user.123"`` (auth required)
- ``PresenceChannel("room.42")`` → ``"presence-room.42"`` (auth + identity)

The prefixes are how the framework recognises auth-gated channels at
subscribe time — see ``ChannelRegistry.authorize`` / Socket-layer
subscribe path.
"""

from __future__ import annotations

from typing import Any


class Channel:
    """Public broadcasting channel — no authorization needed."""

    prefix: str = ""

    def __init__(self, name: str) -> None:
        if not isinstance(name, str) or not name:
            raise ValueError(f"Channel name must be a non-empty string, got {name!r}")
        # Strip any prefix the caller already attached, so
        # ``PrivateChannel("private-foo")`` and
        # ``PrivateChannel("foo")`` produce the same result.
        if self.prefix and name.startswith(f"{self.prefix}-"):
            name = name[len(self.prefix) + 1 :]
        self.name = name

    @property
    def full_name(self) -> str:
        """Channel name as it appears on the wire."""
        return f"{self.prefix}-{self.name}" if self.prefix else self.name

    def __str__(self) -> str:
        return self.full_name

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.name!r})"

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Channel):
            return self.full_name == other.full_name
        if isinstance(other, str):
            return self.full_name == other
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self.full_name)


class PrivateChannel(Channel):
    """Authenticated channel — only callers passing the registered
    authorization callback for the matching pattern may subscribe.

    Wire form: ``private-{name}``. The ``private-`` prefix is the
    framework's signal that ``ChannelRegistry.authorize`` must run
    before the subscription is accepted.
    """

    prefix = "private"


class PresenceChannel(PrivateChannel):
    """Like PrivateChannel but the authorization callback is expected
    to return a *user data dict* (or ``True`` for legacy callers) so
    other subscribers can see who else is on the channel.

    Wire form: ``presence-{name}``.
    """

    prefix = "presence"


def channel_name(value: Any) -> str:
    """Coerce a Channel-or-string to its canonical string form.

    Used at every API boundary that accepts both — the contract is
    "give us anything channel-shaped and we'll normalize it".
    """
    if isinstance(value, Channel):
        return value.full_name
    if isinstance(value, str):
        return value
    raise TypeError(
        f"Expected str or Channel instance for channel name, got {type(value).__name__}"
    )


__all__ = ["Channel", "PrivateChannel", "PresenceChannel", "channel_name"]
