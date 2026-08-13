"""Canonical definition of ``PresenceChannel``."""

from __future__ import annotations

from .PrivateChannel import PrivateChannel


class PresenceChannel(PrivateChannel):
    """Like PrivateChannel but the authorization callback is expected
    to return a *user data dict* so other subscribers can see who else
    is on the channel.

    Wire form: ``presence-{name}``.
    """

    prefix = "presence"
