"""Canonical definition of ``PrivateChannel``."""

from __future__ import annotations

from .Channel import Channel


class PrivateChannel(Channel):
    """Authenticated channel — only callers passing the registered
    authorization callback for the matching pattern may subscribe.

    Wire form: ``private-{name}``. The ``private-`` prefix is the
    framework's signal that ``ChannelRegistry.authorize`` must run
    before the subscription is accepted.
    """

    prefix = "private"
