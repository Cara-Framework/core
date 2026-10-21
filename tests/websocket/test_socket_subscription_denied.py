"""A denied subscription must DENY — return ``False`` and send its frame.

The ``subscription_denied`` frame carried the raw ``Channel`` object. The
JSON encoder has no rule for it, so ``send_json`` raised
``WebSocketException(4009)`` and ``subscribe_channel`` never returned
``False``: the caller's ``close(4007, "Forbidden")`` was unreachable, the
exception landed in the controller's generic handler, and the conductor closed
the socket with a normal 1000 while the client never saw the denial frame.
"""

from __future__ import annotations

import importlib
import json

import pytest

from cara.broadcasting import PrivateChannel
from cara.websocket.Socket import Socket

# The package barrel binds the CLASS at ``cara.websocket.Socket``; the facade
# name is rebound on the module itself.
socket_module = importlib.import_module("cara.websocket.Socket")


class _DenyingBroadcast:
    def __init__(self) -> None:
        self.subscribed: list = []

    async def add_connection(self, *_args) -> None:
        return None

    async def authorize_subscription(self, _channel, _user) -> bool:
        return False

    async def subscribe(self, connection_id, channel) -> bool:
        self.subscribed.append(channel)
        return True


@pytest.mark.asyncio
async def test_denied_subscription_returns_false_and_sends_the_wire_name(
    monkeypatch,
) -> None:
    broadcast = _DenyingBroadcast()
    monkeypatch.setattr(socket_module, "Broadcast", broadcast)
    sent: list[dict] = []

    async def _send(message) -> None:
        sent.append(message)

    async def _receive():
        return {"type": "websocket.receive"}

    sock = Socket(
        application=None, scope={"type": "websocket"}, receive=_receive, send=_send
    )
    sock._ws_connected = True  # skip the handshake for the test

    channel = PrivateChannel("user.7.tenant.abc.events")
    assert await sock.subscribe_channel(channel) is False

    assert broadcast.subscribed == [], "a denied channel must never be joined"
    assert sock.subscribed_channels == set()
    assert [json.loads(frame["text"]) for frame in sent] == [
        {
            "event": "subscription_denied",
            "channel": "private-user.7.tenant.abc.events",
            "reason": "unauthorized",
        }
    ]
