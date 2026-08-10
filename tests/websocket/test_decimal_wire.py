"""A websocket frame carries the same money the HTTP body carries.

``Socket.send_json`` used to own a private ``_json_default`` that mapped
``Decimal`` to ``float``. Two doors out of the same process therefore
disagreed about the same value: a price fetched over HTTP arrived exact
and the live update for that price arrived rounded, so a dashboard that
opened a socket drifted away from the page it was rendered on.

The frame is asserted after the ASGI ``send`` callable, not before —
the defect was in the encoder argument, so only the bytes prove it.
"""

from __future__ import annotations

import json
from datetime import UTC
from decimal import Decimal

import pytest

from cara.exceptions.types.websocket import WebSocketException
from cara.websocket.Socket import Socket

# NUMERIC(17,6) ceiling — ``float(MONEY)`` is ``100000000000.0``.
MONEY = Decimal("99999999999.999999")


def _connected_socket(sent: list[dict]) -> Socket:
    async def _send(message: dict) -> None:
        sent.append(message)

    async def _receive() -> dict:
        return {"type": "websocket.receive"}

    socket = Socket(
        application=None, scope={"type": "websocket"}, receive=_receive, send=_send
    )
    socket._ws_connected = True  # skip the handshake; this is about the payload
    return socket


@pytest.mark.asyncio
async def test_websocket_frame_carries_exact_decimal_digits() -> None:
    sent: list[dict] = []
    socket = _connected_socket(sent)

    await socket.send_json({"total": MONEY, "unit": Decimal("19.90")})

    payload = json.loads(sent[-1]["text"])
    assert payload["total"] == "99999999999.999999"
    assert Decimal(payload["total"]) == MONEY
    assert payload["total"] != float(MONEY)
    # Scale is part of a price: "19.90" must not shrink to "19.9".
    assert payload["unit"] == "19.90"


@pytest.mark.asyncio
async def test_websocket_datetime_stays_iso8601() -> None:
    """The private encoder is gone; its ``.isoformat()`` rule is not.

    ``str(datetime)`` is space-separated and ``new Date(...)`` rejects it
    outside V8, so the shared rule keeps the websocket's behaviour rather
    than the HTTP sites' accidental ``default=str``.
    """
    from datetime import datetime

    sent: list[dict] = []
    socket = _connected_socket(sent)

    await socket.send_json({"at": datetime(2026, 8, 9, 12, 0, tzinfo=UTC)})

    assert json.loads(sent[-1]["text"])["at"] == "2026-08-09T12:00:00+00:00"


@pytest.mark.asyncio
async def test_unserializable_payload_closes_with_a_protocol_error() -> None:
    """A frame cara cannot encode must not become a stringified repr."""

    class _Order:
        pass

    sent: list[dict] = []
    socket = _connected_socket(sent)

    with pytest.raises(WebSocketException) as raised:
        await socket.send_json({"order": _Order()})

    assert raised.value.code == 4009
    assert not sent, "a refused payload must not reach the ASGI channel"
