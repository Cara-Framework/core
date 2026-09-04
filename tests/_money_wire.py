"""The money probe every wire test measures against, owned once.

Leading underscore: a test-support module, not a test file itself (mirrors
``tests/architecture/_fixtures.py`` and ``tests/docs/_fixtures.py``).

``MONEY`` is cara's ``NUMERIC(17,6)`` ceiling — the smallest realistic money
value whose double is visibly wrong: ``float(MONEY)`` is ``100000000000.0``, a
penny short of a hundred billion rounded up to exactly a hundred billion, with
no exception and no log line. Every boundary that hands JSON to a client is
measured against it, so the ceiling has ONE source rather than a copy per door
(DOCTRINE §5) — a copy that drifts down to a value whose double happens to be
exact turns its test green forever.

``drain`` and ``connected_socket`` are the two send-path harnesses those tests
share: the defect they guard lived in the encoder ARGUMENT, so the assertion
has to be made on the bytes that left through the real ASGI callable, never on
a hand-assembled payload.
"""

from __future__ import annotations

from decimal import Decimal

from cara.http import Response
from cara.websocket.Socket import Socket

MONEY = Decimal("99999999999.999999")
EXACT = "99999999999.999999"


async def drain(response: Response) -> str:
    """Run a configured response through ASGI and return the body text."""
    events: list[dict] = []

    async def send(event: dict) -> None:
        events.append(event)

    await response({}, None, send)
    return b"".join(event.get("body", b"") for event in events[1:]).decode("utf-8")


def connected_socket(sent: list[dict]) -> Socket:
    """A ``Socket`` past the handshake whose frames land in ``sent``."""

    async def _send(message: dict) -> None:
        sent.append(message)

    async def _receive() -> dict:
        return {"type": "websocket.receive"}

    socket = Socket(
        application=None, scope={"type": "websocket"}, receive=_receive, send=_send
    )
    socket._ws_connected = True  # skip the handshake; this is about the payload
    return socket
