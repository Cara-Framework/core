"""``Socket`` must serialize every write to the ASGI send channel.

ASGI ``websocket.send`` is not safe under concurrent calls, yet a connection's
own controller reply, a channel/user broadcast fan-out, the heartbeat ping, and
``close()`` are all coroutines on the same event loop that can target ONE socket
at once. Without a per-socket send lock two of them suspended at ``send``
interleave into 'Unexpected ASGI message' errors / dropped frames. This test
drives many concurrent writes through one Socket and asserts the underlying
``send`` is never re-entered concurrently.
"""

from __future__ import annotations

import asyncio

import pytest

from cara.websocket.Socket import Socket


def _make_socket(send):
    async def _receive():
        return {"type": "websocket.receive"}

    sock = Socket(application=None, scope={"type": "websocket"}, receive=_receive, send=send)
    sock._ws_connected = True  # skip the handshake for the test
    return sock


@pytest.mark.asyncio
async def test_concurrent_sends_never_overlap_on_the_asgi_channel() -> None:
    in_flight = 0
    max_in_flight = 0

    async def _send(_message) -> None:
        nonlocal in_flight, max_in_flight
        in_flight += 1
        max_in_flight = max(max_in_flight, in_flight)
        # Force a suspension point INSIDE the send — this is where an unlocked
        # implementation would let a second writer interleave.
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        in_flight -= 1

    sock = _make_socket(_send)

    # 50 concurrent writers targeting the ONE socket: broadcasts, replies, pings.
    await asyncio.gather(*(sock.send_json({"n": i}) for i in range(50)))

    assert max_in_flight == 1, (
        f"the ASGI send channel was entered {max_in_flight} times concurrently "
        f"— per-socket send serialization is broken (frames would interleave)"
    )


@pytest.mark.asyncio
async def test_close_and_sends_do_not_interleave() -> None:
    in_flight = 0
    max_in_flight = 0

    async def _send(_message) -> None:
        nonlocal in_flight, max_in_flight
        in_flight += 1
        max_in_flight = max(max_in_flight, in_flight)
        await asyncio.sleep(0)
        in_flight -= 1

    sock = _make_socket(_send)

    # A close racing a burst of broadcasts must still serialize on the channel.
    await asyncio.gather(
        sock.close(code=1000),
        *(sock.send_json({"n": i}) for i in range(20)),
    )
    assert max_in_flight == 1
    # Post-close sends are silently dropped (socket is closed), never sent.
    assert sock.closed is True


@pytest.mark.asyncio
async def test_send_after_close_is_discarded_not_raised() -> None:
    sent: list = []

    async def _send(message) -> None:
        sent.append(message)

    sock = _make_socket(_send)
    await sock.close()
    before = len(sent)
    await sock.send_json({"late": True})  # must be a silent no-op
    assert len(sent) == before, "a send after close must be discarded, not written"
