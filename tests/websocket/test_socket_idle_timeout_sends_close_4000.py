"""An idle timeout closes the socket WITH code 4000 before raising.

Live defect in ``cara/websocket/Socket.py``: on ``receive_timeout`` the
socket marked itself closed and raised. The caller's later ``close()`` was
then a no-op, so no close frame was ever sent and the client saw an
abnormal 1006 instead of the documented 4000. The close frame is now sent
first, bounded by ``send_timeout``; a send that fails is logged and the
timeout is still what is raised.
"""

from __future__ import annotations

import asyncio
import importlib

import pytest

from cara.exceptions import WebSocketException
from cara.websocket.Socket import Socket

socket_module = importlib.import_module("cara.websocket.Socket")


async def _never_receives():
    await asyncio.Event().wait()


def _socket(send) -> Socket:
    sock = Socket(
        application=None,
        scope={"type": "websocket"},
        receive=_never_receives,
        send=send,
    )
    sock._ws_connected = True
    # A zero bound times the wait out at its first suspension: no wall clock.
    sock.receive_timeout = 0
    return sock


@pytest.fixture
def debug_logs(monkeypatch):
    logged: list[str] = []
    monkeypatch.setattr(
        socket_module.Log,
        "debug",
        lambda message, *args, **_k: logged.append(message % args),
        raising=False,
    )
    return logged


@pytest.mark.asyncio
async def test_idle_timeout_sends_a_4000_close_frame_then_raises(debug_logs) -> None:
    sent: list[dict] = []

    async def _send(message) -> None:
        sent.append(message)

    sock = _socket(_send)

    with pytest.raises(WebSocketException) as raised:
        await sock.receive_message()

    assert raised.value.code == 4000
    assert sent == [{"type": "websocket.close", "code": 4000, "reason": "idle timeout"}]
    assert sock.closed is True
    assert sock.close_code == 4000
    assert debug_logs == []


@pytest.mark.asyncio
async def test_a_failed_close_send_is_logged_and_the_timeout_still_raised(
    debug_logs,
) -> None:
    async def _send(_message) -> None:
        raise ConnectionResetError("peer gone")

    sock = _socket(_send)

    with pytest.raises(WebSocketException) as raised:
        await sock.receive_message()

    assert raised.value.code == 4000
    assert "idle" in str(raised.value)
    assert sock.closed is True
    assert len(debug_logs) == 1 and "peer gone" in debug_logs[0]


@pytest.mark.asyncio
async def test_a_peer_that_stopped_reading_cannot_hold_the_close(debug_logs) -> None:
    async def _send(_message) -> None:
        await asyncio.Event().wait()

    sock = _socket(_send)
    # The bound itself is what ends the stalled send; no wait on our side.
    # (A zero bound would cancel close() before it started at all.)
    sock.send_timeout = 0.01

    with pytest.raises(WebSocketException) as raised:
        await sock.receive_message()

    assert raised.value.code == 4000
    assert sock.closed is True
    assert len(debug_logs) == 1
