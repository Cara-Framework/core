"""``Socket.serve_frames``: every way the one reader of a connection ends."""

from __future__ import annotations

import asyncio
import json

import pytest

from cara.exceptions import WebSocketException
from cara.websocket import MessageBudget, Socket


class _Client:
    """An ASGI peer: frames go in, what the server sent comes out."""

    def __init__(self) -> None:
        self.inbox: asyncio.Queue = asyncio.Queue()
        self.sent: list[dict] = []

    async def receive(self) -> dict:
        return await self.inbox.get()

    async def send(self, message: dict) -> None:
        self.sent.append(message)

    def frame(self, text: str) -> None:
        self.inbox.put_nowait({"type": "websocket.receive", "text": text})

    @property
    def closed_with(self) -> int | None:
        codes = [m["code"] for m in self.sent if m["type"] == "websocket.close"]
        return codes[-1] if codes else None


def _socket(client: _Client) -> Socket:
    socket = Socket(None, {"path": "/ws", "headers": []}, client.receive, client.send)
    socket._ws_connected = True
    socket._broadcast_touch_cache = "_no_touch"
    return socket


def _budget(limit: int = 10) -> MessageBudget:
    return MessageBudget(limit, 60.0, clock=lambda: 0.0)


async def _forever() -> None:
    await asyncio.Event().wait()


def _serve(client: _Client, budget: MessageBudget, *, fence=None):
    answered: list = []

    async def answer(message) -> None:
        answered.append(message)

    async def run() -> None:
        guard = asyncio.create_task(fence() if fence else _forever())
        try:
            await _socket(client).serve_frames(guard, answer, budget)
        finally:
            guard.cancel()
            await asyncio.gather(guard, return_exceptions=True)

    asyncio.run(asyncio.wait_for(run(), timeout=5))
    return answered


def test_a_client_over_its_budget_is_closed_and_its_frame_is_not_answered():
    client = _Client()
    client.frame(json.dumps({"n": 1}))
    client.frame(json.dumps({"n": 2}))
    client.frame(json.dumps({"n": 3}))

    answered = _serve(client, _budget(limit=2))

    assert answered == [{"n": 1}, {"n": 2}]
    assert client.closed_with == 4008


def test_a_frame_that_is_not_json_ends_the_connection_with_its_own_code():
    client = _Client()
    client.frame("{not json")

    with pytest.raises(WebSocketException) as refused:
        _serve(client, _budget())

    assert refused.value.code == 4009


def test_the_client_leaving_ends_the_reader_without_an_error():
    client = _Client()
    client.frame(json.dumps({"n": 1}))
    client.inbox.put_nowait({"type": "websocket.disconnect", "code": 1001})

    assert _serve(client, _budget()) == [{"n": 1}]
    assert client.closed_with is None


def test_the_fence_ending_stops_a_reader_parked_on_a_silent_client():
    # The session was revoked: nothing arrives from the client, yet the read
    # must not outlive the authorization that admitted it.
    async def revoked() -> None:
        await asyncio.sleep(0)

    assert _serve(_Client(), _budget(), fence=revoked) == []


def test_the_answer_failing_reaches_the_caller():
    client = _Client()
    client.frame(json.dumps({"n": 1}))

    async def boom(_message) -> None:
        raise ValueError("answer failed")

    async def run() -> None:
        guard = asyncio.create_task(_forever())
        try:
            await _socket(client).serve_frames(guard, boom, _budget())
        finally:
            guard.cancel()
            await asyncio.gather(guard, return_exceptions=True)

    with pytest.raises(ValueError, match="answer failed"):
        asyncio.run(asyncio.wait_for(run(), timeout=5))
