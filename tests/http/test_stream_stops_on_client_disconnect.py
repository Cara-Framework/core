"""A streamed response stops when the client disconnects.

uvicorn's ``send`` is a silent no-op once the client has gone, so a stream
that only sends never learned of it: a CSV export whose tab was closed kept
reading the database to its last row. ``StreamingResponse.stream`` now watches
``receive`` while it pumps; ``http.disconnect`` cancels the pump, closes the
source and sends no end frame. A ``receive`` that fails cannot say the client
left, so the stream then runs to its end; a normal stream is unchanged.
"""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest

from cara.http import Response

_GUARD_SECONDS = 5  # only bounds a regression that would hang; never waited on


class _Source:
    """A row source that can block like a slow query and records its cleanup."""

    def __init__(self, chunks, *, then_block: bool = False) -> None:
        self._chunks = list(chunks)
        self._then_block = then_block
        self.produced = 0
        self.closed = 0

    async def body(self):
        try:
            for chunk in self._chunks:
                self.produced += 1
                yield chunk
            if self._then_block:
                await asyncio.Event().wait()  # the next page never arrives
                yield b"never"
        finally:
            self.closed += 1


def _recording_send():
    events: list[dict] = []
    first_body = asyncio.Event()

    async def send(event: dict) -> None:
        events.append(event)
        if event["type"] == "http.response.body":
            first_body.set()

    return send, events, first_body


def _bodies(events: list[dict]) -> list[bytes]:
    return [e["body"] for e in events if e["type"] == "http.response.body"]


@pytest.mark.asyncio
async def test_a_disconnect_stops_the_stream_and_closes_the_source() -> None:
    source = _Source([b"one", b"two"], then_block=True)
    response = Response(MagicMock()).stream(source.body(), content_type="text/plain")
    send, events, first_body = _recording_send()
    messages = [{"type": "http.request", "body": b"", "more_body": False}]

    async def receive() -> dict:
        if messages:
            return messages.pop(0)  # an unread body frame is drained, not fatal
        await first_body.wait()
        return {"type": "http.disconnect"}

    async with asyncio.timeout(_GUARD_SECONDS):
        await response({}, receive, send)

    assert source.closed == 1
    assert b"never" not in _bodies(events)
    # No end frame: nobody is there to read it.
    assert all(e.get("more_body", True) for e in events[1:])
    assert response.is_sent() is True


@pytest.mark.asyncio
async def test_a_failing_receive_lets_the_stream_run_to_its_end() -> None:
    source = _Source([b"one", b"two", b"three"])
    response = Response(MagicMock()).stream(source.body(), content_type="text/plain")
    send, events, _first_body = _recording_send()

    async def receive() -> dict:
        raise RuntimeError("receive channel broken")

    async with asyncio.timeout(_GUARD_SECONDS):
        await response({}, receive, send)

    assert _bodies(events) == [b"one", b"two", b"three", b""]
    assert events[-1] == {"type": "http.response.body", "body": b"", "more_body": False}
    assert source.closed == 1


@pytest.mark.asyncio
async def test_a_stream_without_a_disconnect_is_unchanged() -> None:
    source = _Source([b"one", b"two"])
    response = Response(MagicMock()).stream(source.body(), content_type="text/plain")
    send, events, _first_body = _recording_send()
    watching = {"cancelled": False}

    async def receive() -> dict:
        try:
            await asyncio.Event().wait()  # the client stays connected
        except asyncio.CancelledError:
            watching["cancelled"] = True
            raise
        return {"type": "http.disconnect"}

    async with asyncio.timeout(_GUARD_SECONDS):
        await response({}, receive, send)

    assert _bodies(events) == [b"one", b"two", b""]
    assert events[-1]["more_body"] is False
    assert source.closed == 1
    # The watcher does not outlive the response.
    assert watching["cancelled"] is True
