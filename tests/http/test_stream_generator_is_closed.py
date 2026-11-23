"""A stream that stops early closes its data source before the request ends.

``StreamingResponse.stream`` iterated the body generator and, when the stream
stopped part-way — ``send`` raised, or the task was cancelled — simply left.
The generator stayed suspended at its ``yield`` with its ``finally`` unrun
until the garbage collector reached it, and the exception that stopped the
stream kept it reachable. For an export that ``finally`` is the row cursor
and its pooled database connection.

The ``stream_csv`` / ``stream_json_lines`` / ``stream_sse`` wrappers had the
same gap one level down: closing a wrapper generator does not close the
generator it reads.
"""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest

from cara.http import Response
from cara.http.response.StreamAborted import StreamAborted


class _Source:
    """An async row source that records when its cleanup ran."""

    def __init__(self, rows):
        self._rows = list(rows)
        self.closed = 0

    async def rows(self):
        try:
            for row in self._rows:
                yield row
        finally:
            self.closed += 1


def _failing_send(fail_on_body_frame: int):
    events: list[dict] = []
    bodies = 0

    async def send(event: dict) -> None:
        nonlocal bodies
        if event["type"] == "http.response.body":
            bodies += 1
            if bodies == fail_on_body_frame:
                raise ConnectionResetError("peer went away")
        events.append(event)

    return send, events


@pytest.mark.asyncio
async def test_generator_is_closed_when_send_fails_mid_stream() -> None:
    source = _Source([b"one", b"two", b"three"])
    response = Response(MagicMock()).stream(source.rows(), content_type="text/plain")
    send, events = _failing_send(fail_on_body_frame=2)

    with pytest.raises(StreamAborted):
        await response({}, None, send)

    # Closed by the time the response call returned — not at some later GC.
    assert source.closed == 1
    assert [event.get("body") for event in events[1:]] == [b"one"]


@pytest.mark.asyncio
async def test_csv_row_source_is_closed_when_send_fails_mid_stream() -> None:
    source = _Source([["id"], [1], [2], [3]])
    response = Response(MagicMock()).stream_csv(source.rows(), "rows.csv")
    send, _events = _failing_send(fail_on_body_frame=1)

    with pytest.raises(StreamAborted):
        await response({}, None, send)

    assert source.closed == 1


@pytest.mark.asyncio
async def test_json_lines_and_sse_sources_are_closed_when_send_fails() -> None:
    lines = _Source([{"n": 1}, {"n": 2}])
    events_source = _Source([{"data": "a"}, {"data": "b"}])

    for response in (
        Response(MagicMock()).stream_json_lines(lines.rows()),
        Response(MagicMock()).stream_sse(events_source.rows()),
    ):
        send, _events = _failing_send(fail_on_body_frame=1)
        with pytest.raises(StreamAborted):
            await response({}, None, send)

    assert (lines.closed, events_source.closed) == (1, 1)


@pytest.mark.asyncio
async def test_source_is_closed_when_the_request_task_is_cancelled() -> None:
    source = _Source([["id"], [1], [2]])
    response = Response(MagicMock()).stream_csv(source.rows(), "rows.csv")
    blocked = asyncio.Event()

    async def send(event: dict) -> None:
        if event["type"] == "http.response.body":
            blocked.set()
            await asyncio.Event().wait()  # a client that stopped reading

    task = asyncio.create_task(response({}, None, send))
    await blocked.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert source.closed == 1


@pytest.mark.asyncio
async def test_completed_stream_is_unchanged() -> None:
    source = _Source([["id"], [1]])
    response = Response(MagicMock()).stream_csv(source.rows(), "rows.csv")
    events: list[dict] = []

    async def send(event: dict) -> None:
        events.append(event)

    await response({}, None, send)

    assert source.closed == 1
    assert b"".join(event.get("body", b"") for event in events) == b"id\r\n1\r\n"
    assert events[-1] == {"type": "http.response.body", "body": b"", "more_body": False}
    assert response.is_sent() is True


@pytest.mark.asyncio
async def test_failing_cleanup_does_not_replace_the_stream_outcome() -> None:
    async def body():
        try:
            yield b"one"
            yield b"two"
        finally:
            raise RuntimeError("cursor close failed")

    response = Response(MagicMock()).stream(body(), content_type="text/plain")
    send, _events = _failing_send(fail_on_body_frame=2)

    # Still the stream's own abort, not the cleanup's RuntimeError.
    with pytest.raises(StreamAborted):
        await response({}, None, send)


@pytest.mark.asyncio
async def test_plain_async_iterable_without_aclose_still_streams() -> None:
    class _Chunks:
        def __init__(self):
            self._left = [b"a", b"b"]

        def __aiter__(self):
            return self

        async def __anext__(self):
            if not self._left:
                raise StopAsyncIteration
            return self._left.pop(0)

    response = Response(MagicMock()).stream(_Chunks(), content_type="text/plain")
    events: list[dict] = []

    async def send(event: dict) -> None:
        events.append(event)

    await response({}, None, send)

    assert b"".join(event.get("body", b"") for event in events) == b"ab"
