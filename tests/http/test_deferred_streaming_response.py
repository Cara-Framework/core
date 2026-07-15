from __future__ import annotations

import asyncio
import csv
import io
from unittest.mock import MagicMock

import pytest

from cara.http import Response
from cara.http.response import StreamingResponse


@pytest.mark.asyncio
async def test_csv_stream_is_deferred_cloneable_and_header_first() -> None:
    state = {"advanced": False}

    async def rows():
        state["advanced"] = True
        yield ["id", "title"]
        yield [1, "Comma, quoted"]

    controller_response = Response(MagicMock())
    configured = controller_response.stream_csv(rows(), "products.csv")

    assert configured is controller_response
    assert state["advanced"] is False
    assert controller_response.is_sent() is False

    request_response = Response(MagicMock())
    request_response.clone_from(controller_response)
    events: list[dict] = []

    async def send(event: dict) -> None:
        if event["type"] == "http.response.start":
            assert state["advanced"] is False
        events.append(event)

    await request_response({}, None, send)

    start = events[0]
    headers = {key.decode().lower(): value.decode() for key, value in start["headers"]}
    assert start["type"] == "http.response.start"
    assert start["status"] == 200
    assert headers["content-type"] == "text/csv; charset=utf-8"
    assert headers["content-disposition"] == 'attachment; filename="products.csv"'
    assert "content-length" not in headers
    assert request_response.is_sent() is True

    body = b"".join(event.get("body", b"") for event in events[1:]).decode()
    assert list(csv.reader(io.StringIO(body))) == [
        ["id", "title"],
        ["1", "Comma, quoted"],
    ]


@pytest.mark.asyncio
async def test_csv_encoder_flushes_header_then_coalesces_bounded_chunks() -> None:
    async def rows():
        yield ["id", "value"]
        for index in range(100):
            yield [index, "x" * 20]

    chunks = [
        chunk async for chunk in StreamingResponse.csv_chunks(rows(), chunk_size=256)
    ]

    assert chunks[0] == b"id,value\r\n"
    assert 2 < len(chunks) < 101
    assert all(len(chunk) < 300 for chunk in chunks)


@pytest.mark.asyncio
async def test_csv_encoder_cooperates_with_other_event_loop_tasks() -> None:
    state = {"scheduled": False}

    async def rows():
        yield ["id"]
        for index in range(1000):
            yield [index]

    async def scheduled_task() -> None:
        await asyncio.sleep(0)
        state["scheduled"] = True

    task = asyncio.create_task(scheduled_task())
    _ = [
        chunk
        async for chunk in StreamingResponse.csv_chunks(
            rows(),
            chunk_size=1024 * 1024,
            cooperative_rows=16,
        )
    ]
    assert state["scheduled"] is True
    await task


@pytest.mark.asyncio
async def test_stream_failure_does_not_append_exception_text_to_body() -> None:
    async def broken():
        yield b"safe"
        raise RuntimeError("database-password-must-not-leak")

    response = Response(MagicMock()).stream(broken(), content_type="text/plain")
    events: list[dict] = []

    async def send(event: dict) -> None:
        events.append(event)

    await response({}, None, send)

    body = b"".join(event.get("body", b"") for event in events)
    assert body == b"safe"
    assert response.is_sent() is True
