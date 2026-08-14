"""One ``http.response.start`` per connection — the ASGI contract.

Before this suite existed, a body-send failure AFTER the start was on
the wire walked four layers of error handling and every one of them
tried a fresh ``http.response.start`` — the exact message uvicorn
rejects with "Expected 'http.response.body', but got
'http.response.start'". These tests pin the repaired contract: once a
start is out, every error path may only CLOSE the body, and the
framework exception handler honors the same rule through the
``response_started`` scope flag.
"""

from __future__ import annotations

import asyncio

import pytest

from cara.exceptions.handlers.DefaultExceptionHandler import DefaultExceptionHandler
from cara.http.response.BaseResponse import BaseResponse
from cara.http.response.Response import Response


class _Send:
    """Records messages; optionally raises on the Nth call."""

    def __init__(self, fail_on: int | None = None, error: Exception | None = None):
        self.messages: list[dict] = []
        self.fail_on = fail_on
        self.error = error or RuntimeError("client vanished")

    async def __call__(self, message: dict) -> None:
        self.messages.append(message)
        if self.fail_on is not None and len(self.messages) == self.fail_on:
            raise self.error

    def starts(self) -> list[dict]:
        return [m for m in self.messages if m["type"] == "http.response.start"]

    def bodies(self) -> list[dict]:
        return [m for m in self.messages if m["type"] == "http.response.body"]


def _response(content: bytes = b'{"ok":true}') -> BaseResponse:
    response = BaseResponse(application=None)
    response.set_content(content)
    return response


def test_body_send_failure_never_starts_a_second_response():
    response = _response()
    send = _Send(fail_on=2)  # the first http.response.body attempt dies

    asyncio.run(response({"type": "http"}, None, send))

    assert len(send.starts()) == 1, "a failed body must not restart the response"
    # The error path closed the connection with a terminal empty body.
    assert send.messages[-1] == {
        "type": "http.response.body",
        "body": b"",
        "more_body": False,
    }
    assert response.is_sent()


def test_pre_start_failure_still_renders_the_500():
    response = _response()
    response.prepare_content = lambda: (_ for _ in ()).throw(ValueError("boom"))
    send = _Send()

    asyncio.run(response({"type": "http"}, None, send))

    # Nothing was on the wire yet, so the error path owns the ONE start.
    assert len(send.starts()) == 1
    assert send.starts()[0]["status"] == 500
    assert send.bodies()[-1]["more_body"] is False
    assert response.is_sent()


def test_error_paths_swallow_a_dead_connection():
    response = _response()
    send = _Send(fail_on=2)

    async def run() -> None:
        await response({"type": "http"}, None, send)

    # Even when EVERY send fails — including the start itself — the
    # response must not raise back into the conductor: the connection is
    # gone; there is nobody left to answer.
    dead = _Send(fail_on=1)
    response_dead = _response()
    asyncio.run(response_dead({"type": "http"}, None, dead))

    asyncio.run(run())
    assert response.is_sent()


@pytest.mark.parametrize("door", ["send_response", "send_manual_response"])
def test_exception_handler_never_restarts_a_started_response(door: str):
    handler = DefaultExceptionHandler(application=None)
    scope = {"type": "http", "response_started": True}
    send = _Send()

    async def run() -> None:
        if door == "send_response":
            await handler.send_response({"error": "x"}, 500, scope, None, send)
        else:
            await handler.send_manual_response({"error": "x"}, 500, scope, None, send)

    asyncio.run(run())

    assert send.starts() == [], "a started connection only ever gets a body close"
    assert send.messages == [
        {"type": "http.response.body", "body": b"", "more_body": False}
    ]
    assert scope["response_sent"] is True


def test_exception_handler_sends_normally_on_a_fresh_connection():
    handler = DefaultExceptionHandler(application=None)
    scope = {"type": "http"}
    send = _Send()

    asyncio.run(handler.send_manual_response({"error": "x"}, 503, scope, None, send))

    assert len(send.starts()) == 1
    assert send.starts()[0]["status"] == 503
    assert send.bodies()[-1].get("more_body", False) is not True


@pytest.mark.parametrize("status", [101, 204, 205, 304])
def test_bodyless_statuses_always_complete_without_entity_headers(status: int):
    response = Response(application=None).json(
        {"must": "not reach the wire"},
        status=status,
        headers={"Transfer-Encoding": "chunked", "X-Test": "preserved"},
    )
    send = _Send()

    asyncio.run(response({"type": "http"}, None, send))

    assert send.starts()[0]["status"] == status
    headers = dict(send.starts()[0]["headers"])
    assert b"content-type" not in headers
    assert b"content-length" not in headers
    assert b"transfer-encoding" not in headers
    assert headers[b"x-test"] == b"preserved"
    assert send.bodies() == [
        {"type": "http.response.body", "body": b"", "more_body": False}
    ]
