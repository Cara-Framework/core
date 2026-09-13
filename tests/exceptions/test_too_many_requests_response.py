"""A 429 leaves by the one error path, carrying its wait and its policy."""

from __future__ import annotations

import asyncio

from cara.exceptions import DefaultExceptionHandler, TooManyRequestsException
from cara.exceptions.handlers._ExceptionResponseHeaders import _EXCEPTION_RESPONSE_HEADERS


class _Send:
    def __init__(self) -> None:
        self.messages: list[dict] = []

    async def __call__(self, message: dict) -> None:
        self.messages.append(message)

    def start(self) -> dict:
        return next(m for m in self.messages if m["type"] == "http.response.start")


def test_a_refusal_carries_retry_after_and_the_ratelimit_pair_on_the_wire() -> None:
    handler = DefaultExceptionHandler(application=None)
    handler.log_exception = lambda _exception: None
    # The CORS / security / request-id builders read application config this
    # suite does not boot; they are covered where that config exists. What is
    # pinned here is that the exception's own headers reach the wire.
    handler._cors_headers_for_scope = lambda _scope: []
    handler._security_headers_for_scope = lambda _scope: []
    handler._request_id_header_for = lambda _request, _scope: []
    refusal = TooManyRequestsException(
        retry_after=3,
        response_headers={
            "RateLimit-Policy": '"api";q=120;w=60',
            "RateLimit": '"api";r=0;t=40',
        },
    )
    send = _Send()

    asyncio.run(handler.handle(refusal, None, {"type": "http", "headers": []}, None, send))

    start = send.start()
    headers = {name.decode(): value.decode() for name, value in start["headers"]}
    assert start["status"] == 429
    assert headers["retry-after"] == "3"
    assert headers["ratelimit"] == '"api";r=0;t=40'
    assert headers["ratelimit-policy"] == '"api";q=120;w=60'


def test_declared_headers_cannot_split_the_response() -> None:
    refusal = TooManyRequestsException(
        retry_after=1,
        response_headers={
            "RateLimit": '"api";r=0;t=1\r\nSet-Cookie: session=stolen',
            "Not A Header": "value",
            "RateLimit-Policy": '"api";q=1;w=1',
        },
    )

    assert _EXCEPTION_RESPONSE_HEADERS.declared(refusal) == [
        [b"ratelimit-policy", b'"api";q=1;w=1']
    ]


def test_an_exception_that_declares_nothing_adds_nothing() -> None:
    assert _EXCEPTION_RESPONSE_HEADERS.declared(ValueError("plain")) == []
