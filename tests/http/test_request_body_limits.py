from __future__ import annotations

from collections.abc import Sequence
from importlib import import_module

import pytest

from cara.configuration import Configuration
from cara.exceptions import PayloadTooLargeException
from cara.http import Request
from cara.http.BodyLimits import BodyLimits

body_parsing_module = import_module("cara.http.request.mixins.MakesBodyParsing")

_WEBHOOK_CAP = 256 * 1024


@pytest.fixture(autouse=True)
def _body_limit_config(monkeypatch) -> None:
    monkeypatch.setattr(Configuration, "_instance", None)
    configuration = Configuration.empty()
    configuration.set("server.max_body_size", BodyLimits.DEFAULT_BODY_BYTES)
    configuration.set("server.max_file_size", BodyLimits.DEFAULT_FILE_BYTES)
    configuration.set("server.max_files", BodyLimits.DEFAULT_FILES)


def _request_for_chunks(
    chunks: Sequence[bytes], request_type: type[Request] = Request
) -> tuple[Request, list[int]]:
    messages = [
        {
            "type": "http.request",
            "body": chunk,
            "more_body": index < len(chunks) - 1,
        }
        for index, chunk in enumerate(chunks)
    ]
    calls: list[int] = []

    async def receive() -> dict:
        calls.append(1)
        if not messages:
            raise AssertionError("request body was read after the stream ended")
        return messages.pop(0)

    return request_type(None).load({"type": "http"}, receive), calls


def test_payload_too_large_exception_has_canonical_http_contract() -> None:
    error = PayloadTooLargeException("too large", max_bytes=10, content_length=11)

    assert error.status_code == 413
    assert error.error_type == "payload_too_large"
    assert error.to_dict() == {
        "error": "too large",
        "type": "payload_too_large",
        "max_bytes": 10,
        "content_length": 11,
    }


@pytest.mark.asyncio
async def test_per_call_cap_is_enforced_against_cached_body() -> None:
    request, calls = _request_for_chunks([b"123456789"])

    assert await request.body(max_bytes=9) == b"123456789"
    with pytest.raises(PayloadTooLargeException) as caught:
        await request._read_body(max_bytes=8)

    assert calls == [1]
    assert caught.value.max_bytes == 8
    assert caught.value.content_length == 9


@pytest.mark.asyncio
async def test_chunked_body_is_drained_without_caching_bytes_beyond_cap() -> None:
    chunks = [b"a" * (128 * 1024), b"b" * (128 * 1024), b"c", b"d" * 32]
    request, calls = _request_for_chunks(chunks)

    with pytest.raises(PayloadTooLargeException) as caught:
        await request.body(max_bytes=_WEBHOOK_CAP)

    assert calls == [1, 1, 1, 1]
    assert request._body is None
    assert caught.value.max_bytes == _WEBHOOK_CAP
    assert caught.value.content_length == sum(map(len, chunks))


@pytest.mark.asyncio
async def test_global_body_cap_uses_payload_too_large_exception(monkeypatch) -> None:
    monkeypatch.setattr(
        body_parsing_module,
        "_body_limits",
        lambda: {"MAX_BODY_SIZE": 4, "MAX_FILE_SIZE": 4, "MAX_FILES": 20},
    )
    request, calls = _request_for_chunks([b"12345"])

    with pytest.raises(PayloadTooLargeException) as caught:
        await request.body(max_bytes=100)

    assert calls == [1]
    assert caught.value.status_code == 413
    assert caught.value.max_bytes == 4
    assert caught.value.content_length == 5


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("max_bytes", "exception_type"),
    [
        (True, TypeError),
        ("10", TypeError),
        (1.5, TypeError),
        (-1, ValueError),
    ],
)
async def test_per_call_cap_rejects_invalid_values(
    max_bytes: object, exception_type: type[Exception]
) -> None:
    request, calls = _request_for_chunks([b"body"])

    with pytest.raises(exception_type):
        await request.body(max_bytes=max_bytes)  # type: ignore[arg-type]

    assert calls == []
