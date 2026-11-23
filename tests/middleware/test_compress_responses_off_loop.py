"""Large responses are gzipped on a worker thread, small ones inline.

``CompressResponses.handle`` runs on the event loop and called
``gzip.compress`` right there. At level 6 that is 0.4 ms of CPU for a 63 KB
JSON list and 5.7 ms for a 630 KB one — a stall every other request on the
worker sits through, on every large response a browser asks for (browsers
always send ``Accept-Encoding: gzip``). zlib releases the GIL while deflating,
so a worker thread really frees the loop; under ``_OFF_LOOP_MIN_SIZE`` the
thread hop would cost more than the compression, so those stay inline.
"""

from __future__ import annotations

import gzip
import importlib
import threading
from unittest.mock import MagicMock

import pytest

from cara.http import Response

module = importlib.import_module("cara.middleware.http.CompressResponses")


def _middleware():
    middleware = module.CompressResponses.__new__(module.CompressResponses)
    middleware.application = MagicMock()
    middleware._enabled = True
    middleware._min_size = 1024
    middleware._level = 6
    middleware._prefixes = ("application/json",)
    return middleware


def _request():
    request = MagicMock()
    request.header = lambda name: "gzip, br" if name == "Accept-Encoding" else None
    return request


async def _compress(body: bytes, compress_threads: list[int]):
    response = Response(MagicMock())
    response.set_content(body)
    response.header("Content-Type", "application/json; charset=utf-8")
    real_compress = gzip.compress

    def recording_compress(data, *args, **kwargs):
        compress_threads.append(threading.get_ident())
        return real_compress(data, *args, **kwargs)

    module.gzip.compress = recording_compress
    try:

        async def next_fn(_request):
            return response

        return await _middleware().handle(_request(), next_fn)
    finally:
        module.gzip.compress = real_compress


@pytest.mark.asyncio
async def test_large_body_is_compressed_off_the_event_loop_thread() -> None:
    body = (b'{"id":"PRD00000001","title":"a product"},' * 4000)[: 128 * 1024]
    threads: list[int] = []

    response = await _compress(body, threads)

    assert len(threads) == 1
    assert threads[0] != threading.get_ident()
    assert gzip.decompress(response.content) == body
    assert response.header("Content-Encoding") == "gzip"
    assert response.header("Content-Length") == str(len(response.content))
    assert "Accept-Encoding" in response.header("Vary")


@pytest.mark.asyncio
async def test_small_body_is_compressed_inline() -> None:
    body = b'{"id":"PRD00000001","title":"a product"},' * 100  # ~4 KB
    assert 1024 < len(body) < module._OFF_LOOP_MIN_SIZE
    threads: list[int] = []

    response = await _compress(body, threads)

    assert threads == [threading.get_ident()]
    assert gzip.decompress(response.content) == body


@pytest.mark.asyncio
async def test_failed_off_loop_compression_leaves_the_response_untouched(
    monkeypatch,
) -> None:
    body = b"x" * (module._OFF_LOOP_MIN_SIZE + 1)
    response = Response(MagicMock())
    response.set_content(body)
    response.header("Content-Type", "application/json")

    def exploding_compress(*_args, **_kwargs):
        raise OSError("zlib failed")

    monkeypatch.setattr(module.gzip, "compress", exploding_compress)

    async def next_fn(_request):
        return response

    returned = await _middleware().handle(_request(), next_fn)

    assert returned.content == body
    assert returned.header("Content-Encoding") is None
