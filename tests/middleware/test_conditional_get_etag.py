"""Tests for the ConditionalGet (ETag / conditional-GET) middleware.

Covers the RFC 7232 contract the middleware promises:

* a safe 2xx GET gets a weak ``ETag`` emitted from the body,
* a matching ``If-None-Match`` collapses to a bodyless ``304`` that keeps
  the same ETag (and any cache headers) but drops the body,
* a non-matching ``If-None-Match`` yields the full ``200`` + body,
* non-GET / non-2xx / streaming responses are left untouched, and the
  middleware never clobbers ``Cache-Control``.

Tests run against the real ``cara.http.Response`` (constructed with a
MagicMock application, the established pattern in this suite) so the
integration with ``HeaderManager`` is exercised, not just a stub.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from cara.http import Response
from cara.middleware.http import ConditionalGet

# ── Test doubles ─────────────────────────────────────────────────────


def _make_request(method: str = "GET", if_none_match: str | None = None):
    """Request stub exposing the ``method`` property + ``header`` lookup."""
    req = MagicMock()
    req.method = method
    headers = {}
    if if_none_match is not None:
        headers["If-None-Match"] = if_none_match

    def _header(name, default=None):
        return headers.get(name, default)

    req.header = _header
    return req


def _make_response(status: int = 200, body: dict | None = None) -> Response:
    resp = Response(MagicMock())
    resp.json(body if body is not None else {"hello": "world"}, status=status)
    return resp


def _middleware() -> ConditionalGet:
    return ConditionalGet(MagicMock())


async def _run(mw: ConditionalGet, request, response: Response) -> Response:
    async def next_fn(_req):
        return response

    return await mw.handle(request, next_fn)


# ── 200 + ETag emission ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_2xx_emits_weak_etag():
    mw = _middleware()
    resp = _make_response(200, {"id": 1})
    out = await _run(mw, _make_request("GET"), resp)

    assert out.status_code == 200
    etag = out.header("ETag")
    assert etag is not None
    # Weak validator form: W/"<hex>".
    assert etag.startswith('W/"') and etag.endswith('"')
    # Body untouched on a plain 200.
    assert out.content == b'{"id": 1}'


@pytest.mark.asyncio
async def test_etag_is_stable_for_same_body():
    mw = _middleware()
    a = await _run(mw, _make_request("GET"), _make_response(200, {"k": "v"}))
    b = await _run(mw, _make_request("GET"), _make_response(200, {"k": "v"}))
    assert a.header("ETag") == b.header("ETag")


@pytest.mark.asyncio
async def test_etag_differs_for_different_body():
    mw = _middleware()
    a = await _run(mw, _make_request("GET"), _make_response(200, {"k": "v1"}))
    b = await _run(mw, _make_request("GET"), _make_response(200, {"k": "v2"}))
    assert a.header("ETag") != b.header("ETag")


# ── Matching If-None-Match → 304 ─────────────────────────────────────


@pytest.mark.asyncio
async def test_matching_if_none_match_returns_304_empty_body_same_etag():
    mw = _middleware()
    # First request to learn the ETag the server would emit.
    primed = await _run(mw, _make_request("GET"), _make_response(200, {"id": 7}))
    etag = primed.header("ETag")
    assert etag is not None

    # Second request carries that ETag — must collapse to 304.
    resp = _make_response(200, {"id": 7})
    out = await _run(mw, _make_request("GET", if_none_match=etag), resp)

    assert out.status_code == 304
    assert out.content == b""
    assert out.header("ETag") == etag


@pytest.mark.asyncio
async def test_matching_works_when_client_sends_strong_form():
    """If-None-Match uses weak comparison: a client echoing the opaque
    tag without the ``W/`` prefix must still match the weak ETag."""
    mw = _middleware()
    primed = await _run(mw, _make_request("GET"), _make_response(200, {"a": 1}))
    weak = primed.header("ETag")
    assert weak.startswith('W/"')
    strong = weak[2:]  # drop the W/ prefix -> "...."

    out = await _run(
        mw, _make_request("GET", if_none_match=strong), _make_response(200, {"a": 1})
    )
    assert out.status_code == 304
    assert out.content == b""


@pytest.mark.asyncio
async def test_star_if_none_match_matches_any_representation():
    mw = _middleware()
    out = await _run(
        mw, _make_request("GET", if_none_match="*"), _make_response(200, {"a": 1})
    )
    assert out.status_code == 304
    assert out.content == b""
    assert out.header("ETag") is not None


@pytest.mark.asyncio
async def test_matching_etag_in_comma_separated_list():
    mw = _middleware()
    primed = await _run(mw, _make_request("GET"), _make_response(200, {"a": 1}))
    etag = primed.header("ETag")
    header_val = f'W/"other-tag", {etag}, W/"another"'

    out = await _run(
        mw,
        _make_request("GET", if_none_match=header_val),
        _make_response(200, {"a": 1}),
    )
    assert out.status_code == 304


@pytest.mark.asyncio
async def test_304_drops_content_length_and_type():
    mw = _middleware()
    primed = await _run(mw, _make_request("GET"), _make_response(200, {"a": 1}))
    etag = primed.header("ETag")

    resp = _make_response(200, {"a": 1})
    # Simulate the controller having set Content-Length explicitly.
    resp.header("Content-Length", "9")
    out = await _run(mw, _make_request("GET", if_none_match=etag), resp)

    assert out.status_code == 304
    # Representation headers describing a body must be gone.
    assert out.headers.get("Content-Length") is None
    assert out.headers.get("Content-Type") is None


# ── Non-matching → full 200 ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_non_matching_if_none_match_returns_full_200():
    mw = _middleware()
    resp = _make_response(200, {"id": 99})
    out = await _run(
        mw, _make_request("GET", if_none_match='W/"deadbeef"'), resp
    )
    assert out.status_code == 200
    assert out.content == b'{"id": 99}'
    assert out.header("ETag") is not None


# ── Method / status gating ───────────────────────────────────────────


@pytest.mark.asyncio
async def test_post_is_untouched_no_etag_no_304():
    mw = _middleware()
    # Even with a matching-looking If-None-Match, POST must pass through.
    resp = _make_response(200, {"id": 1})
    out = await _run(mw, _make_request("POST", if_none_match="*"), resp)
    assert out.status_code == 200
    assert out.content == b'{"id": 1}'
    assert out.header("ETag") is None


@pytest.mark.asyncio
async def test_non_2xx_is_untouched():
    mw = _middleware()
    resp = _make_response(404, {"error": "nope"})
    out = await _run(mw, _make_request("GET", if_none_match="*"), resp)
    assert out.status_code == 404
    assert out.header("ETag") is None


@pytest.mark.asyncio
async def test_head_request_gets_etag_and_can_304():
    mw = _middleware()
    primed = await _run(mw, _make_request("HEAD"), _make_response(200, {"a": 1}))
    assert primed.header("ETag") is not None

    etag = primed.header("ETag")
    out = await _run(
        mw, _make_request("HEAD", if_none_match=etag), _make_response(200, {"a": 1})
    )
    assert out.status_code == 304


# ── Does not fight cache-control ──────────────────────────────────────


@pytest.mark.asyncio
async def test_does_not_touch_cache_control_on_200():
    mw = _middleware()
    resp = _make_response(200, {"a": 1})
    resp.cache_control("public, max-age=60")
    out = await _run(mw, _make_request("GET"), resp)
    assert out.headers.get("Cache-Control") == "public, max-age=60"


@pytest.mark.asyncio
async def test_cache_control_preserved_on_304():
    mw = _middleware()
    primed = await _run(mw, _make_request("GET"), _make_response(200, {"a": 1}))
    etag = primed.header("ETag")

    resp = _make_response(200, {"a": 1})
    resp.cache_control("public, max-age=60, stale-while-revalidate=30")
    out = await _run(mw, _make_request("GET", if_none_match=etag), resp)

    assert out.status_code == 304
    assert out.headers.get("Cache-Control") == (
        "public, max-age=60, stale-while-revalidate=30"
    )


# ── Robustness ───────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_never_raises_when_response_is_broken():
    """A malformed response must fall through, not blow up the chain."""
    mw = _middleware()

    class _Broken:
        # No status_code / content attributes the middleware can read.
        status_code = 200

        def __init__(self):
            self.content = b"{}"

        def header(self, *a, **k):  # raises on attempt to set ETag
            raise RuntimeError("boom")

    broken = _Broken()

    async def next_fn(_req):
        return broken

    out = await mw.handle(_make_request("GET"), next_fn)
    assert out is broken  # returned unchanged, no exception escaped


@pytest.mark.asyncio
async def test_streaming_body_is_skipped():
    """A non-bytes/str iterable content (streaming) yields no ETag."""
    mw = _middleware()
    resp = Response(MagicMock())
    resp.status(200)
    resp.content = iter([b"chunk1", b"chunk2"])  # generator-like body

    out = await _run(mw, _make_request("GET"), resp)
    assert out.header("ETag") is None
