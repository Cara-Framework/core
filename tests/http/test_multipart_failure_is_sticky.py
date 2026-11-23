"""A refused form body is refused on every read, not only the first.

``_parse_multipart`` marks the body as parsed (``_files = {}``) before it
parses, and caches nothing about a failure. So the first ``files()`` raised
"Too many files uploaded" and the SECOND returned the files parsed before the
limit — an upload the framework had just refused, handed over as accepted.
``all()`` forgave a form it could not parse, so a handler reading only
``all()`` answered on the query alone for an upload it had dropped; it now
raises the refusal exactly as ``files()`` does, on every read.
"""

from __future__ import annotations

import importlib

import pytest

from cara.exceptions import BadRequestException
from cara.http import Request

body_parsing = importlib.import_module("cara.http.request.mixins.MakesBodyParsing")
limits_module = importlib.import_module("cara.http.BodyLimits")

_BOUNDARY = "cara-test-boundary"


@pytest.fixture(autouse=True)
def _two_file_limit(monkeypatch):
    limits = {"server.max_files": 2}
    monkeypatch.setattr(
        limits_module, "config", lambda key, default=None: limits.get(key, default)
    )
    body_parsing._body_limits.cache_clear()
    yield
    body_parsing._body_limits.cache_clear()


def _multipart(*parts: tuple[str, str | None, bytes]) -> bytes:
    body = b""
    for name, filename, content in parts:
        disposition = f'form-data; name="{name}"'
        if filename is not None:
            disposition += f'; filename="{filename}"'
        body += f"--{_BOUNDARY}\r\nContent-Disposition: {disposition}\r\n".encode()
        if filename is not None:
            body += b"Content-Type: application/octet-stream\r\n"
        body += b"\r\n" + content + b"\r\n"
    return body + f"--{_BOUNDARY}--\r\n".encode()


def _request(raw: bytes, query: bytes = b"") -> Request:
    async def receive() -> dict:
        return {"type": "http.request", "body": raw, "more_body": False}

    headers = [
        (b"content-type", f"multipart/form-data; boundary={_BOUNDARY}".encode()),
    ]
    scope = {"type": "http", "headers": headers, "query_string": query}
    return Request(None).load(scope, receive)


_THREE_FILES = _multipart(
    ("title", None, b"Invoices"),
    ("first", "a.txt", b"aaa"),
    ("second", "b.txt", b"bbb"),
    ("third", "c.txt", b"ccc"),
)


@pytest.mark.asyncio
async def test_files_keeps_refusing_an_upload_over_the_file_limit() -> None:
    request = _request(_THREE_FILES)

    with pytest.raises(BadRequestException, match="Too many files"):
        await request.files()
    # Was: {"first": ..., "second": ...} — the refused upload, truncated.
    with pytest.raises(BadRequestException, match="Too many files"):
        await request.files()
    with pytest.raises(BadRequestException, match="Too many files"):
        await request.file("first")
    with pytest.raises(BadRequestException, match="Too many files"):
        await request.form()


@pytest.mark.asyncio
async def test_all_then_files_still_surfaces_the_refusal() -> None:
    request = _request(_THREE_FILES, query=b"page=2")

    # ``all()`` refuses the form exactly as ``files()`` does…
    with pytest.raises(BadRequestException, match="Too many files"):
        await request.all()
    # …the refusal is not turned into accepted files by a later read…
    with pytest.raises(BadRequestException, match="Too many files"):
        await request.files()
    # …and ``all()`` refuses again, never answering query + half a form.
    with pytest.raises(BadRequestException, match="Too many files"):
        await request.all()


@pytest.mark.asyncio
async def test_missing_boundary_is_reported_on_every_read() -> None:
    async def receive() -> dict:
        return {"type": "http.request", "body": b"x", "more_body": False}

    request = Request(None).load(
        {"type": "http", "headers": [(b"content-type", b"multipart/form-data")]},
        receive,
    )

    for _ in range(2):
        with pytest.raises(BadRequestException, match="boundary"):
            await request.files()


@pytest.mark.asyncio
async def test_accepted_upload_is_parsed_once_and_cached() -> None:
    request = _request(
        _multipart(("title", None, b"Invoices"), ("first", "a.txt", b"aaa"))
    )

    files = await request.files()

    assert await request.form() == {"title": "Invoices"}
    assert await request.files() is files
    assert files["first"].content == b"aaa"
    assert await request.all() == {"title": "Invoices"}


@pytest.mark.asyncio
async def test_form_refused_for_unstorable_text_still_reads_as_empty() -> None:
    """The NUL refusal keeps its own contract: raised once, then an EMPTY
    form (``all()`` is what keeps refusing it) — products pin exactly this."""

    async def receive() -> dict:
        return {"type": "http.request", "body": b"title=a%00b", "more_body": False}

    request = Request(None).load(
        {
            "type": "http",
            "headers": [(b"content-type", b"application/x-www-form-urlencoded")],
        },
        receive,
    )

    with pytest.raises(BadRequestException):
        await request.form()
    assert await request.form() == {}
    assert await request.files() == {}
    with pytest.raises(BadRequestException):
        await request.all()
