from __future__ import annotations

import importlib

import pytest

from cara.exceptions import UnsupportedMediaTypeException
from cara.http import Request

body_parsing = importlib.import_module("cara.http.request.mixins.MakesBodyParsing")
limits_module = importlib.import_module("cara.http.BodyLimits")


@pytest.fixture(autouse=True)
def _default_body_limits(monkeypatch):
    """The body limits come from an unconfigured application's defaults.

    Without this the file passed only after some earlier test in the same
    process had installed a configuration (alone, or on an xdist worker,
    ``config()`` raised "Configuration is unavailable")."""
    monkeypatch.setattr(limits_module, "config", lambda _key, default=None: default)
    body_parsing._body_limits.cache_clear()
    yield
    body_parsing._body_limits.cache_clear()


def _request(raw: bytes, content_type: str | None) -> Request:
    async def receive() -> dict:
        return {"type": "http.request", "body": raw, "more_body": False}

    headers = []
    if content_type is not None:
        headers.append((b"content-type", content_type.encode()))
    return Request(None).load({"type": "http", "headers": headers}, receive)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "content_type",
    [
        "application/json",
        "application/json; charset=utf-8",
        "application/merge-patch+json",
    ],
)
async def test_all_accepts_declared_json_representations(content_type: str) -> None:
    request = _request(b'{"name":"Ada"}', content_type)

    assert await request.all() == {"name": "Ada"}


@pytest.mark.asyncio
@pytest.mark.parametrize("content_type", [None, "text/plain", "application/octet-stream"])
async def test_all_rejects_nonempty_unsupported_representations(
    content_type: str | None,
) -> None:
    request = _request(b"{}", content_type)

    with pytest.raises(UnsupportedMediaTypeException) as caught:
        await request.all()

    error = caught.value
    assert error.status_code == 415
    assert error.error_type == "unsupported_media_type"
    assert error.received_media_type == (content_type or "missing")


@pytest.mark.asyncio
@pytest.mark.parametrize("content_type", [None, "text/plain"])
async def test_all_ignores_media_type_when_there_is_no_body(
    content_type: str | None,
) -> None:
    request = _request(b"", content_type)

    assert await request.all() == {}
