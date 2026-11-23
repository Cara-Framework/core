"""A non-UTF-8 header byte must not crash the exception handler.

HTTP header values are latin-1 on the wire. Behind a trusted proxy — and any
loopback peer is one — HTTPS detection decoded EVERY request header as strict
UTF-8 just to read ``X-Forwarded-Proto``. One high byte anywhere (a latin-1
``User-Agent``) raised ``UnicodeDecodeError``:

* on the error path nothing caught it, so ``DefaultExceptionHandler`` itself
  crashed and the 404 / 401 / 422 it was rendering left as a bare 500 with no
  envelope and no CORS headers;
* on the success path ``SecurityHeaders`` swallowed it and silently dropped
  ``Strict-Transport-Security`` from that response.
"""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import pytest

from cara.configuration import Configuration
from cara.exceptions import RouteNotFoundException
from cara.exceptions.handlers.DefaultExceptionHandler import DefaultExceptionHandler
from cara.middleware.http._SecurityHeaderPolicy import _scope_is_https
from cara.middleware.http.SecurityHeaders import SecurityHeaders


@pytest.fixture(autouse=True)
def _configuration(monkeypatch):
    monkeypatch.setattr(Configuration, "_instance", None)
    Configuration.empty().set("app.debug", False)


def _scope(*extra: tuple[bytes, bytes]) -> dict:
    return {
        "type": "http",
        "method": "GET",
        "path": "/api/missing",
        "scheme": "http",
        # Loopback: the reverse proxy / tunnel daemon on the same host.
        "client": ("127.0.0.1", 50000),
        "headers": [
            (b"host", b"api.example.com"),
            (b"user-agent", b"Caf\xe9Browser/1.0"),
            (b"x-forwarded-proto", b"https"),
            *extra,
        ],
    }


def test_exception_handler_still_answers_the_real_error():
    sent: list[dict] = []

    async def send(message: dict) -> None:
        sent.append(message)

    asyncio.run(
        DefaultExceptionHandler(None).handle(
            RouteNotFoundException("No route matches path '/api/missing'"),
            None,
            _scope(),
            None,
            send,
        )
    )

    start, body = sent
    assert start["status"] == 404  # was: UnicodeDecodeError out of the handler
    assert json.loads(body["body"])["error"].startswith("No route matches")
    headers = dict(start["headers"])
    assert headers[b"strict-transport-security"].startswith(b"max-age=")
    assert headers[b"x-content-type-options"] == b"nosniff"


def test_success_path_keeps_hsts_for_the_same_request():
    class _Response:
        def __init__(self):
            self.headers: dict[str, str] = {}

        def header(self, name, value):
            self.headers[name] = value

    response = _Response()

    async def next_fn(_request):
        return response

    middleware = SecurityHeaders(None)
    asyncio.run(middleware.handle(SimpleNamespace(scope=_scope()), next_fn))

    assert "Strict-Transport-Security" in response.headers


def test_forwarded_header_values_are_read_as_before():
    assert _scope_is_https(_scope()) is True
    assert _scope_is_https({**_scope(), "headers": [(b"forwarded", b"proto=https")]})
    assert not _scope_is_https({**_scope(), "headers": [(b"x-forwarded-proto", b"http")]})
    # A repeated header: the last line is the one the nearest proxy wrote.
    assert not _scope_is_https(
        {
            **_scope(),
            "headers": [
                (b"X-Forwarded-Proto", b"https"),
                (b"x-forwarded-proto", b"http"),
            ],
        }
    )
    # String-typed header pairs (non-ASGI callers) keep working.
    assert _scope_is_https({**_scope(), "headers": [("X-Forwarded-Proto", "https")]})


def test_untrusted_peer_cannot_claim_https():
    scope = {**_scope(), "client": ("203.0.113.9", 4000)}

    assert _scope_is_https(scope) is False
