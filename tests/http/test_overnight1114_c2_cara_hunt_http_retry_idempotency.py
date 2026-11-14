"""Cycle-2 Cara hunt: the HTTP client never resends a write that may have landed.

``Http.retry(n)`` re-sends on 429/5xx and on timeout/read errors for EVERY
method. For a POST/PATCH a 500/502/503/504 or a read timeout means the server
may already have acted (created the listing, captured the refund, sent the
message): sending it again doubles the effect. Market norm (urllib3
``Retry.allowed_methods``, RFC 9110 §9.2.2) retries only idempotent methods
on an ambiguous answer; a non-idempotent request is retried only when it
provably never reached the server (connect failure) or the server said
"not processed" (429 with Retry-After is a refusal, not an execution).

Red today: the POST is sent twice.
"""

from __future__ import annotations

import asyncio

import httpx
import pytest

from cara.facades import Http
from cara.http.client import HttpFake

URL = "https://api.example.com/refunds"


@pytest.fixture(autouse=True)
def _restore_fake():
    yield
    HttpFake.deactivate()


def _sends(method: str) -> int:
    return sum(1 for r in Http.recorded() if r["method"] == method)


@pytest.mark.parametrize("status", [500, 502, 503, 504])
def test_post_answered_with_an_ambiguous_5xx_is_sent_once(status):
    with Http.fake({"api.example.com/*": [status, {"ok": True}]}):
        response = asyncio.run(Http.retry(2).post(URL, json={"amount": "10.00"}))
        sends = _sends("POST")

    assert sends == 1, f"a POST answered {status} was sent {sends} times"
    assert response.status_code == status


def test_get_answered_with_503_is_still_retried():
    with Http.fake({"api.example.com/*": [503, {"ok": True}]}):
        response = asyncio.run(Http.retry(2).get(URL))

    assert response.status_code == 200


class _Transport:
    """Stands in for ``httpx.AsyncClient`` and counts the real sends."""

    def __init__(self, error: type[Exception]):
        self.error = error
        self.sends: list[str] = []

    def client(self, *args, **kwargs):
        transport = self

        class _Client:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                return False

            async def request(self, method, url, **kw):
                transport.sends.append(method)
                if len(transport.sends) == 1:
                    raise transport.error("boom", request=httpx.Request(method, url))
                return httpx.Response(200, request=httpx.Request(method, url))

        return _Client()


@pytest.fixture
def no_sleep(monkeypatch):
    async def _instant(_seconds):
        return None

    monkeypatch.setattr(asyncio, "sleep", _instant)


def test_post_whose_answer_timed_out_is_not_resent(monkeypatch, no_sleep):
    transport = _Transport(httpx.ReadTimeout)
    monkeypatch.setattr(httpx, "AsyncClient", transport.client)

    with pytest.raises(httpx.ReadTimeout):
        asyncio.run(Http.retry(2).post(URL, json={"amount": "10.00"}))

    assert transport.sends == ["POST"], transport.sends


@pytest.mark.parametrize("error", [httpx.ConnectError, httpx.ConnectTimeout])
def test_post_that_never_connected_is_retried(monkeypatch, no_sleep, error):
    # ConnectTimeout is also a TimeoutException: it must be classed "never
    # sent", not "ambiguous", or a DNS/connect blip fails a seller's push.
    transport = _Transport(error)
    monkeypatch.setattr(httpx, "AsyncClient", transport.client)

    response = asyncio.run(Http.retry(2).post(URL, json={"amount": "10.00"}))

    assert response.status_code == 200
    assert transport.sends == ["POST", "POST"]


class _StatusTransport:
    """Real-transport stand-in answering a scripted status sequence."""

    def __init__(self, *answers: tuple[int, dict[str, str]]):
        self.answers = list(answers)
        self.sends: list[str] = []

    def client(self, *args, **kwargs):
        transport = self

        class _Client:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                return False

            async def request(self, method, url, **kw):
                transport.sends.append(method)
                status, headers = transport.answers.pop(0)
                return httpx.Response(
                    status, headers=headers, request=httpx.Request(method, url)
                )

        return _Client()


def test_post_refused_with_429_waits_exactly_retry_after_then_resends(monkeypatch):
    # A 429 + Retry-After is "not processed": the write must go through once
    # the server asks, and the wait is the server's number, not number+backoff
    # (backoff on top makes a rate-limited bulk push crawl and miss its window).
    slept: list[float] = []

    async def _record(seconds):
        slept.append(seconds)

    monkeypatch.setattr(asyncio, "sleep", _record)
    transport = _StatusTransport((429, {"Retry-After": "3"}), (201, {}))
    monkeypatch.setattr(httpx, "AsyncClient", transport.client)

    response = asyncio.run(
        Http.retry(2, backoff=30.0).post(URL, json={"amount": "10.00"})
    )

    assert response.status_code == 201
    assert transport.sends == ["POST", "POST"]
    assert slept == [3.0]


def test_post_refused_with_429_without_retry_after_is_not_resent(monkeypatch, no_sleep):
    transport = _StatusTransport((429, {}), (201, {}))
    monkeypatch.setattr(httpx, "AsyncClient", transport.client)

    response = asyncio.run(Http.retry(2).post(URL, json={"amount": "10.00"}))

    assert response.status_code == 429
    assert transport.sends == ["POST"]


@pytest.mark.parametrize(
    "headers",
    [{"Idempotency-Key": "refund-42"}, [("X-Idempotency-Key", "refund-42")]],
    ids=["dict", "pairs"],
)
def test_post_carrying_an_idempotency_key_is_retried_after_503(headers):
    # The server dedupes a keyed replay, so the ambiguous answer is safe to retry.
    # httpx also takes headers as a list of pairs; the key must be seen there too.
    with Http.fake({"api.example.com/*": [503, {"ok": True}]}):
        response = asyncio.run(
            Http.retry(2).post(URL, json={"amount": "10.00"}, headers=headers)
        )
        sends = _sends("POST")

    assert response.status_code == 200
    assert sends == 2
