"""Canonical definition of ``HttpFacade``."""

from __future__ import annotations

import fnmatch
from typing import Any

import httpx

from . import HttpFake
from .FakeResponses import make_response
from .PendingRequest import PendingRequest


class HttpFacade:
    """Static facade providing fluent HTTP client builders.

    Every method returns a :class:`PendingRequest` or executes directly.
    """

    @staticmethod
    def base_url(url: str) -> PendingRequest:
        return PendingRequest().base_url(url)

    @staticmethod
    def timeout(seconds: float) -> PendingRequest:
        return PendingRequest().timeout(seconds)

    @staticmethod
    def retry(
        times: int, *, backoff: float = 2.0, jitter: float = 0.15
    ) -> PendingRequest:
        return PendingRequest().retry(times, backoff=backoff, jitter=jitter)

    @staticmethod
    def retry_on(*status_codes: int) -> PendingRequest:
        return PendingRequest().retry_on(*status_codes)

    @staticmethod
    def with_headers(headers: dict[str, str]) -> PendingRequest:
        return PendingRequest().with_headers(headers)

    @staticmethod
    def with_token(token: str) -> PendingRequest:
        return PendingRequest().with_token(token)

    @staticmethod
    def accept_json() -> PendingRequest:
        return PendingRequest().accept_json()

    @staticmethod
    async def get(url: str, **kwargs: Any) -> httpx.Response:
        return await PendingRequest().get(url, **kwargs)

    @staticmethod
    async def post(url: str, **kwargs: Any) -> httpx.Response:
        return await PendingRequest().post(url, **kwargs)

    @staticmethod
    async def put(url: str, **kwargs: Any) -> httpx.Response:
        return await PendingRequest().put(url, **kwargs)

    @staticmethod
    async def patch(url: str, **kwargs: Any) -> httpx.Response:
        return await PendingRequest().patch(url, **kwargs)

    @staticmethod
    async def delete(url: str, **kwargs: Any) -> httpx.Response:
        return await PendingRequest().delete(url, **kwargs)

    # ── test-time faking (Laravel Http::fake parity) ──────────────────

    @staticmethod
    def fake(stubs: dict[str, Any] | None = None):
        """Route every request through an in-memory fake.

        ``stubs`` maps URL globs to responses (dict → JSON 200, int →
        bare status, str → text body, list → per-call sequence,
        ``Http.response(...)`` for full control). No stubs = everything
        200. Unmatched requests RAISE — a test that reaches the real
        network is a bug. Usable as a context manager for auto-restore.
        """

        return HttpFake.activate(stubs)

    @staticmethod
    def restore() -> None:
        """Drop the active fake — subsequent requests hit the network."""

        HttpFake.deactivate()

    @staticmethod
    def response(
        json: Any = None,
        status: int = 200,
        headers: dict[str, str] | None = None,
        body: str | bytes | None = None,
    ) -> httpx.Response:
        """Build a stub response for ``fake()`` maps (or direct returns)."""
        return make_response(json=json, status=status, headers=headers, body=body)

    @staticmethod
    def recorded() -> list[dict[str, Any]]:
        """Requests captured by the active fake, in send order."""

        state = HttpFake.current()
        return list(state.recorded) if state else []

    @staticmethod
    def assert_sent(matcher) -> None:
        """Assert at least one faked request satisfies ``matcher``.

        ``matcher`` is a URL glob string or a callable receiving the
        recorded request dict (``{"method", "url", ...send kwargs}``).
        """
        recorded = HttpFacade.recorded()
        if callable(matcher):
            if any(matcher(request) for request in recorded):
                return
        else:
            if any(
                fnmatch.fnmatch(request["url"], matcher)
                or fnmatch.fnmatch(request["url"].split("://", 1)[-1], matcher)
                for request in recorded
            ):
                return
        raise AssertionError(
            f"No recorded HTTP request matched {matcher!r}. "
            f"Recorded: {[(r['method'], r['url']) for r in recorded]}"
        )

    @staticmethod
    def assert_nothing_sent() -> None:
        """Assert the active fake recorded zero requests."""
        recorded = HttpFacade.recorded()
        if recorded:
            raise AssertionError(
                "Expected no HTTP requests, but recorded: "
                f"{[(r['method'], r['url']) for r in recorded]}"
            )
