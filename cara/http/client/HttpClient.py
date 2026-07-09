"""Cara HTTP Client — Laravel-style facade for external HTTP requests.

Provides a fluent interface for making HTTP requests with built-in retry,
exponential backoff, timeout management, and Retry-After header parsing.

Usage::

    from cara.facades import Http

    # Simple GET
    response = await Http.get("https://api.example.com/data")

    # With retry and timeout
    response = await Http.timeout(10).retry(3, backoff=2.0).get(url)

    # With headers
    response = await Http.with_headers({"Authorization": "Bearer ..."}).post(url, json=payload)

    # With base URL (for API clients)
    client = Http.base_url("https://api.example.com").with_headers({"X-API-Key": key})
    response = await client.get("/users")
"""

from __future__ import annotations

import asyncio
import random
from typing import Any

import httpx

# ``Log`` is imported lazily inside the one method that uses it. A module-top
# ``from cara.facades import Log`` re-enters ``cara.facades`` while it is still
# initialising (this module is pulled in by ``cara/facades/Http.py`` during
# ``cara.facades.__init__``), leaving later facades resolving to their submodules
# instead of the facade classes and breaking early boot. Keep it local.


class PendingRequest:
    """Fluent builder for an HTTP request with retry/timeout configuration."""

    def __init__(self) -> None:
        self._base_url: str = ""
        self._headers: dict[str, str] = {}
        self._timeout_seconds: float = 30.0
        self._retries: int = 0
        self._backoff_base: float = 2.0
        self._backoff_jitter: float = 0.15
        self._retry_on_status: set[int] = {429, 500, 502, 503, 504}

    def base_url(self, url: str) -> PendingRequest:
        self._base_url = url.rstrip("/")
        return self

    def timeout(self, seconds: float) -> PendingRequest:
        self._timeout_seconds = seconds
        return self

    def retry(self, times: int, *, backoff: float = 2.0, jitter: float = 0.15) -> PendingRequest:
        self._retries = times
        self._backoff_base = backoff
        self._backoff_jitter = jitter
        return self

    def retry_on(self, *status_codes: int) -> PendingRequest:
        self._retry_on_status = set(status_codes)
        return self

    def with_headers(self, headers: dict[str, str]) -> PendingRequest:
        self._headers.update(headers)
        return self

    def with_token(self, token: str) -> PendingRequest:
        self._headers["Authorization"] = f"Bearer {token}"
        return self

    def accept_json(self) -> PendingRequest:
        self._headers["Accept"] = "application/json"
        return self

    async def get(self, url: str, **kwargs: Any) -> httpx.Response:
        return await self._send("GET", url, **kwargs)

    async def post(self, url: str, **kwargs: Any) -> httpx.Response:
        return await self._send("POST", url, **kwargs)

    async def put(self, url: str, **kwargs: Any) -> httpx.Response:
        return await self._send("PUT", url, **kwargs)

    async def patch(self, url: str, **kwargs: Any) -> httpx.Response:
        return await self._send("PATCH", url, **kwargs)

    async def delete(self, url: str, **kwargs: Any) -> httpx.Response:
        return await self._send("DELETE", url, **kwargs)

    async def _send(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        full_url = f"{self._base_url}/{url.lstrip('/')}" if self._base_url else url
        last_exc: Exception | None = None

        from . import HttpFake

        for attempt in range(self._retries + 1):
            if attempt > 0 and HttpFake.current() is None:
                # Real transport only — faked retries stay instant so a
                # test exercising the retry ladder doesn't sleep.
                delay = self._backoff_base ** attempt
                delay *= 1.0 + random.uniform(-self._backoff_jitter, self._backoff_jitter)
                await asyncio.sleep(delay)

            fake = HttpFake.current()
            if fake is not None:
                response = fake.resolve(method, full_url, kwargs)
                if (
                    response.status_code in self._retry_on_status
                    and attempt < self._retries
                ):
                    last_exc = httpx.HTTPStatusError(
                        f"{response.status_code}",
                        request=response.request,
                        response=response,
                    )
                    continue
                return response

            try:
                async with httpx.AsyncClient(
                    timeout=self._timeout_seconds,
                    headers=self._headers,
                    http2=True,
                ) as client:
                    response = await client.request(method, full_url, **kwargs)

                if response.status_code in self._retry_on_status and attempt < self._retries:
                    retry_after = response.headers.get("Retry-After")
                    if retry_after and retry_after.isdigit():
                        await asyncio.sleep(min(int(retry_after), 60))
                    last_exc = httpx.HTTPStatusError(
                        f"{response.status_code}", request=response.request, response=response
                    )
                    continue

                return response

            except (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError) as exc:
                last_exc = exc
                if attempt >= self._retries:
                    raise
                from cara.facades import Log

                Log.debug(
                    "http.retry",
                    context={"url": full_url, "attempt": attempt + 1, "error": str(exc)},
                )

        raise last_exc  # type: ignore[misc]


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
    def retry(times: int, *, backoff: float = 2.0, jitter: float = 0.15) -> PendingRequest:
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
        from . import HttpFake

        return HttpFake.activate(stubs)

    @staticmethod
    def restore() -> None:
        """Drop the active fake — subsequent requests hit the network."""
        from . import HttpFake

        HttpFake.deactivate()

    @staticmethod
    def response(
        json: Any = None,
        status: int = 200,
        headers: dict[str, str] | None = None,
        body: str | bytes | None = None,
    ) -> httpx.Response:
        """Build a stub response for ``fake()`` maps (or direct returns)."""
        from . import HttpFake

        return HttpFake.make_response(json=json, status=status, headers=headers, body=body)

    @staticmethod
    def recorded() -> list[dict[str, Any]]:
        """Requests captured by the active fake, in send order."""
        from . import HttpFake

        state = HttpFake.current()
        return list(state.recorded) if state else []

    @staticmethod
    def assert_sent(matcher) -> None:
        """Assert at least one faked request satisfies ``matcher``.

        ``matcher`` is a URL glob string or a callable receiving the
        recorded request dict (``{"method", "url", ...send kwargs}``).
        """
        import fnmatch

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
