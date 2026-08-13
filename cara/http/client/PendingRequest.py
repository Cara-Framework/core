"""Canonical definition of ``PendingRequest``."""

from __future__ import annotations

import asyncio
import random
from typing import Any

import httpx

from . import HttpFake


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

    def retry(
        self, times: int, *, backoff: float = 2.0, jitter: float = 0.15
    ) -> PendingRequest:
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

        for attempt in range(self._retries + 1):
            if attempt > 0 and HttpFake.current() is None:
                # Real transport only — faked retries stay instant so a
                # test exercising the retry ladder doesn't sleep.
                delay = self._backoff_base**attempt
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

                if (
                    response.status_code in self._retry_on_status
                    and attempt < self._retries
                ):
                    retry_after = response.headers.get("Retry-After")
                    if retry_after and retry_after.isdigit():
                        await asyncio.sleep(min(int(retry_after), 60))
                    last_exc = httpx.HTTPStatusError(
                        f"{response.status_code}",
                        request=response.request,
                        response=response,
                    )
                    continue

                return response

            except (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError) as exc:
                last_exc = exc
                if attempt >= self._retries:
                    raise
                from cara.facades import Log  # local: cycle with cara.facades

                Log.debug(
                    "http.retry",
                    context={"url": full_url, "attempt": attempt + 1, "error": str(exc)},
                )

        raise last_exc  # type: ignore[misc]
