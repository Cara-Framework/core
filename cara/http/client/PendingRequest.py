"""Canonical definition of ``PendingRequest``."""

from __future__ import annotations

import asyncio
import random
import ssl
from typing import Any

import httpx

from cara.support import retry_after_seconds

from . import HttpFake
from ._SharedClients import _shared_client


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

    def _replay_safe(self, method: str, kwargs: dict[str, Any]) -> bool:
        """True when sending this request twice cannot double its effect.

        RFC 9110 §9.2.2 idempotent methods, or any request carrying an
        idempotency key (the server dedupes the replay).
        """
        if method.upper() in _IDEMPOTENT_METHODS:
            return True
        # httpx.Headers accepts every shape httpx does (dict, list of pairs,
        # Headers) and lower-cases names; iterating a list of pairs raw would
        # miss the key and refuse a safe retry.
        headers = httpx.Headers(self._headers)
        headers.update(httpx.Headers(kwargs.get("headers") or {}))
        return any(headers.get(name) for name in _IDEMPOTENCY_KEY_HEADERS)

    def _should_retry_status(self, response: httpx.Response, replay_safe: bool) -> bool:
        """Whether a status answer may be re-sent.

        A 5xx after a POST/PATCH is ambiguous — the server may already have
        created the listing or captured the refund — so only replay-safe
        requests retry it (urllib3 ``Retry.allowed_methods`` norm). A 429
        carrying ``Retry-After`` is an explicit "not processed" refusal and
        retries for every method.
        """
        if response.status_code not in self._retry_on_status:
            return False
        if replay_safe:
            return True
        return response.status_code == 429 and _retry_after_seconds(response) is not None

    def _backoff_delay(self, attempt: int) -> float:
        delay = self._backoff_base**attempt
        return delay * (1.0 + random.uniform(-self._backoff_jitter, self._backoff_jitter))

    async def _send(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        full_url = f"{self._base_url}/{url.lstrip('/')}" if self._base_url else url
        replay_safe = self._replay_safe(method, kwargs)
        poolable = self._poolable(method, kwargs)
        # Delay before the next attempt: the server's Retry-After when it named
        # one (it replaces backoff, it is not added to it), else exponential
        # backoff. Faked retries never sleep so retry-ladder tests stay instant.
        retry_after: float | None = None

        for attempt in range(self._retries + 1):
            if attempt > 0 and HttpFake.current() is None:
                delay = (
                    retry_after
                    if retry_after is not None
                    else self._backoff_delay(attempt)
                )
                await asyncio.sleep(delay)
            retry_after = None

            fake = HttpFake.current()
            if fake is not None:
                response = fake.resolve(method, full_url, kwargs)
                if attempt < self._retries and self._should_retry_status(
                    response, replay_safe
                ):
                    continue
                return response

            try:
                if poolable:
                    response = await self._send_pooled(method, full_url, kwargs)
                else:
                    response = await self._send_fresh(method, full_url, kwargs)
            except _NEVER_SENT_ERRORS as exc:
                # The request never reached the server: safe to resend any method.
                if attempt >= self._retries:
                    raise
                _log_retry(full_url, attempt, exc)
                continue
            except _AMBIGUOUS_ERRORS as exc:
                # Bytes may have reached the server: only replay-safe requests resend.
                if attempt >= self._retries or not replay_safe:
                    raise
                _log_retry(full_url, attempt, exc)
                continue

            if attempt < self._retries and self._should_retry_status(
                response, replay_safe
            ):
                retry_after = _retry_after_seconds(response)
                continue
            return response

        raise AssertionError("unreachable: the final attempt always returns or raises")

    @staticmethod
    def _poolable(method: str, kwargs: dict[str, Any]) -> bool:
        """Whether this request may ride a kept-alive pooled connection.

        Only methods this client already treats as replay-safe by themselves
        (an idempotency key does not count: POST and PATCH keep a fresh
        connection per call, exactly as before), and only with options the
        shared client carries per request and a body it can send twice. A
        redirect-following request stays fresh: its hops share cookies through
        the client's jar, which the shared client never fills.
        """
        if method.upper() not in _POOLED_METHODS:
            return False
        if not set(kwargs) <= _POOLED_KWARGS:
            return False
        content = kwargs.get("content")
        return content is None or isinstance(content, (bytes, str))

    async def _send_fresh(
        self, method: str, url: str, kwargs: dict[str, Any]
    ) -> httpx.Response:
        """One request on its own client and connection (the pre-pool path)."""
        async with httpx.AsyncClient(
            timeout=self._timeout_seconds,
            headers=self._headers,
            http2=True,
            verify=_tls_context(),
        ) as client:
            return await client.request(method, url, **kwargs)

    async def _send_pooled(
        self, method: str, url: str, kwargs: dict[str, Any]
    ) -> httpx.Response:
        """One request on the loop's shared client.

        The builder's headers and timeout go with the request, never onto the
        shared client: merged in the order a per-call client merged them
        (builder headers, then the call's own), so the wire is unchanged.

        A kept-alive connection the server closed between requests fails as
        "disconnected without a response", a read error or a write error.
        When the request went out on a REUSED connection (no TCP connect was
        traced for it), that failure is the stale connection, not the
        server's answer: it is retried once on a fresh connection, whose
        outcome (success or error) is exactly what the pre-pool path gives.
        """
        client = await _shared_client(_tls_context())
        options = dict(kwargs)
        headers = httpx.Headers(self._headers)
        headers.update(httpx.Headers(kwargs.get("headers") or {}))
        options["headers"] = headers
        options.setdefault("timeout", self._timeout_seconds)
        opened: list[bool] = []

        async def trace(event: str, _info: dict[str, Any]) -> None:
            # "connection." on a direct or HTTP-proxy route, "socks." via SOCKS.
            if event.endswith(".connect_tcp.started"):
                opened.append(True)

        options["extensions"] = {"trace": trace}
        try:
            return await client.request(method, url, **options)
        except _STALE_CONNECTION_ERRORS:
            if opened:
                raise
        return await self._send_fresh(method, url, kwargs)


_shared_tls_context: ssl.SSLContext | None = None


def _tls_context() -> ssl.SSLContext:
    """The verifying TLS context every attempt's client uses, built once.

    A client is built per attempt, and ``httpx.AsyncClient()`` with the
    default ``verify=True`` builds its own TLS context each time — parsing
    the whole CA bundle, 5.7 ms of synchronous CPU on the event loop before a
    byte is sent, on every outbound call. This is the context httpx would
    have built (``httpx.create_ssl_context()`` with its defaults: certificate
    and hostname verification on, ``SSL_CERT_FILE`` / ``SSL_CERT_DIR``
    honoured), built once per process and shared; a client then costs 0.3 ms.
    """
    global _shared_tls_context
    if _shared_tls_context is None:
        _shared_tls_context = httpx.create_ssl_context()
    return _shared_tls_context


_IDEMPOTENT_METHODS = frozenset({"GET", "HEAD", "OPTIONS", "PUT", "DELETE", "TRACE"})
_IDEMPOTENCY_KEY_HEADERS = frozenset({"idempotency-key", "x-idempotency-key"})
_POOLED_METHODS = frozenset({"GET", "HEAD", "OPTIONS", "PUT", "DELETE"})
# Per-request arguments the shared client applies to this request alone.
# Anything else (``extensions``, ``follow_redirects``, ``files``, or an option
# httpx only takes on a client) keeps the per-call client and its behaviour.
_POOLED_KWARGS = frozenset(
    {"params", "headers", "cookies", "auth", "timeout", "content", "data", "json"}
)
# What a server closing a kept-alive connection under a request looks like.
_STALE_CONNECTION_ERRORS = (
    httpx.RemoteProtocolError,
    httpx.ReadError,
    httpx.WriteError,
)
# ConnectTimeout/PoolTimeout subclass TimeoutException, so this tuple is
# matched first: a connect or pool wait failure means nothing was sent.
_NEVER_SENT_ERRORS = (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout)
_AMBIGUOUS_ERRORS = (httpx.TimeoutException, httpx.ReadError)
_RETRY_AFTER_CAP_SECONDS = 60.0


def _retry_after_seconds(response: httpx.Response) -> float | None:
    """The answer's ``Retry-After``, capped for an in-request wait."""
    return retry_after_seconds(response.headers, cap=_RETRY_AFTER_CAP_SECONDS)


def _log_retry(url: str, attempt: int, exc: Exception) -> None:
    from cara.facades import Log  # local: cycle with cara.facades

    Log.debug(
        "http.retry", context={"url": url, "attempt": attempt + 1, "error": str(exc)}
    )
