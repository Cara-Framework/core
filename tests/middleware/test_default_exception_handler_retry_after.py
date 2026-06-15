"""``DefaultExceptionHandler`` — ``Retry-After`` HTTP header contract.

``HttpException.ServiceUnavailableException`` (and the ORM-side twin
``DatabaseUnavailableException`` raised on connection-pool exhaustion)
documents in its docstring that ``retry_after`` is surfaced "both in
the JSON envelope AND the ``Retry-After`` header so callers don't have
to parse the body to know when to come back."

Pre-fix only the body half landed. ``format_response`` propagated
``retry_after`` into the response dict, and the 5xx-prod redaction
allowlist preserved it across the generic-message rewrite — but
``send_response`` only emitted CORS / security-header / request-id
extras, never promoted ``retry_after`` to the HTTP layer. Load
balancers, browser retry, urllib3's ``Retry`` adapter, and
``requests``' ``Retry`` config all read the header, not the body —
they fell back to default exponential backoff or pinned long
intervals on what should have been a "come back in 1 second" 503.

The fix adds a ``_retry_after_header_for(data)`` helper that
inspects ``data`` (the response body the handler is about to send)
and emits ``Retry-After: N`` when ``retry_after`` is present and
positive. These tests pin the contract on the handler unit:
``data`` carries ``retry_after`` → ``Retry-After`` header in the
ASGI send payload.

Tests deliberately use the *manual* send path
(``send_manual_response``) so we can observe the raw header list
without wiring up a full ``Response`` object. Same code path runs
on the production fast-path via ``send_response`` → ``extras`` list
(``send_response`` calls ``response.header(name, value)`` for each
extra), so the assertion on extras covers both branches.
"""

from __future__ import annotations

import importlib
import json
from typing import Any

import pytest


_handler_mod = importlib.import_module("cara.exceptions.handlers.DefaultExceptionHandler")
DefaultExceptionHandler = _handler_mod.DefaultExceptionHandler


# ── ASGI send recorder ─────────────────────────────────────────


class _SendRecorder:
    """Records every ASGI send() message so tests can assert against
    the raw header list on ``http.response.start``."""

    def __init__(self) -> None:
        self.messages: list[dict[str, Any]] = []

    async def __call__(self, message: dict[str, Any]) -> None:
        self.messages.append(message)

    def headers(self) -> list[tuple[bytes, bytes]]:
        for m in self.messages:
            if m.get("type") == "http.response.start":
                return [(k, v) for k, v in (m.get("headers") or [])]
        return []

    def status(self) -> int | None:
        for m in self.messages:
            if m.get("type") == "http.response.start":
                return int(m.get("status") or 0)
        return None

    def body(self) -> bytes:
        for m in self.messages:
            if m.get("type") == "http.response.body":
                return bytes(m.get("body") or b"")
        return b""


def _scope() -> dict[str, Any]:
    return {
        "type": "http",
        "scheme": "http",
        "method": "GET",
        "path": "/",
        "headers": [],
        "client": None,
    }


# ── Helper-unit tests ──────────────────────────────────────────


class TestRetryAfterHelper:
    """The static helper is the single source of truth for the
    header pair. Pin its acceptance / rejection shape directly."""

    def test_present_positive_int_emits_pair(self) -> None:
        result = DefaultExceptionHandler._retry_after_header_for({"retry_after": 5})
        assert result == [[b"retry-after", b"5"]]

    def test_present_positive_string_coerced(self) -> None:
        # Some exception constructors pass string seconds (e.g.
        # config-driven). The helper coerces via ``int`` — RFC 7231
        # §7.1.3 delta-seconds form is integer.
        result = DefaultExceptionHandler._retry_after_header_for({"retry_after": "30"})
        assert result == [[b"retry-after", b"30"]]

    def test_zero_dropped(self) -> None:
        # 0 means "no wait" — emitting ``Retry-After: 0`` would tell
        # the client to hammer the failing endpoint without backoff.
        # Drop and let the client's default backoff strategy run.
        assert DefaultExceptionHandler._retry_after_header_for({"retry_after": 0}) == []

    def test_negative_dropped(self) -> None:
        assert DefaultExceptionHandler._retry_after_header_for({"retry_after": -1}) == []

    def test_non_numeric_dropped(self) -> None:
        # A bogus value (e.g. ``"soon"``) must not crash the handler
        # or land on the wire as ``Retry-After: soon`` (RFC 7231
        # tolerant parsers might accept HTTP-date there, but ours is
        # the delta-seconds branch only).
        assert (
            DefaultExceptionHandler._retry_after_header_for({"retry_after": "soon"}) == []
        )

    def test_absent_dropped(self) -> None:
        # Standard 4xx without retry hints — no header, body
        # unchanged. Pre-fix this branch already silently did the
        # right thing; pin it so a future refactor doesn't start
        # emitting ``Retry-After: None`` or similar.
        assert (
            DefaultExceptionHandler._retry_after_header_for({"error": "not found"}) == []
        )

    def test_none_data_safe(self) -> None:
        # Defensive — the helper is called from ``send_response``
        # which receives ``data`` from ``format_response``; a future
        # refactor that passes None must not crash the error path.
        assert (
            DefaultExceptionHandler._retry_after_header_for(
                None  # type: ignore[arg-type]
            )
            == []
        )


# ── End-to-end via send_manual_response ─────────────────────────


@pytest.mark.asyncio
async def test_503_retry_after_lands_as_http_header() -> None:
    """The load-bearing case: a ``DatabaseUnavailableException``-shaped
    503 must ship ``Retry-After: 1`` so load balancers honour the
    documented retry hint instead of falling back to long default
    backoff."""
    handler = DefaultExceptionHandler(application=None)
    data = {
        "error": "Internal server error",
        "type": "internal_error",
        "retry_after": 1,
    }
    recorder = _SendRecorder()
    retry = handler._retry_after_header_for(data)
    await handler.send_manual_response(
        data=data,
        status_code=503,
        scope=_scope(),
        receive=None,
        send=recorder,
        extra_headers=retry,
    )

    headers = dict(recorder.headers())
    assert recorder.status() == 503
    # The HTTP header — the part load balancers + urllib3 / requests
    # ``Retry`` adapters actually look at.
    assert headers.get(b"retry-after") == b"1", (
        f"Retry-After HTTP header missing or wrong: "
        f"{recorder.headers()!r}. Pre-fix the body carried "
        f"retry_after but the HTTP header was absent — clients fell "
        f"back to default backoff."
    )
    # Body still carries the documented JSON contract.
    parsed = json.loads(recorder.body().decode())
    assert parsed.get("retry_after") == 1


@pytest.mark.asyncio
async def test_404_without_retry_after_does_not_emit_header() -> None:
    """Regression guard: a normal 404 / 422 with no retry hint must
    NOT emit ``Retry-After``. The header is a positive signal — its
    presence tells the client "yes, try again later" — and emitting
    it on every error response would mis-train clients into retrying
    things they shouldn't (a 404 isn't going to start existing if you
    wait)."""
    handler = DefaultExceptionHandler(application=None)
    data = {"error": "Not Found", "type": "not_found"}
    recorder = _SendRecorder()
    retry = handler._retry_after_header_for(data)
    await handler.send_manual_response(
        data=data,
        status_code=404,
        scope=_scope(),
        receive=None,
        send=recorder,
        extra_headers=retry,
    )

    headers = dict(recorder.headers())
    assert recorder.status() == 404
    assert b"retry-after" not in headers


@pytest.mark.asyncio
async def test_retry_after_survives_5xx_prod_redaction() -> None:
    """End-to-end pin on the format_response → send_response chain:
    a 503 in production mode (no debug) collapses the body to the
    generic envelope but PRESERVES ``retry_after`` via the
    ``_5XX_PROD_SAFE_KEYS`` allowlist. The HTTP header must then
    follow because it reads from the post-redaction body.
    """
    # Stub config so is_debug_mode() returns False (prod).

    class _ProdConfig:
        @staticmethod
        def __call__(key, default=None):
            if key == "app.debug":
                return False
            return default

    # The handler accesses ``config`` lazily inside ``is_debug_mode``,
    # ``_security_headers_for_scope``, etc. Patch the module-level
    # ``config`` import path for the duration of the test.
    from cara import configuration as _cfg

    original = _cfg.config
    _cfg.config = _ProdConfig()  # type: ignore[assignment]
    try:
        handler = DefaultExceptionHandler(application=None)

        # Build an exception-shaped object with retry_after but no
        # to_dict — exercises the getattr propagation path.
        class _PoolExhausted(Exception):
            status_code = 503

            def __init__(self) -> None:
                super().__init__("Connection pool exhausted")
                self.retry_after = 1

        exc = _PoolExhausted()
        body = handler.format_response(exc, 503)
        # Redacted: generic error/type, but retry_after preserved.
        assert body == {
            "error": "Internal server error",
            "type": "internal_error",
            "retry_after": 1,
        }
        # The header helper reads from the post-redaction body —
        # ``Retry-After: 1`` survives.
        assert handler._retry_after_header_for(body) == [[b"retry-after", b"1"]]
    finally:
        _cfg.config = original  # type: ignore[assignment]
