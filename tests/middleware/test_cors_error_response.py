"""Regression test for the HandleCors middleware.

Previously ``handle`` was structured as::

    response = await next_handler(request)
    self._add_cors_headers(request, response)
    return response

When ``next_handler`` raised (auth failure, 500, validation), the
header-application step was skipped. Browsers enforce CORS on every
status code — the JS client sees a generic CORS error and the real
status is unreachable. The fix wraps the inner call in try/finally so
headers are applied regardless of how the chain terminates.
"""

from unittest.mock import MagicMock

import pytest

from cara.middleware.http.HandleCors import HandleCors


class _FakeResponse:
    """Minimum Response-shaped stub for header capture."""

    def __init__(self):
        self.headers = {}
        self._status = 200

    def header(self, key, value):
        self.headers[key] = value
        return self

    def status(self, code):
        self._status = code
        return self


def _build_middleware():
    """Construct HandleCors without going through the full Application
    bootstrap by patching out config loading.

    NB: ``cara.middleware.http.HandleCors`` is shadowed by the
    ``HandleCors`` class re-exported from the package ``__init__``
    when accessed via dotted attribute, so we have to reach the
    module through ``sys.modules`` to monkey-patch the ``config``
    name imported at module top.
    """
    import sys

    cors_mod = sys.modules["cara.middleware.http.HandleCors"]

    fake_cfg = {
        "paths": ["api/*"],
        "allowed_methods": ["GET", "POST"],
        "allowed_origins": ["https://app.example.com"],
        "allowed_origins_patterns": [],
        "allowed_headers": ["Content-Type", "Authorization"],
        "exposed_headers": [],
        "max_age": 3600,
        "supports_credentials": False,
    }

    def fake_config(key, default=None):
        # Look up the sub-key after "cors.cors."
        leaf = key.split(".")[-1]
        return fake_cfg.get(leaf, default)

    original_config = cors_mod.config
    cors_mod.config = fake_config

    app = MagicMock()
    mw = HandleCors(app)

    return mw, lambda: setattr(cors_mod, "config", original_config)


@pytest.fixture
def cors_middleware():
    mw, restore = _build_middleware()
    try:
        yield mw
    finally:
        restore()


def _make_request(method="GET", origin="https://app.example.com"):
    req = MagicMock()
    req.method = method
    req.header = MagicMock(return_value=origin)
    return req


# ── Headers on the happy path ────────────────────────────────────────


@pytest.mark.asyncio
async def test_cors_headers_applied_on_successful_response(cors_middleware):
    req = _make_request()
    resp = _FakeResponse()

    async def next_handler(request):
        return resp

    out = await cors_middleware.handle(req, next_handler)
    assert out is resp
    assert "Access-Control-Allow-Origin" in resp.headers
    assert resp.headers["Access-Control-Allow-Origin"] == "https://app.example.com"


# ── The bug we are fixing: headers on error path ─────────────────────


@pytest.mark.asyncio
async def test_cors_headers_applied_when_inner_handler_raises_http_exception(
    cors_middleware,
):
    """When the downstream chain raises an exception carrying a
    response (e.g. ``HttpException(response=...)``), headers must be
    attached to that response before the exception re-raises — so
    the framework's outer exception handler can serialize a CORS-
    compliant error to the client."""
    error_response = _FakeResponse()
    error_response.status(401)

    class _AuthError(Exception):
        def __init__(self):
            self.response = error_response

    async def next_handler(request):
        raise _AuthError()

    req = _make_request()
    with pytest.raises(_AuthError):
        await cors_middleware.handle(req, next_handler)

    # Headers must still have been written to the error response.
    assert "Access-Control-Allow-Origin" in error_response.headers, (
        "CORS headers must be attached even when the chain raises — "
        "without them the browser hides the real status code from JS"
    )
    assert error_response.headers["Access-Control-Allow-Origin"] == (
        "https://app.example.com"
    )


@pytest.mark.asyncio
async def test_cors_headers_skipped_only_when_no_response_to_attach_to(
    cors_middleware,
):
    """If the exception carries no response object, there is nothing
    to attach headers to — the outer error handler will build the
    response. The middleware must NOT swallow the exception."""

    class _RawError(RuntimeError):
        pass

    async def next_handler(request):
        raise _RawError("boom")

    req = _make_request()
    with pytest.raises(_RawError):
        await cors_middleware.handle(req, next_handler)


@pytest.mark.asyncio
async def test_cors_middleware_does_not_swallow_inner_exception(cors_middleware):
    """The fix uses try/except + raise — the original exception must
    propagate unchanged."""

    class _SentinelError(Exception):
        pass

    async def next_handler(request):
        raise _SentinelError("propagate me")

    req = _make_request()
    with pytest.raises(_SentinelError, match="propagate me"):
        await cors_middleware.handle(req, next_handler)


@pytest.mark.asyncio
async def test_header_application_failure_does_not_mask_original_exception(
    cors_middleware,
):
    """If header application itself throws (corrupt response object),
    the original exception path must still surface — masking it would
    hide the real failure cause behind a header-application stack
    trace."""

    class _BrokenResponse:
        # Causes _add_cors_headers to raise (no .header attribute).
        pass

    class _InnerErr(Exception):
        def __init__(self):
            self.response = _BrokenResponse()

    async def next_handler(request):
        raise _InnerErr()

    req = _make_request()
    with pytest.raises(_InnerErr):
        await cors_middleware.handle(req, next_handler)
