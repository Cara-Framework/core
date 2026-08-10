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

from cara.middleware.http import HandleCors


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
    bootstrap by installing a policy.

    RE-PINNED: this used to monkey-patch the ``config`` name that
    ``HandleCors`` imported at module top, because the middleware carried
    its own ``config("cors.cors.<key>", <default>)`` block. That block was
    the second copy of the policy — the one the error path had already
    been migrated off — so the patch target moved with it. The seam is now
    ``cara.configuration.config``, which means this fixture drives the REAL
    ``Cors.load_cors_policy``: the key list and the defaults under test are
    the shipped ones, not a restatement living in a test file.
    """
    import cara.configuration as configuration

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

    original_config = configuration.config
    configuration.config = fake_config

    app = MagicMock()
    mw = HandleCors(app)

    return mw, lambda: setattr(configuration, "config", original_config)


@pytest.fixture
def cors_middleware():
    mw, restore = _build_middleware()
    try:
        yield mw
    finally:
        restore()


def _make_request(method="GET", origin="https://app.example.com", path="/api/test"):
    req = MagicMock()
    req.method = method
    req.header = MagicMock(return_value=origin)
    # ``path`` MUST be set: HandleCors only applies CORS to requests whose path
    # matches the configured ``paths`` (``["api/*"]`` in the fake config) — a
    # MagicMock path never matches, so without this the middleware correctly
    # SKIPS CORS and no headers are attached. Use an in-scope api/ path.
    req.path = path
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


# ── Allow-Credentials is paired with a usable Allow-Origin ───────────


def _credentialed_middleware(allowed_origins, patterns=None):
    """A HandleCors with ``supports_credentials`` on and the given allowlist."""
    import cara.configuration as configuration

    fake_cfg = {
        "paths": ["api/*"],
        "allowed_methods": ["GET", "POST"],
        "allowed_origins": allowed_origins,
        "allowed_origins_patterns": patterns or [],
        "allowed_headers": ["Content-Type", "Authorization"],
        "exposed_headers": [],
        "max_age": 3600,
        "supports_credentials": True,
    }

    def fake_config(key, default=None):
        return fake_cfg.get(key.split(".")[-1], default)

    original_config = configuration.config
    configuration.config = fake_config
    try:
        return HandleCors(MagicMock())
    finally:
        configuration.config = original_config


def test_credentials_header_accompanies_an_allowlisted_origin():
    mw = _credentialed_middleware(["https://app.example.com"])
    resp = _FakeResponse()
    mw._add_cors_headers(_make_request(), resp)

    assert resp.headers["Access-Control-Allow-Origin"] == "https://app.example.com"
    assert resp.headers["Access-Control-Allow-Credentials"] == "true"
    # ACAO varies by request Origin, so caches must key on it.
    assert resp.headers["Vary"] == "Origin"


def test_credentials_header_is_withheld_when_the_origin_is_not_allowed():
    """``resolve_allow_origin`` fails closed for an off-allowlist origin, and
    the credentials header must fail closed WITH it.

    The condition used to be ``if supports_credentials`` alone, so this
    response went out as ``Allow-Credentials: true`` with no
    ``Allow-Origin`` — a pair no browser grants anything on, but one that
    tells any origin that asks that this endpoint accepts credentials. The
    docstring next to it already claimed the ACAO was required; only the
    code disagreed.
    """
    mw = _credentialed_middleware(["https://app.example.com"])
    resp = _FakeResponse()
    mw._add_cors_headers(_make_request(origin="https://attacker.example"), resp)

    assert "Access-Control-Allow-Origin" not in resp.headers
    assert "Access-Control-Allow-Credentials" not in resp.headers


def test_credentials_header_is_withheld_alongside_a_wildcard_origin():
    """Wildcard + credentials is the misconfiguration ``resolve_allow_origin``
    treats as "no origin allowed" rather than reflecting the caller's. The
    credentials header must not survive that decision either — emitting it
    beside a ``*`` is the shape browsers reject and operators misread as
    working."""
    mw = _credentialed_middleware(["*"])
    resp = _FakeResponse()
    mw._add_cors_headers(_make_request(origin="https://anyone.example"), resp)

    assert "Access-Control-Allow-Origin" not in resp.headers
    assert "Access-Control-Allow-Credentials" not in resp.headers
