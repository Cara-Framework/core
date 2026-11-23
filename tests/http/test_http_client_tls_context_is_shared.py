"""Outbound HTTP attempts share one TLS context instead of building one each.

``PendingRequest`` builds an ``httpx.AsyncClient`` per attempt. With the
default ``verify=True`` every client built its own TLS context — parsing the
CA bundle, 5.7 ms of synchronous CPU on the event loop per outbound call. The
context is now built once per process (0.3 ms per client after that), and it
must be exactly as strict as the one httpx builds by default.
"""

from __future__ import annotations

import asyncio
import importlib
import ssl

import httpx

from cara.facades import Http

pending_module = importlib.import_module("cara.http.client.PendingRequest")

URL = "https://api.example.com/items"


def _record_clients(monkeypatch) -> list[dict]:
    built: list[dict] = []

    def client(*args, **kwargs):
        built.append(kwargs)

        class _Client:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                return False

            async def request(self, method, url, **kw):
                return httpx.Response(200, request=httpx.Request(method, url))

        return _Client()

    monkeypatch.setattr(httpx, "AsyncClient", client)
    return built


def test_every_attempt_is_given_the_same_verifying_context(monkeypatch):
    built = _record_clients(monkeypatch)

    for _ in range(3):
        assert asyncio.run(Http.timeout(5).get(URL)).status_code == 200

    contexts = [kwargs["verify"] for kwargs in built]
    assert len(contexts) == 3
    assert all(context is contexts[0] for context in contexts)
    assert isinstance(contexts[0], ssl.SSLContext)


def test_the_context_is_built_once_per_process(monkeypatch):
    _record_clients(monkeypatch)
    monkeypatch.setattr(pending_module, "_shared_tls_context", None)
    builds: list[int] = []
    real_create = httpx.create_ssl_context

    def counting_create(*args, **kwargs):
        builds.append(1)
        return real_create(*args, **kwargs)

    monkeypatch.setattr(httpx, "create_ssl_context", counting_create)

    for _ in range(4):
        asyncio.run(Http.retry(1).post(URL, json={"n": 1}))

    assert len(builds) == 1  # was one CA-bundle parse per client


def test_shared_context_verifies_certificates_and_hostnames():
    context = pending_module._tls_context()

    assert context.verify_mode == ssl.CERT_REQUIRED
    assert context.check_hostname is True
    # The same strictness httpx's own default context has.
    default = httpx.create_ssl_context()
    assert context.minimum_version == default.minimum_version
    assert context.options == default.options


def test_per_request_settings_still_reach_the_client(monkeypatch):
    built = _record_clients(monkeypatch)

    asyncio.run(Http.timeout(7).with_token("t0k").get(URL))

    assert built[0]["timeout"] == 7
    assert built[0]["headers"] == {"Authorization": "Bearer t0k"}
    assert built[0]["http2"] is True
