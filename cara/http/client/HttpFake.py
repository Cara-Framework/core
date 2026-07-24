"""Test-time faking for the Cara HTTP client (Laravel ``Http::fake`` parity).

Usage::

    from cara.facades import Http

    # Stub by URL glob — dict → JSON 200, int → bare status, list → sequence
    with Http.fake(
        {
            "api.example.com/users/*": {"id": 1, "name": "Ada"},
            "api.example.com/flaky": [503, {"ok": True}],  # first call 503, then 200
            "*": Http.response(status=404),
        }
    ):
        response = await Http.get("https://api.example.com/users/1")

    Http.assert_sent(lambda r: r["method"] == "GET" and "/users/1" in r["url"])

Divergence from Laravel: unmatched requests RAISE (`StrayHttpRequestError`)
instead of passing through to the network — a test that talks to the real
internet is a bug, not a fallback.
"""

from __future__ import annotations

import fnmatch
from typing import Any

import httpx


class StrayHttpRequestError(AssertionError):
    """A faked test made an HTTP request no fake pattern covers."""


class FakeExhaustedError(AssertionError):
    """A response sequence ran out of entries."""


def make_response(
    json: Any = None,
    status: int = 200,
    headers: dict[str, str] | None = None,
    body: str | bytes | None = None,
) -> httpx.Response:
    """Build a real ``httpx.Response`` stub (usable inside and outside fakes)."""
    kwargs: dict[str, Any] = {"status_code": status, "headers": headers or {}}
    if json is not None:
        kwargs["json"] = json
    elif body is not None:
        kwargs["content"] = body.encode() if isinstance(body, str) else body
    return httpx.Response(**kwargs)


def _coerce(stub: Any) -> httpx.Response:
    """Normalize a registered stub into an ``httpx.Response``."""
    if isinstance(stub, httpx.Response):
        return stub
    if isinstance(stub, int):
        return make_response(status=stub)
    if isinstance(stub, (dict, list)):
        return make_response(json=stub)
    if isinstance(stub, str):
        return make_response(body=stub)
    raise TypeError(f"Unsupported fake response stub: {type(stub).__name__}")


class HttpFakeState:
    """Active fake registry + request recorder.

    Also a context manager so ``with Http.fake({...}):`` restores the
    real client automatically.
    """

    def __init__(self, stubs: dict[str, Any] | None):
        # ``fake()`` with no stubs = everything returns an empty 200.
        self._stubs: dict[str, Any] = dict(stubs) if stubs else {"*": 200}
        self.recorded: list[dict[str, Any]] = []

    # ── resolution ────────────────────────────────────────────────────

    def resolve(self, method: str, url: str, kwargs: dict[str, Any]) -> httpx.Response:
        self.recorded.append({"method": method, "url": url, **kwargs})

        bare = url.split("://", 1)[-1]
        for pattern, stub in self._stubs.items():
            if fnmatch.fnmatch(url, pattern) or fnmatch.fnmatch(bare, pattern):
                if isinstance(stub, list) and not isinstance(stub, httpx.Response):
                    # Sequence: consume one entry per matching request.
                    # (A JSON-array body should be wrapped via
                    # ``Http.response(json=[...])`` instead.)
                    if not stub:
                        raise FakeExhaustedError(
                            f"Http fake sequence for {pattern!r} is exhausted "
                            f"({method} {url})."
                        )
                    entry = stub.pop(0)
                    response = _coerce(entry)
                else:
                    response = _coerce(stub)
                # Attach the request so ``.raise_for_status()`` and
                # ``.request`` behave like a live response.
                response._request = httpx.Request(method, url)
                return response

        raise StrayHttpRequestError(
            f"Http fake received an unstubbed request: {method} {url}. "
            "Add a matching pattern (or a catch-all '*') to Http.fake({...})."
        )

    # ── context manager ───────────────────────────────────────────────

    def __enter__(self) -> HttpFakeState:
        return self

    def __exit__(self, *exc_info: Any) -> None:
        deactivate()


# Process-global active fake (tests are the only writer). Module-level on
# purpose: the client and the facade both consult it without container
# plumbing, matching how CacheFake/LogFake swap in.
_active: HttpFakeState | None = None


def activate(stubs: dict[str, Any] | None = None) -> HttpFakeState:
    global _active
    _active = HttpFakeState(stubs)
    return _active


def deactivate() -> None:
    global _active
    _active = None


def current() -> HttpFakeState | None:
    return _active
