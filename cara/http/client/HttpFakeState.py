"""Canonical definition of ``HttpFakeState``."""

from __future__ import annotations

import fnmatch
from typing import Any

import httpx

from .FakeExhaustedError import FakeExhaustedError
from .FakeResponses import coerce
from .StrayHttpRequestError import StrayHttpRequestError


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
                    response = coerce(entry)
                else:
                    response = coerce(stub)
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
        from .HttpFake import deactivate  # local: cycle with cara.http.client.HttpFake

        deactivate()
