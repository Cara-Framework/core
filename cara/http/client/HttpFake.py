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

from typing import Any

from .HttpFakeState import HttpFakeState

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
