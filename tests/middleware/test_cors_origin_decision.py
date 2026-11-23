"""Which origin a CORS response names, and what a cache may do with it.

Two defects in ``cara.middleware.http.Cors``:

* ``allowed_origins_patterns`` name WHOLE origins. ``origin_explicitly_allowed``
  tested each pattern with ``re.match``, which anchors only the start. A
  pattern written without a trailing ``$`` — ``https://.*\\.example\\.com`` —
  therefore also matched ``https://app.example.com.attacker.net``, and with
  ``supports_credentials`` the attacker's origin was reflected beside
  ``Access-Control-Allow-Credentials: true``: any page on that host could read
  the signed-in user's responses.
* ``Vary: Origin`` was sent only beside an ``Allow-Origin``, so the response
  that REFUSES an origin could be cached under the bare URL and replayed to
  the allowed one (second half of this file).
"""

from __future__ import annotations

import pytest

from cara.middleware.http.Cors import (
    CORS_DEFAULTS,
    cors_headers,
    origin_explicitly_allowed,
    resolve_allow_origin,
)

_UNANCHORED = r"https://.*\.example\.com"
_ANCHORED = r"^https?://(?:localhost|127\.0\.0\.1|\[::1\])(?::\d{1,5})?$"


def _policy(*patterns: str, credentials: bool = True) -> dict:
    return {
        **CORS_DEFAULTS,
        "allowed_origins": [],
        "allowed_origins_patterns": list(patterns),
        "supports_credentials": credentials,
    }


@pytest.mark.parametrize(
    "origin",
    [
        "https://app.example.com.attacker.net",
        "https://app.example.com:8443.attacker.net",
        "https://app.example.com/..@attacker.net",
        "https://app.example.comx",
    ],
)
def test_origin_that_only_starts_like_an_allowed_one_is_refused(origin):
    policy = _policy(_UNANCHORED)

    assert origin_explicitly_allowed(origin, policy) is False
    assert resolve_allow_origin(origin, policy) is None

    names = [name for name, _ in cors_headers(origin, policy)]
    assert "Access-Control-Allow-Origin" not in names
    assert "Access-Control-Allow-Credentials" not in names


def test_whole_origin_matches_keep_working_for_an_unanchored_pattern():
    policy = _policy(_UNANCHORED)

    assert origin_explicitly_allowed("https://app.example.com", policy) is True
    assert dict(cors_headers("https://eu.app.example.com", policy))[
        "Access-Control-Allow-Origin"
    ] == ("https://eu.app.example.com")


@pytest.mark.parametrize(
    ("origin", "allowed"),
    [
        ("http://localhost:3000", True),
        ("https://127.0.0.1", True),
        ("http://[::1]:5173", True),
        ("http://localhost.attacker.net", False),
        ("http://localhost:3000.attacker.net", False),
    ],
)
def test_explicitly_anchored_pattern_means_the_same_as_before(origin, allowed):
    assert origin_explicitly_allowed(origin, _policy(_ANCHORED)) is allowed


def test_prefix_match_is_refused_without_credentials_too():
    policy = _policy(_UNANCHORED, credentials=False)

    assert resolve_allow_origin("https://app.example.com.attacker.net", policy) is None


# ── Vary: Origin whenever the answer depends on the origin ───────────


@pytest.mark.parametrize("origin", [None, "", "https://refused.example"])
def test_refusal_also_varies_on_origin(origin):
    """A response WITHOUT ``Allow-Origin`` is as origin-dependent as one with.

    Without ``Vary: Origin`` on it, a shared cache keeps the no-ACAO copy
    under the bare URL (first fetched by a crawler or a server-side render)
    and serves it to the allowed cross-origin app, whose browser blocks it.
    """
    policy = {
        **CORS_DEFAULTS,
        "allowed_origins": ["https://app.example.com"],
        "supports_credentials": True,
    }

    headers = cors_headers(origin, policy)

    assert ("Vary", "Origin") in headers
    assert "Access-Control-Allow-Origin" not in [name for name, _ in headers]


def test_allowed_origin_still_varies_exactly_once():
    policy = {**CORS_DEFAULTS, "allowed_origins": ["https://app.example.com"]}

    headers = cors_headers("https://app.example.com", policy)

    assert headers[:2] == [
        ("Access-Control-Allow-Origin", "https://app.example.com"),
        ("Vary", "Origin"),
    ]
    assert [name for name, _ in headers].count("Vary") == 1


def test_wildcard_policy_does_not_vary():
    # ``*`` is the same answer for every origin: nothing to key a cache on.
    headers = cors_headers("https://anyone.example", dict(CORS_DEFAULTS))

    assert ("Access-Control-Allow-Origin", "*") in headers
    assert "Vary" not in [name for name, _ in headers]
