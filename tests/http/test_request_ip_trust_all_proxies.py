"""``TRUSTED_PROXIES=*`` must not let the client choose ``request.ip()``.

Under trust-all every ``X-Forwarded-For`` hop counts as a trusted proxy, so
the right-to-left walk never finds an untrusted address and fell through to
the LEFTMOST entry — the one the client wrote. A client sending
``X-Forwarded-For: 1.2.3.4`` picked the address every per-IP rate limit and
audit row keyed on. The only honest answer under trust-all is the hop our
peer appended (the rightmost). An explicit proxy list keeps its behaviour.
"""

from __future__ import annotations

import importlib

import pytest

from cara.http.request import Request

request_module = importlib.import_module("cara.http.request.Request")
trusted_proxies = importlib.import_module("cara.security.TrustedProxies")


@pytest.fixture
def trusted(monkeypatch):
    """Set ``app.trusted_proxies`` for both readers of it."""

    def _set(value: str) -> None:
        def _config(key, default=None):
            return value if key == "app.trusted_proxies" else default

        monkeypatch.setattr(trusted_proxies, "config", _config)
        monkeypatch.setattr(request_module, "config", _config)

    return _set


def _request(forwarded: str, peer: str = "198.51.100.7") -> Request:
    return Request(application=None).load(
        {
            "type": "http",
            "client": (peer, 443),
            "headers": [(b"x-forwarded-for", forwarded.encode())],
        }
    )


def test_trust_all_answers_the_hop_our_peer_appended(trusted):
    trusted("*")
    assert _request("1.2.3.4, 203.0.113.9").ip() == "203.0.113.9"


def test_trust_all_ignores_a_single_client_written_entry_spoof(trusted):
    trusted("*")
    # The client wrote ``6.6.6.6``; the edge appended the real address.
    assert _request("6.6.6.6, 192.0.2.44, 203.0.113.9").ip() == "203.0.113.9"


def test_an_explicit_list_still_walks_to_the_first_untrusted_hop(trusted):
    trusted("203.0.113.0/24, 198.51.100.0/24")
    assert _request("1.2.3.4, 192.0.2.44, 203.0.113.9").ip() == "192.0.2.44"


def test_an_explicit_list_with_every_hop_trusted_keeps_the_first_entry(trusted):
    trusted("203.0.113.0/24, 198.51.100.0/24")
    assert _request("203.0.113.1, 203.0.113.9").ip() == "203.0.113.1"


def test_an_untrusted_peer_is_its_own_address(trusted):
    trusted("203.0.113.0/24")
    assert _request("1.2.3.4", peer="192.0.2.200").ip() == "192.0.2.200"
