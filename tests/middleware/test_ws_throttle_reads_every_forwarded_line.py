"""``ws.throttle`` reads every ``X-Forwarded-For`` line, not only the first.

Behind a proxy that ADDS its own header line (HAProxy ``option forwardfor``)
the client's line arrives first. ``Throttle._client_ip`` read that first line
alone, so the client wrote any address it liked there, named its own throttle
bucket and never ran out of handshakes. The lines are now joined, in order,
before the right-to-left trusted-proxy walk (RFC 9110 §5.3).
"""

from __future__ import annotations

import sys

import pytest

from cara.middleware.ws import Throttle

_ws_module = sys.modules["cara.middleware.ws.Throttle"]
_PROXY = "10.0.0.2"


class _Socket:
    def __init__(self, headers, client=(_PROXY, 5000)):
        self.scope = {"path": "/ws/deals", "headers": list(headers), "client": client}
        self.route = None


@pytest.fixture(autouse=True)
def only_the_proxy_is_trusted(monkeypatch):
    monkeypatch.setattr(
        _ws_module, "_is_trusted_proxy", lambda address: address == _PROXY
    )


def test_a_client_sent_first_line_does_not_name_the_bucket():
    socket = _Socket(
        [
            (b"x-forwarded-for", b"6.6.6.6"),  # written by the client
            (b"x-forwarded-for", b"203.0.113.9"),  # appended by the trusted proxy
        ]
    )

    assert Throttle._client_ip(socket) == "203.0.113.9"


def test_a_spoofed_line_cannot_vary_the_bucket():
    addresses = {
        Throttle._client_ip(
            _Socket(
                [
                    (b"x-forwarded-for", f"6.6.6.{n}".encode()),
                    (b"x-forwarded-for", b"203.0.113.9"),
                ]
            )
        )
        for n in range(20)
    }

    assert addresses == {"203.0.113.9"}


def test_one_merged_line_is_read_as_before():
    socket = _Socket([(b"x-forwarded-for", b"6.6.6.6, 203.0.113.9")])

    assert Throttle._client_ip(socket) == "203.0.113.9"


def test_an_untrusted_peer_is_still_its_own_address():
    socket = _Socket([(b"x-forwarded-for", b"6.6.6.6")], client=("198.51.100.7", 1))

    assert Throttle._client_ip(socket) == "198.51.100.7"
