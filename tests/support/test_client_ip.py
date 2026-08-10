"""``cara.support.ClientIp`` — the IP an audit record is allowed to believe.

The helper's value is entirely in what it refuses to do: it never reads a
client-supplied header itself, and it never turns a broken request object
into a raised exception on an audit-write path.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace

from cara.support.ClientIp import trusted_client_ip


def test_delegates_to_request_ip():
    """``Request.ip()`` owns the TRUSTED_PROXIES walk; this helper adds
    tolerance, not a second IP-resolution policy."""
    assert trusted_client_ip(SimpleNamespace(ip=lambda: "203.0.113.9")) == "203.0.113.9"


def test_passes_through_a_none_from_request_ip():
    """No client tuple and no trusted proxy means "unknown" — recorded as an
    absent IP rather than substituted with a header value."""
    assert trusted_client_ip(SimpleNamespace(ip=lambda: None)) is None


def test_returns_none_when_the_object_has_no_ip_at_all():
    """Audit paths are routinely exercised with stand-in request objects."""
    assert trusted_client_ip(SimpleNamespace()) is None
    assert trusted_client_ip(object()) is None


def test_returns_none_when_ip_is_not_callable():
    """A slotted/fake request exposing ``ip`` as a plain attribute must not be
    returned as if it were a resolved IP — the value did not go through the
    trusted-proxy walk, so it is not trustworthy."""
    assert trusted_client_ip(SimpleNamespace(ip="10.0.0.1")) is None


def test_swallows_and_logs_a_raising_ip(caplog):
    """A failed IP lookup must never be the thing that fails the request the
    audit record is describing."""

    class Boom:
        def ip(self):
            raise RuntimeError("no scope")

    with caplog.at_level(logging.WARNING, logger="cara.support.ClientIp"):
        assert trusted_client_ip(Boom()) is None

    assert "request.ip() failed" in caplog.text
