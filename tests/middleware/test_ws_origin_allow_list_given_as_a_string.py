"""A WebSocket origin allow-list configured as one string is read as a list.

``broadcasting.websocket.allowed_origins`` set straight from an environment
value (``"https://app.cheapa.com,https://admin.cheapa.com"``) reached the gate
as a ``str``, and ``origin in allowed`` on a string is a SUBSTRING test: a
look-alike origin such as ``https://app.cheapa.co`` — any piece of the list —
passed the gate.
"""

from __future__ import annotations

import sys

import pytest

from cara.middleware.ws.Authenticate import Authenticate

# The package barrel shadows the submodule attribute.
module = sys.modules["cara.middleware.ws.Authenticate"]

_AS_STRING = "https://app.cheapa.com, https://admin.cheapa.com"


class _Socket:
    def __init__(self, origin: str) -> None:
        self.scope = {"headers": [(b"origin", origin.encode())]}


def _allowed(monkeypatch, configured, origin) -> bool:
    monkeypatch.setattr(module, "config", lambda key, default=None: configured)
    return Authenticate.__new__(Authenticate)._origin_is_allowed(_Socket(origin))


@pytest.mark.parametrize(
    "origin",
    [
        "https://app.cheapa.co",
        "https://app.cheapa",
        "app.cheapa.com",
        "https://app.cheapa.com, https://admin",
    ],
)
def test_a_piece_of_the_string_is_not_an_allowed_origin(monkeypatch, origin):
    assert _allowed(monkeypatch, _AS_STRING, origin) is False


@pytest.mark.parametrize("origin", ["https://app.cheapa.com", "https://admin.cheapa.com"])
def test_every_listed_origin_is_allowed(monkeypatch, origin):
    assert _allowed(monkeypatch, _AS_STRING, origin) is True


def test_an_unlisted_origin_is_refused(monkeypatch):
    assert _allowed(monkeypatch, _AS_STRING, "https://evil.example") is False
