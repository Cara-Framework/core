"""The response's last-resort error path logs when even the fallback 500 fails.

``BaseResponse._handle_error`` ends in a fallback that sends a plain 500. When
that ``send`` also failed (the connection is already gone), the ``except``
was a bare ``pass``: the client got no response and nothing anywhere said
why. It still must not raise, but it now logs a warning naming the failure.
"""

from __future__ import annotations

import importlib
import logging
from unittest.mock import MagicMock

import pytest

from cara.http.response.BaseResponse import BaseResponse

_base_module = importlib.import_module("cara.http.response.BaseResponse")


@pytest.fixture(autouse=True)
def _production(monkeypatch):
    monkeypatch.setattr(_base_module, "config", lambda _key, default=None: False)


@pytest.mark.asyncio
async def test_a_dead_connection_on_the_fallback_is_warned_not_raised(caplog):
    response = BaseResponse(MagicMock())

    async def send(_event):
        raise ConnectionResetError("peer closed")

    with caplog.at_level(logging.WARNING, logger="cara.http.response"):
        await response._handle_error(RuntimeError("encoder failed"), send)

    warnings = [r for r in caplog.records if r.name == "cara.http.response"]
    assert len(warnings) == 1
    assert warnings[0].levelno == logging.WARNING
    assert "ConnectionResetError" in warnings[0].getMessage()
    assert "peer closed" in warnings[0].getMessage()
    assert response.is_sent() is False


@pytest.mark.asyncio
async def test_a_fallback_that_reaches_the_client_logs_nothing(caplog):
    response = BaseResponse(MagicMock())
    sent: list[dict] = []
    calls = {"n": 0}

    async def send(event):
        calls["n"] += 1
        if calls["n"] == 1:  # the JSON 500 fails, the plain fallback goes out
            raise ConnectionResetError("transient")
        sent.append(event)

    with caplog.at_level(logging.WARNING, logger="cara.http.response"):
        await response._handle_error(RuntimeError("encoder failed"), send)

    assert [r for r in caplog.records if r.name == "cara.http.response"] == []
    assert sent[0]["status"] == 500
    assert sent[-1]["body"] == b"Internal Server Error"
