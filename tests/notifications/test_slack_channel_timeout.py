"""SlackChannel.send must pass a hard timeout to urlopen.

Bug shape (pre-fix)
~~~~~~~~~~~~~~~~~~~
``_send_to_slack`` called ``urllib.request.urlopen(req)`` with NO
``timeout`` argument. The Python default is the global socket timeout
which is ``None`` (infinite) unless explicitly overridden. If the Slack
webhook endpoint hangs (TLS handshake stall, response never arrives,
network black-hole), the calling worker thread blocks indefinitely —
consuming a queue slot, stalling the notification pipeline, and
eventually triggering the queue-backlog watchdog alert without any
recovery path short of a process kill.

Contrast with ``AlertSink`` (5 s) and the logging ``SlackChannel``
(2 s), both of which pass explicit timeouts. The notification-framework
channel was the only code path missing it.

Fix: ``urlopen(req, timeout=5)``

What this file pins
~~~~~~~~~~~~~~~~~~~
Mocks ``urllib.request.urlopen`` and asserts it receives a ``timeout``
keyword argument > 0 on every send path.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from cara.notifications.channels.SlackChannel import SlackChannel


@pytest.fixture
def channel() -> SlackChannel:
    return SlackChannel(webhook_url="https://hooks.slack.test/services/X/Y/Z")


class _FakeNotifiable:
    pass


class _FakeNotification:
    def to_slack(self, notifiable):
        return {"text": "hello"}


def test_send_passes_timeout_to_urlopen(channel: SlackChannel):
    """urlopen MUST receive a timeout > 0 to prevent indefinite hangs."""
    fake_response = MagicMock()
    fake_response.status = 200
    fake_response.__enter__ = MagicMock(return_value=fake_response)
    fake_response.__exit__ = MagicMock(return_value=False)

    with patch("urllib.request.urlopen", return_value=fake_response) as mock_urlopen:
        result = channel.send(_FakeNotifiable(), _FakeNotification())

    assert result is True
    mock_urlopen.assert_called_once()
    call_kwargs = mock_urlopen.call_args
    # timeout can be positional arg #2 or keyword
    if call_kwargs.kwargs and "timeout" in call_kwargs.kwargs:
        timeout_val = call_kwargs.kwargs["timeout"]
    else:
        # positional: urlopen(req, timeout)
        assert len(call_kwargs.args) >= 2, (
            "urlopen called without timeout argument — worker will hang "
            "indefinitely when Slack is unresponsive"
        )
        timeout_val = call_kwargs.args[1]

    assert isinstance(timeout_val, (int, float))
    assert timeout_val > 0, "timeout must be positive"
    assert timeout_val <= 30, "timeout should be reasonable (<=30s)"


def test_send_timeout_on_unreachable_does_not_hang(channel: SlackChannel):
    """When urlopen raises TimeoutError the channel returns False, not hang."""
    with patch(
        "urllib.request.urlopen",
        side_effect=TimeoutError("timed out"),
    ):
        result = channel.send(_FakeNotifiable(), _FakeNotification())

    assert result is False
