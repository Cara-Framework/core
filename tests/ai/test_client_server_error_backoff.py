"""A 5xx or a timeout is retried after a jittered exponential backoff.

They were retried at once: a provider that is down was hit again within
milliseconds by every worker in step. The wait is bounded like a 429's
(``min(30 s, timeout)``) and never taken after the last attempt.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from cara.ai import AIClient


def _server_error(status: int = 503) -> MagicMock:
    response = MagicMock()
    response.status_code = status
    response.text = "unavailable"
    response.headers = {}
    answer = MagicMock()
    answer.raise_for_status.side_effect = requests.exceptions.HTTPError(response=response)
    return answer


def _ok() -> MagicMock:
    answer = MagicMock()
    answer.json.return_value = {
        "choices": [{"message": {"content": "hi"}, "finish_reason": "stop"}],
        "usage": {"prompt_tokens": 3, "completion_tokens": 5},
    }
    answer.raise_for_status.return_value = None
    return answer


def _client(jitter: float, **overrides) -> AIClient:
    options = {"provider": "openrouter", "model": "m", "api_key": "k", "max_retries": 3}
    options.update(overrides)
    client = AIClient(**options)
    client._jitter = lambda: jitter
    return client


def _waits(sleep: MagicMock) -> list[float]:
    return [call.args[0] for call in sleep.call_args_list]


@pytest.mark.parametrize("status", [500, 502, 503])
def test_server_errors_back_off_exponentially_and_not_after_the_last(status):
    client = _client(jitter=0.999999)
    with (
        patch(
            "cara.ai.AIClient.requests.post", return_value=_server_error(status)
        ) as post,
        patch("cara.ai.AIClient.time.sleep") as sleep,
        pytest.raises(Exception, match=f"AI HTTP {status}"),
    ):
        client.chat("hi")

    assert post.call_count == 4
    # 1, 2, 4 at full jitter; nothing after the fourth (last) attempt.
    assert _waits(sleep) == pytest.approx([1.0, 2.0, 4.0], rel=1e-5)


def test_jitter_spreads_the_wait_into_its_upper_half():
    client = _client(jitter=0.0, max_retries=2)
    with (
        patch("cara.ai.AIClient.requests.post", return_value=_server_error()),
        patch("cara.ai.AIClient.time.sleep") as sleep,
        pytest.raises(Exception),
    ):
        client.chat("hi")

    assert _waits(sleep) == [0.5, 1.0]


def test_timeouts_back_off_too():
    client = _client(jitter=0.5, max_retries=1)
    with (
        patch(
            "cara.ai.AIClient.requests.post",
            side_effect=[requests.exceptions.Timeout(), _ok()],
        ),
        patch("cara.ai.AIClient.time.sleep") as sleep,
    ):
        assert client.chat("hi").content == "hi"

    assert _waits(sleep) == [0.75]


def test_the_wait_is_bounded_by_one_attempt_timeout():
    client = _client(jitter=0.999999, max_retries=8, timeout=5)
    with (
        patch("cara.ai.AIClient.requests.post", return_value=_server_error()),
        patch("cara.ai.AIClient.time.sleep") as sleep,
        pytest.raises(Exception),
    ):
        client.chat("hi")

    waits = _waits(sleep)
    assert len(waits) == 8
    assert max(waits) <= 5.0
    assert waits[-1] == pytest.approx(5.0, rel=1e-5)


def test_no_wait_with_no_retry_left_and_none_before_the_fallback():
    client = _client(jitter=0.5, max_retries=0, fallback_model="backup")
    with (
        patch("cara.ai.AIClient.requests.post", return_value=_server_error()) as post,
        patch("cara.ai.AIClient.time.sleep") as sleep,
        pytest.raises(Exception),
    ):
        client.chat("hi")

    assert post.call_count == 2  # primary, then fallback
    sleep.assert_not_called()


def test_a_client_error_is_not_retried_or_waited_on():
    client = _client(jitter=0.5)
    with (
        patch("cara.ai.AIClient.requests.post", return_value=_server_error(400)) as post,
        patch("cara.ai.AIClient.time.sleep") as sleep,
        pytest.raises(Exception),
    ):
        client.chat("hi")

    assert post.call_count == 1
    sleep.assert_not_called()
