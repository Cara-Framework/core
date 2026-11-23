"""A 429's ``Retry-After`` is honoured, and a null ``content`` is an answer.

* ``if e.response`` guarded the header read; a ``requests.Response`` is falsy
  for every 4xx/5xx, so ``Retry-After`` was never read. The loop also slept
  after the LAST attempt, where nothing follows.
* ``"content": null`` (refusal / content filter / tool call) crashed
  ``.strip()`` and burned every retry plus the fallback model.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from cara.ai import AIClient


def _throttled(retry_after: str | None) -> MagicMock:
    """A transport answer whose ``raise_for_status`` raises a real 429."""
    response = requests.Response()
    response.status_code = 429
    if retry_after is not None:
        response.headers["Retry-After"] = retry_after
    assert not response  # the trap: a 429 Response is falsy
    answer = MagicMock()
    answer.raise_for_status.side_effect = requests.exceptions.HTTPError(response=response)
    return answer


def _ok(content: object = "hi", finish_reason: str = "stop") -> MagicMock:
    answer = MagicMock()
    answer.json.return_value = {
        "choices": [{"message": {"content": content}, "finish_reason": finish_reason}],
        "usage": {"prompt_tokens": 3, "completion_tokens": 5},
    }
    answer.raise_for_status.return_value = None
    return answer


def _client(**overrides) -> AIClient:
    options = {"provider": "openrouter", "model": "m", "api_key": "k", "max_retries": 1}
    options.update(overrides)
    return AIClient(**options)


def test_retry_after_is_honoured_and_only_between_attempts():
    client = _client(max_retries=1)
    with (
        patch(
            "cara.ai.AIClient.requests.post",
            side_effect=[_throttled("7"), _throttled("7")],
        ) as post,
        patch("cara.ai.AIClient.time.sleep") as sleep,
        pytest.raises(Exception, match="AI HTTP 429"),
    ):
        client.chat("hi")

    assert post.call_count == 2
    # One wait, of the length the provider named — none after the last attempt.
    assert [call.args[0] for call in sleep.call_args_list] == [7.0]


def test_no_wait_when_no_retry_remains():
    client = _client(max_retries=0)
    with (
        patch("cara.ai.AIClient.requests.post", return_value=_throttled("7")),
        patch("cara.ai.AIClient.time.sleep") as sleep,
        pytest.raises(Exception, match="AI HTTP 429"),
    ):
        client.chat("hi")

    sleep.assert_not_called()


def test_retry_after_is_capped_at_one_attempt_timeout():
    client = _client(max_retries=1, timeout=10)
    with (
        patch(
            "cara.ai.AIClient.requests.post",
            side_effect=[_throttled("86400"), _ok()],
        ),
        patch("cara.ai.AIClient.time.sleep") as sleep,
    ):
        assert client.chat("hi").content == "hi"

    assert [call.args[0] for call in sleep.call_args_list] == [10.0]


def test_a_429_without_retry_after_uses_the_backoff_seam():
    client = _client(max_retries=1)
    client._backoff_seconds = lambda retry, *, status=None: 3.5
    with (
        patch(
            "cara.ai.AIClient.requests.post",
            side_effect=[_throttled(None), _ok()],
        ),
        patch("cara.ai.AIClient.time.sleep") as sleep,
    ):
        client.chat("hi")

    assert [call.args[0] for call in sleep.call_args_list] == [3.5]


def test_null_content_is_an_empty_answer_not_a_crash():
    client = _client(max_retries=2, fallback_model="backup")
    with patch(
        "cara.ai.AIClient.requests.post",
        return_value=_ok(content=None, finish_reason="content_filter"),
    ) as post:
        response = client.chat("hi")

    assert response.content == ""
    assert response.finish_reason == "content_filter"
    assert post.call_count == 1  # no retries, no fallback model spent


def test_ollama_null_content_is_an_empty_answer():
    client = _client(provider="ollama")
    answer = MagicMock()
    answer.json.return_value = {"message": {"content": None}, "done_reason": "stop"}
    answer.raise_for_status.return_value = None
    with patch("cara.ai.AIClient.requests.post", return_value=answer) as post:
        response = client.chat("hi")

    assert response.content == ""
    assert post.call_count == 1
