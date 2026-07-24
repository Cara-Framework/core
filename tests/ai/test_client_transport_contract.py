"""Tests for the AIClient transport contract.

Pins the framework surface app policy layers build on:

* ``response_format`` passes through to the OpenAI-compatible payload and
  maps to ``format: "json"`` on Ollama; ``json_mode=True`` is sugar for
  ``{"type": "json_object"}`` and an explicit ``response_format`` wins,
* ``finish_reason`` rides the usage dict out of both transports and lands
  on ``AIResponse.finish_reason``,
* the retry seams (``_models_to_try`` / ``_on_attempt_success`` /
  ``_on_attempt_error``) fire so subclasses can attach metrics, breakers
  and fallback chains without re-implementing the loop.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from cara.ai import AIClient
from cara.ai.AIProvider import AIProvider


def _openai_response(content="hi", finish_reason="stop"):
    resp = MagicMock()
    resp.json.return_value = {
        "choices": [{"message": {"content": content}, "finish_reason": finish_reason}],
        "usage": {"prompt_tokens": 3, "completion_tokens": 5},
    }
    resp.raise_for_status.return_value = None
    return resp


def _ollama_response(content="hi", done_reason="stop"):
    resp = MagicMock()
    resp.json.return_value = {
        "message": {"content": content},
        "prompt_eval_count": 3,
        "eval_count": 5,
        "done_reason": done_reason,
    }
    resp.raise_for_status.return_value = None
    return resp


def _client(provider="openrouter"):
    return AIClient(provider=provider, model="m", api_key="k", max_retries=0)


class TestResponseFormatPassthrough:
    def test_openai_payload_carries_response_format(self):
        client = _client()
        with patch(
            "cara.ai.Client.requests.post", return_value=_openai_response()
        ) as post:
            client.chat("hi", response_format={"type": "json_object"})
        payload = post.call_args.kwargs["json"]
        assert payload["response_format"] == {"type": "json_object"}

    def test_json_mode_sugar_sets_json_object(self):
        client = _client()
        with patch(
            "cara.ai.Client.requests.post", return_value=_openai_response()
        ) as post:
            client.chat("hi", json_mode=True)
        assert post.call_args.kwargs["json"]["response_format"] == {"type": "json_object"}

    def test_explicit_response_format_beats_json_mode(self):
        client = _client()
        explicit = {"type": "json_schema", "json_schema": {"name": "x"}}
        with patch(
            "cara.ai.Client.requests.post", return_value=_openai_response()
        ) as post:
            client.chat("hi", response_format=explicit, json_mode=True)
        assert post.call_args.kwargs["json"]["response_format"] == explicit

    def test_no_format_by_default(self):
        client = _client()
        with patch(
            "cara.ai.Client.requests.post", return_value=_openai_response()
        ) as post:
            client.chat("hi")
        assert "response_format" not in post.call_args.kwargs["json"]

    def test_ollama_maps_to_format_json(self):
        client = _client(provider="ollama")
        with patch(
            "cara.ai.Client.requests.post", return_value=_ollama_response()
        ) as post:
            client.chat("hi", json_mode=True)
        assert post.call_args.kwargs["json"]["format"] == "json"


class TestFinishReason:
    def test_openai_finish_reason_lands_on_response(self):
        client = _client()
        with patch(
            "cara.ai.Client.requests.post",
            return_value=_openai_response(finish_reason="length"),
        ):
            resp = client.chat("hi")
        assert resp.finish_reason == "length"
        assert resp.tokens_in == 3 and resp.tokens_out == 5

    def test_ollama_done_reason_lands_on_response(self):
        client = _client(provider="ollama")
        with patch(
            "cara.ai.Client.requests.post",
            return_value=_ollama_response(done_reason="length"),
        ):
            resp = client.chat("hi")
        assert resp.finish_reason == "length"


class TestRetrySeams:
    def test_models_to_try_default_chain(self):
        client = AIClient(
            provider="openrouter", model="a", api_key="k", fallback_model="b"
        )
        assert client._models_to_try("a") == ["a", "b"]
        assert client._models_to_try("b") == ["b"]

    def test_success_hook_fires_with_response(self):
        client = _client()
        seen = []
        client._on_attempt_success = lambda model, resp: seen.append(
            (model, resp.content)
        )
        with patch("cara.ai.Client.requests.post", return_value=_openai_response("ok")):
            client.chat("hi")
        assert seen == [("m", "ok")]

    def test_error_hook_fires_with_status(self):
        client = _client()
        seen = []
        client._on_attempt_error = lambda model, err, status, retry: seen.append(status)

        err_resp = MagicMock()
        err_resp.status_code = 503
        err_resp.text = "unavailable"
        err_resp.headers = {}
        http_error = requests.exceptions.HTTPError(response=err_resp)
        bad = MagicMock()
        bad.raise_for_status.side_effect = http_error

        with (
            patch("cara.ai.Client.requests.post", return_value=bad),
            pytest.raises(Exception, match="AI HTTP 503"),
        ):
            client.chat("hi")
        assert seen == [503]

    def test_backoff_seconds_default_is_capped_exponential(self):
        client = _client()
        assert client._backoff_seconds(0) == 1.0
        assert client._backoff_seconds(3) == 8.0
        assert client._backoff_seconds(10) == 30.0


class TestProviderEnumUnchanged:
    def test_providers(self):
        assert {p.value for p in AIProvider} >= {"openrouter", "ollama"}
