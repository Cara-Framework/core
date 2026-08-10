"""Cara ships no OpenRouter model name.

OpenRouter routes across hundreds of models from many vendors, so pinning one
is a deployment decision — it belongs in each deployable's ``config/ai.py``,
never in the framework. Cara used to bake in
``mistralai/mistral-small-3.1-24b-instruct``, a value that matched no product's
actual configuration; anything that reached the client without config would
quietly bill a vendor nobody chose.

The replacement is not another default. It is a loud failure at first use,
naming the config key. Construction stays cheap and total, because
``AIServiceProvider.register`` builds an ``AIClient()`` eagerly — a raise in
``__init__`` would turn a missing AI setting into a dead application boot.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from cara.ai.Client import AIClient
from cara.ai.exceptions import AIConfigurationError, AIException


def _response(content="hi"):
    resp = MagicMock()
    resp.json.return_value = {
        "choices": [{"message": {"content": content}, "finish_reason": "stop"}],
        "usage": {"prompt_tokens": 1, "completion_tokens": 1},
    }
    resp.raise_for_status.return_value = None
    return resp


def test_openrouter_carries_no_framework_model_default():
    client = AIClient(provider="openrouter", api_key="k")

    assert client.model == ""
    assert "mistral" not in str(client.model)


def test_construction_never_raises_so_boot_survives():
    # AIServiceProvider.register() does exactly this at container-register
    # time. It must not be able to kill the application.
    client = AIClient(provider="openrouter", api_key="k")

    assert client.get_config()["model"] == ""


def test_chat_without_a_model_fails_loudly_naming_the_key():
    client = AIClient(provider="openrouter", api_key="k", max_retries=0)

    with (
        patch("cara.ai.Client.requests.post") as post,
        pytest.raises(AIConfigurationError) as excinfo,
    ):
        client.chat("hi")

    message = str(excinfo.value)
    assert "ai.openrouter_model" in message
    assert "openrouter" in message
    # Nothing was sent: the failure happens before any transport call.
    post.assert_not_called()


def test_configuration_error_is_an_ai_exception():
    assert issubclass(AIConfigurationError, AIException)


def test_per_call_model_still_works_without_configuration():
    client = AIClient(provider="openrouter", api_key="k", max_retries=0)

    with patch("cara.ai.Client.requests.post", return_value=_response()) as post:
        response = client.chat("hi", model="vendor/some-model")

    assert response.model == "vendor/some-model"
    assert post.call_args.kwargs["json"]["model"] == "vendor/some-model"


def test_json_fallback_does_not_swallow_the_configuration_error():
    # ``json(fallback=...)`` absorbs flaky responses on purpose. A missing
    # model is not flakiness — swallowing it would hide the misconfiguration
    # behind a permanently-returned fallback value.
    client = AIClient(provider="openrouter", api_key="k", max_retries=0)

    with pytest.raises(AIConfigurationError):
        client.json("hi", fallback={"safe": True})


def test_single_vendor_providers_keep_their_canonical_model():
    # Ollama and the generic OpenAI-compatible endpoint are single-vendor:
    # the model name is part of choosing that provider, not a routing choice
    # across vendors, so their defaults are not deployment snapshots.
    assert AIClient(provider="ollama").model == "mistral"
    assert AIClient(provider="openai").model == "gpt-4o-mini"
