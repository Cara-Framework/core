"""JWT command output must remain framework-generic."""

from __future__ import annotations

from unittest.mock import MagicMock

from cara.commands.core.JWTGenerateCommand import JWTGenerateCommand


def test_usage_examples_use_a_generic_protected_resource_url() -> None:
    command = JWTGenerateCommand(application=None)
    command.info = MagicMock()

    command._show_usage_examples("token-value-that-is-long-enough")

    output = "\n".join(call.args[0] for call in command.info.call_args_list)
    protected_url = "https://api.example.com/protected-resource"
    assert output.count(protected_url) == 3
    assert "localhost" not in output
    assert "user/resolve" not in output
