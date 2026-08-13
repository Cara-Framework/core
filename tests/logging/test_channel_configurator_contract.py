"""Logging startup rejects incomplete topology instead of inventing one."""

from __future__ import annotations

from importlib import import_module
from unittest.mock import Mock

import pytest

module = import_module("cara.logging.ChannelConfigurator")


def _config(values: dict[str, object]):
    return lambda key: values.get(key)


def test_unknown_default_stack_is_configuration_error(monkeypatch) -> None:
    monkeypatch.setattr(
        module,
        "config",
        _config(
            {
                "logging.default": "missing",
                "logging.stacks": {"daily": ["console"]},
                "logging.channels": {"console": {"ENABLED": True}},
                "logging.slack": {},
            }
        ),
    )

    with pytest.raises(ValueError, match="not configured"):
        module.ChannelConfigurator(Mock()).configure()


def test_stack_cannot_name_an_unknown_channel(monkeypatch) -> None:
    monkeypatch.setattr(
        module,
        "config",
        _config(
            {
                "logging.default": "daily",
                "logging.stacks": {"daily": ["missing"]},
                "logging.channels": {"console": {"ENABLED": True}},
                "logging.slack": {},
            }
        ),
    )

    with pytest.raises(ValueError, match="configured channel"):
        module.ChannelConfigurator(Mock()).configure()


def test_enabled_slack_requires_a_webhook(monkeypatch) -> None:
    monkeypatch.setattr(
        module,
        "config",
        _config(
            {
                "logging.default": "production",
                "logging.stacks": {"production": ["slack"]},
                "logging.channels": {"slack": {"ENABLED": True}},
                "logging.slack": {},
            }
        ),
    )

    with pytest.raises(ValueError, match="WEBHOOK_URL"):
        module.ChannelConfigurator(Mock()).configure()


def test_disabled_slack_does_not_require_a_webhook(monkeypatch) -> None:
    logger = Mock()
    monkeypatch.setattr(
        module,
        "config",
        _config(
            {
                "logging.default": "production",
                "logging.stacks": {"production": ["console", "slack"]},
                "logging.channels": {
                    "console": {"ENABLED": True},
                    "slack": {"ENABLED": False},
                },
                "logging.slack": {},
            }
        ),
    )

    module.ChannelConfigurator(logger).configure()

    assert logger.add.call_count == 1
