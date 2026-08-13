"""Notification channel topology is complete at provider boot."""

from __future__ import annotations

import importlib
from types import SimpleNamespace

import pytest

from cara.notifications import NotificationProvider

provider_module = importlib.import_module("cara.notifications.NotificationProvider")


class _Application:
    def __init__(self, *, fail: str | None = None) -> None:
        self.fail = fail
        self.bound = None

    def make(self, name: str):
        if name == self.fail:
            raise RuntimeError(f"{name} unavailable")
        if name == "DB":
            query = SimpleNamespace(table=lambda _name: object())
            return SimpleNamespace(query=lambda: query)
        if name == "mail":
            return SimpleNamespace(to=lambda _recipient: object())
        raise KeyError(name)

    def bind(self, name: str, value: object) -> None:
        self.bound = (name, value)


def _config(key: str, default=None):
    return default


def test_provider_registers_every_required_channel(monkeypatch) -> None:
    monkeypatch.setattr(provider_module, "config", _config)
    application = _Application()

    NotificationProvider(application).register()

    name, manager = application.bound
    assert name == "notification"
    assert set(manager._channels) == {"database", "log", "mail"}


def test_provider_treats_blank_optional_link_settings_as_unconfigured(
    monkeypatch,
) -> None:
    def blank_config(key: str, default=None):
        if key == "app.unsubscribe_secret":
            return ""
        return default

    monkeypatch.setattr(provider_module, "config", blank_config)
    application = _Application()

    NotificationProvider(application).register()

    _, manager = application.bound
    assert manager._channels["mail"].link_settings == {}


@pytest.mark.parametrize("binding", ["mail", "DB"])
def test_required_channel_failure_blocks_provider_boot(monkeypatch, binding: str) -> None:
    monkeypatch.setattr(provider_module, "config", _config)
    application = _Application(fail=binding)

    with pytest.raises(RuntimeError, match="unavailable"):
        NotificationProvider(application).register()

    assert application.bound is None
