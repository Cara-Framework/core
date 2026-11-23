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
            # Shaped like ``DatabaseManager``: ``query()`` hands out ONE
            # builder object here so a provider that captured it at boot is
            # caught, while ``table()`` builds a fresh one per call.
            self.shared_builder = SimpleNamespace(table=lambda _name: "shared")
            return SimpleNamespace(
                query=lambda: self.shared_builder,
                table=lambda _name: object(),
            )
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


def test_database_channel_gets_a_fresh_builder_per_operation(monkeypatch) -> None:
    """One process-wide QueryBuilder bled predicates between notifiables.

    ``make("DB").query()`` is a single builder whose ``table()``/``where()``
    mutate and return it: a call that raised left its ``notifiable_id``
    filter for the next user, and concurrent sends lost rows. The channel
    must hold the manager, whose ``table()`` builds a new builder each time.
    """
    monkeypatch.setattr(provider_module, "config", _config)
    application = _Application()

    NotificationProvider(application).register()

    _, manager = application.bound
    database = manager._channels["database"].database_manager
    assert database is not application.shared_builder
    first, second = database.table("notifications"), database.table("notifications")
    assert first is not second
    assert "shared" not in (first, second)


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
