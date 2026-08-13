"""Configuration access is owned by provider registration."""

from __future__ import annotations

import pytest

from cara.configuration import Configuration, config
from cara.eloquent.models import Model
from cara.eloquent.utils import DateManager
from cara.exceptions import InvalidConfigurationSetupException
from cara.security import is_trusted_proxy


def test_config_refuses_access_before_provider_registration(monkeypatch) -> None:
    monkeypatch.setattr(Configuration, "_instance", None)

    with pytest.raises(InvalidConfigurationSetupException, match="before"):
        config("app.name", "invented")


def test_explicit_configuration_instance_remains_readable(monkeypatch) -> None:
    monkeypatch.setattr(Configuration, "_instance", None)
    configuration = Configuration.empty()
    configuration.set("app.name", "Synkronus")

    assert config("app.name") == "Synkronus"


def test_constructor_refuses_to_invent_an_application(monkeypatch) -> None:
    monkeypatch.setattr(Configuration, "_instance", None)

    with pytest.raises(InvalidConfigurationSetupException, match="application"):
        Configuration(None)


def test_temporal_conversion_does_not_invent_utc_before_boot(monkeypatch) -> None:
    monkeypatch.setattr(Configuration, "_instance", None)

    with pytest.raises(InvalidConfigurationSetupException):
        DateManager.to_user_timezone("2026-08-13T12:00:00Z")
    with pytest.raises(InvalidConfigurationSetupException):
        object.__new__(Model)._get_user_timezone()


def test_preboot_exception_policy_trusts_no_external_proxy(monkeypatch) -> None:
    monkeypatch.setattr(Configuration, "_instance", None)

    assert is_trusted_proxy("203.0.113.10") is False
