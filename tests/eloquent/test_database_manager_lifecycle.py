"""DatabaseManager lifecycle belongs to the application container."""

from __future__ import annotations

import importlib

import pytest

from cara.container import Container
from cara.eloquent import DatabaseManager
from cara.exceptions import ConfigurationException

_provider_module = importlib.import_module("cara.eloquent.EloquentProvider")
EloquentProvider = _provider_module.EloquentProvider


def _drivers(name: str) -> dict[str, dict[str, str]]:
    return {name: {"driver": "sqlite", "database": ":memory:"}}


def test_provider_binds_one_configured_manager_per_application(monkeypatch) -> None:
    active = {
        "database.default": "first",
        "database.drivers": _drivers("first"),
    }
    monkeypatch.setattr(_provider_module, "config", lambda key: active[key])

    first_application = Container()
    EloquentProvider(first_application).register()
    first = first_application.make("DB")

    active.update(
        {
            "database.default": "second",
            "database.drivers": _drivers("second"),
        }
    )
    second_application = Container()
    EloquentProvider(second_application).register()
    second = second_application.make("DB")

    assert first is first_application.make(DatabaseManager)
    assert second is second_application.make(DatabaseManager)
    assert first is not second
    assert first.get_default_connection() == "first"
    assert second.get_default_connection() == "second"
    assert first.get_connection_details() == {
        "default": "first",
        **_drivers("first"),
    }
    assert second.get_connection_details() == {
        "default": "second",
        **_drivers("second"),
    }


def test_manager_requires_configuration_and_has_no_global_fallback() -> None:
    with pytest.raises(TypeError):
        DatabaseManager()
    assert "get_instance" not in DatabaseManager.__dict__
    assert "_auto_configure" not in DatabaseManager.__dict__
    assert "set_database_config" not in DatabaseManager.__dict__


@pytest.mark.parametrize(
    ("default", "drivers", "message"),
    [
        (None, _drivers("app"), "database.default"),
        ("app", None, "database.drivers must be a mapping"),
        ("missing", _drivers("app"), "is not present"),
        ("app", {"app": {}}, "driver must be configured"),
        ("app", {"app": {"driver": "sqlite"}, "audit": None}, "audit must be a mapping"),
    ],
)
def test_incomplete_configuration_is_rejected_during_composition(
    default, drivers, message
) -> None:
    with pytest.raises(ConfigurationException, match=message):
        DatabaseManager(default, drivers)


def test_manager_owns_an_isolated_snapshot_of_connection_configuration() -> None:
    drivers = _drivers("app")
    manager = DatabaseManager("app", drivers)

    drivers["app"]["driver"] = "postgres"

    assert manager.get_connection_info("app")["driver"] == "sqlite"


def test_resolver_injects_its_manager_into_query_builders() -> None:
    manager = DatabaseManager("app", _drivers("app"))

    builder = manager.query()

    assert builder._db_manager is manager
    assert builder.connection == "app"
