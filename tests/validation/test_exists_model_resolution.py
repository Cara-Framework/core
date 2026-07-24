"""Tenant-safe model resolution contract for the ``exists`` rule."""

from __future__ import annotations

import sys
from types import ModuleType

import pytest

from cara.exceptions import ConfigurationException
from cara.support import ModuleManager
from cara.validation.rules.ExistsRule import ExistsRule


class _Query:
    def __init__(self, result=object(), error: Exception | None = None) -> None:
        self.result = result
        self.error = error
        self.conditions: list[tuple[str, object]] = []

    def where(self, column: str, value):
        self.conditions.append((column, value))
        return self

    def first(self):
        if self.error is not None:
            raise self.error
        return self.result


def _install_models(
    monkeypatch: pytest.MonkeyPatch,
    *models: type,
    module_name: str = "tests.fake_models",
) -> None:
    module = ModuleType(module_name)
    for model in models:
        setattr(module, model.__name__, model)
    monkeypatch.setitem(sys.modules, module_name, module)
    monkeypatch.setattr(ModuleManager, "models_module", lambda: module_name)


def test_exists_resolves_exact_table_from_configured_model_barrel(monkeypatch) -> None:
    query = _Query()

    class User:
        __table__ = "users"

        @classmethod
        def where(cls, column: str, value):
            query.conditions.append((column, value))
            return query

    _install_models(monkeypatch, User)

    assert (
        ExistsRule().validate(
            "user_id",
            7,
            {"exists": "users,id,status,active"},
        )
        is True
    )
    assert query.conditions == [("id", 7), ("status", "active")]


def test_exists_missing_model_is_configuration_error(monkeypatch) -> None:
    _install_models(monkeypatch)

    with pytest.raises(ConfigurationException, match="found 0"):
        ExistsRule().validate("user_id", 7, {"exists": "users,id"})


def test_exists_does_not_turn_database_failure_into_invalid_input(monkeypatch) -> None:
    query = _Query(error=RuntimeError("database unavailable"))

    class User:
        __table__ = "users"

        @classmethod
        def where(cls, _column: str, _value):
            return query

    _install_models(monkeypatch, User)

    with pytest.raises(RuntimeError, match="database unavailable"):
        ExistsRule().validate("user_id", 7, {"exists": "users,id"})
