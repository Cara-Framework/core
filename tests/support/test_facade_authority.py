"""Facades never construct a second service authority outside the container."""

from __future__ import annotations

import builtins
from types import SimpleNamespace

import pytest

from cara.exceptions import CaraException
from cara.facades import DB, Log, Validation


def test_data_facades_require_bootstrap(monkeypatch) -> None:
    monkeypatch.delattr(builtins, "app", raising=False)

    with pytest.raises(CaraException, match="not bootstrapped"):
        DB.table("users")
    with pytest.raises(CaraException, match="not bootstrapped"):
        Validation.make({}, {})


def test_logging_remains_available_during_boot_failure(monkeypatch) -> None:
    monkeypatch.delattr(builtins, "app", raising=False)

    Log.warning("boot failed", category="boot")


def test_missing_container_binding_is_not_relabelled_as_attribute_error(
    monkeypatch,
) -> None:
    failure = CaraException("cache binding missing")
    monkeypatch.setattr(
        builtins,
        "app",
        lambda: SimpleNamespace(make=lambda _key: (_ for _ in ()).throw(failure)),
        raising=False,
    )

    with pytest.raises(CaraException, match="cache binding missing"):
        DB.table("users")
