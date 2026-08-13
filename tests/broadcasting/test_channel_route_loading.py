"""Optional channel routes may be absent, but an existing policy must boot."""

from __future__ import annotations

import importlib

import pytest

from cara.broadcasting import BroadcastingProvider
from cara.exceptions import BroadcastingConfigurationException

provider_module = importlib.import_module("cara.broadcasting.BroadcastingProvider")


def test_absent_channel_routes_are_optional(monkeypatch) -> None:
    def missing(_name: str):
        raise ModuleNotFoundError("No module named 'routes'", name="routes")

    monkeypatch.setattr(provider_module.importlib, "import_module", missing)

    BroadcastingProvider._load_channel_routes()


def test_missing_dependency_inside_channel_routes_blocks_boot(monkeypatch) -> None:
    def broken(_name: str):
        raise ModuleNotFoundError("No module named 'policy_sdk'", name="policy_sdk")

    monkeypatch.setattr(provider_module.importlib, "import_module", broken)

    with pytest.raises(BroadcastingConfigurationException, match="policy_sdk"):
        BroadcastingProvider._load_channel_routes()


def test_syntax_failure_inside_channel_routes_blocks_boot(monkeypatch) -> None:
    def broken(_name: str):
        raise SyntaxError("invalid policy")

    monkeypatch.setattr(provider_module.importlib, "import_module", broken)

    with pytest.raises(BroadcastingConfigurationException, match="invalid policy"):
        BroadcastingProvider._load_channel_routes()
