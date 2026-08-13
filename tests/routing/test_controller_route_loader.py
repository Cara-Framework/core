from __future__ import annotations

import pytest

from cara.routing.loaders.ControllerRouteLoader import ControllerRouteLoader


class _Application:
    def __init__(self, location: object) -> None:
        self.location = location

    def make(self, key: str) -> object:
        assert key == "controllers.location"
        return self.location


def test_controller_discovery_rejects_a_missing_configured_module() -> None:
    with pytest.raises(ModuleNotFoundError):
        ControllerRouteLoader(_Application("not_a_real_controller_package")).load()


@pytest.mark.parametrize("location", [None, ""])
def test_controller_discovery_requires_an_explicit_module(location: object) -> None:
    with pytest.raises(RuntimeError, match="controllers.location"):
        ControllerRouteLoader(_Application(location)).load()
