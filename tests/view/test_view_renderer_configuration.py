"""Template disclosure policy is exact and safe during boot failure."""

from __future__ import annotations

import importlib

import pytest

from cara.exceptions import InvalidConfigurationSetupException

module = importlib.import_module("cara.view.ViewRenderer")


def test_preboot_renderer_does_not_disclose_template_failures(monkeypatch) -> None:
    def unavailable(_key: str, _default=None):
        raise InvalidConfigurationSetupException("not booted")

    monkeypatch.setattr(module, "config", unavailable)

    assert module.ViewRenderer(engine=object()).debug is False


@pytest.mark.parametrize("value", ["false", "true", 0, 1, None])
def test_ambiguous_debug_configuration_is_rejected(monkeypatch, value: object) -> None:
    monkeypatch.setattr(module, "config", lambda _key, _default=None: value)

    with pytest.raises(TypeError, match="app.debug"):
        module.ViewRenderer(engine=object())
