"""Response compression configuration is exact and provider-owned."""

from __future__ import annotations

import importlib

import pytest

module = importlib.import_module("cara.middleware.http.CompressResponses")


def _config(values: dict[str, object]):
    return lambda key, default=None: values.get(key, default)


@pytest.mark.parametrize(
    ("key", "value", "error"),
    [
        ("compression.compression.enabled", "false", TypeError),
        ("compression.compression.min_size", True, ValueError),
        ("compression.compression.min_size", -1, ValueError),
        ("compression.compression.level", "6", ValueError),
        ("compression.compression.level", 10, ValueError),
        ("compression.compression.content_types", [], TypeError),
        ("compression.compression.content_types", [""], ValueError),
        ("compression.compression.content_types", [1], ValueError),
    ],
)
def test_invalid_configuration_is_not_replaced_with_defaults(
    monkeypatch, key: str, value: object, error: type[Exception]
) -> None:
    monkeypatch.setattr(module, "config", _config({key: value}))

    with pytest.raises(error):
        module.CompressResponses._load_config()


def test_configuration_is_normalized_without_coercion(monkeypatch) -> None:
    monkeypatch.setattr(
        module,
        "config",
        _config(
            {
                "compression.compression.enabled": False,
                "compression.compression.min_size": 2048,
                "compression.compression.level": 4,
                "compression.compression.content_types": [" Application/JSON "],
            }
        ),
    )

    assert module.CompressResponses._load_config() == (
        False,
        2048,
        4,
        ("application/json",),
    )
