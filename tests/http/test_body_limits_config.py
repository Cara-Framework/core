"""Body preflight and streaming parsing share one exact configuration."""

from __future__ import annotations

import importlib

import pytest

from cara.exceptions import BadRequestException
from cara.http.BodyLimits import BodyLimits

limits_module = importlib.import_module("cara.http.BodyLimits")
middleware_module = importlib.import_module("cara.middleware.http.EnforceBodySizeLimit")


def _config(values: dict[str, object]):
    return lambda key, default=None: values.get(key, default)


def test_all_body_limits_are_read_from_the_server_vocabulary(monkeypatch) -> None:
    monkeypatch.setattr(
        limits_module,
        "config",
        _config(
            {
                "server.max_body_size": 1024,
                "server.max_file_size": 512,
                "server.max_files": 3,
            }
        ),
    )

    assert BodyLimits.configured() == BodyLimits(
        body_bytes=1024,
        file_bytes=512,
        files=3,
    )


@pytest.mark.parametrize("value", [True, False, 0, -1, 1.5, "1024"])
def test_ambiguous_body_limit_is_rejected(monkeypatch, value: object) -> None:
    monkeypatch.setattr(
        limits_module,
        "config",
        _config({"server.max_body_size": value}),
    )

    with pytest.raises(ValueError, match="server.max_body_size"):
        BodyLimits.configured()


class _Request:
    def __init__(self, value: object) -> None:
        self._value = value

    def header(self, _name: str) -> object:
        return self._value


@pytest.mark.parametrize("value", ["", " ", "-1", "abc", 12, True])
def test_malformed_declared_length_is_not_treated_as_chunked(value: object) -> None:
    with pytest.raises(BadRequestException, match="Content-Length"):
        middleware_module.EnforceBodySizeLimit._content_length(_Request(value))


def test_absent_declared_length_remains_streaming() -> None:
    assert middleware_module.EnforceBodySizeLimit._content_length(_Request(None)) is None
