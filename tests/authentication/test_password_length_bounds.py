"""Password length bounds — one reader for every password-setting form."""

from __future__ import annotations

import pytest

from cara.authentication import (
    MAX_PASSWORD_LENGTH,
    MIN_PASSWORD_LENGTH,
    password_length_bounds,
)
from cara.configuration import Configuration
from cara.exceptions import AuthenticationConfigurationException


@pytest.fixture
def auth_config(monkeypatch: pytest.MonkeyPatch):
    Configuration.empty()
    store = Configuration._instance._config
    for key in ("password_min_length", "password_max_length"):
        monkeypatch.setitem(store, f"auth.{key}", None)

    def _set(**values: object) -> None:
        for name, value in values.items():
            monkeypatch.setitem(store, f"auth.{name}", value)

    return _set


def test_password_length_defaults(auth_config):
    assert (
        password_length_bounds()
        == (MIN_PASSWORD_LENGTH, MAX_PASSWORD_LENGTH)
        == (15, 128)
    )


def test_password_length_configured_values_are_read_at_call_time(auth_config):
    auth_config(password_min_length=9)
    assert password_length_bounds() == (9, 128)
    auth_config(password_max_length=64)
    assert password_length_bounds() == (9, 64)


@pytest.mark.parametrize("bad", [0, -1, "15", True, 1.5])
def test_password_length_malformed_value_raises(auth_config, bad):
    auth_config(password_min_length=bad)
    with pytest.raises(AuthenticationConfigurationException):
        password_length_bounds()


def test_password_length_inverted_pair_raises(auth_config):
    auth_config(password_min_length=200, password_max_length=128)
    with pytest.raises(AuthenticationConfigurationException):
        password_length_bounds()
