"""Rate-limit accounting is global, atomic, and unavailable rather than local."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from cara.exceptions import (
    RateLimitConfigurationException,
    ServiceUnavailableException,
)
from cara.rates import RateLimitAuthority as authority


@pytest.fixture(autouse=True)
def reset_health() -> None:
    authority._reset_for_tests()


def _install_cache(monkeypatch, *, count: object = 1, ttl: object = 60) -> None:
    cache = SimpleNamespace(
        increment=lambda *_args: count,
        ttl=lambda *_args: ttl,
    )
    log = SimpleNamespace(warning=lambda *_args, **_kwargs: None)
    monkeypatch.setattr(authority.facades, "Cache", cache)
    monkeypatch.setattr(authority.facades, "Log", log)


def test_authoritative_counter_returns_exact_budget(monkeypatch) -> None:
    _install_cache(monkeypatch, count=3, ttl=17)

    assert authority.attempt_rate_limit("rate:key", 60, 5) == (True, 2, 17)


@pytest.mark.parametrize("count", [None, True, 0, "1"])
def test_invalid_counter_state_denies(monkeypatch, count: object) -> None:
    _install_cache(monkeypatch, count=count)

    with pytest.raises(ServiceUnavailableException):
        authority.attempt_rate_limit("rate:key", 60, 5)


@pytest.mark.parametrize("ttl", [None, True, -1, "60"])
def test_missing_or_invalid_expiry_denies(monkeypatch, ttl: object) -> None:
    _install_cache(monkeypatch, ttl=ttl)

    with pytest.raises(ServiceUnavailableException):
        authority.attempt_rate_limit("rate:key", 60, 5)


def test_backend_failure_denies_without_a_process_local_counter(monkeypatch) -> None:
    cache = SimpleNamespace(
        increment=lambda *_args: (_ for _ in ()).throw(ConnectionError("down")),
        ttl=lambda *_args: 60,
    )
    log = SimpleNamespace(warning=lambda *_args, **_kwargs: None)
    monkeypatch.setattr(authority.facades, "Cache", cache)
    monkeypatch.setattr(authority.facades, "Log", log)

    with pytest.raises(ServiceUnavailableException):
        authority.attempt_rate_limit("rate:key", 60, 5)


@pytest.mark.parametrize(
    ("key", "window", "limit"),
    [
        ("", 60, 5),
        ("rate:key", True, 5),
        ("rate:key", 0, 5),
        ("rate:key", 60, "5"),
    ],
)
def test_invalid_authority_input_is_configuration_error(
    monkeypatch, key: object, window: object, limit: object
) -> None:
    _install_cache(monkeypatch)

    with pytest.raises(RateLimitConfigurationException):
        authority.attempt_rate_limit(key, window, limit)  # type: ignore[arg-type]
