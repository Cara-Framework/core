"""One shared stamp for "the AI provider is refusing / down"."""

from __future__ import annotations

import pytest

import cara.ai.ProviderHealth as health
from cara.ai import (
    AIConfigurationError,
    provider_failure_kind,
    provider_unhealthy,
    record_provider_failure,
    record_provider_success,
)


class _Cache:
    def __init__(self) -> None:
        self.store: dict = {}
        self.ttls: dict = {}

    def put(self, key, value, ttl=None):
        self.store[key] = value
        self.ttls[key] = ttl

    def get(self, key, default=None):
        return self.store.get(key, default)

    def forget(self, key):
        self.store.pop(key, None)


@pytest.fixture
def cache(monkeypatch):
    fake = _Cache()
    monkeypatch.setattr(health, "Cache", fake)
    return fake


@pytest.mark.parametrize(
    ("status", "kind"),
    [(401, "auth"), (402, "auth"), (403, "auth"), (500, "outage"), (503, "outage")],
)
def test_provider_side_statuses_are_classified(status, kind) -> None:
    assert provider_failure_kind(status=status) == kind


@pytest.mark.parametrize("status", [400, 404, 408, 429])
def test_request_side_statuses_are_not_a_health_signal(status, cache) -> None:
    assert provider_failure_kind(status=status) is None
    assert record_provider_failure("openrouter", status=status) is None
    assert cache.store == {}


def test_timeout_and_missing_configuration() -> None:
    class Timeout(Exception):
        pass

    assert provider_failure_kind(error=Timeout()) == "outage"
    assert provider_failure_kind(error=AIConfigurationError("no key")) == "auth"
    assert provider_failure_kind(error=ValueError("bad json")) is None


def test_refused_key_is_held_longer_and_cleared_by_success(cache) -> None:
    assert record_provider_failure("OpenRouter", status=401) == "auth"
    assert provider_unhealthy("openrouter") == "auth"
    assert provider_unhealthy("ollama") is None
    (key,) = cache.ttls
    assert cache.ttls[key] == health.AUTH_HOLD_SECONDS
    record_provider_success("openrouter")
    assert provider_unhealthy("openrouter") is None


def test_outage_hold(cache) -> None:
    record_provider_failure("openrouter", status=502)
    assert list(cache.ttls.values()) == [health.OUTAGE_HOLD_SECONDS]


def test_cache_failure_leaves_the_door_open(monkeypatch) -> None:
    class _Broken:
        def get(self, *a):
            raise ConnectionError

        def put(self, *a):
            raise ConnectionError

    monkeypatch.setattr(health, "Cache", _Broken())
    assert provider_unhealthy("openrouter") is None
    assert record_provider_failure("openrouter", status=401) is None
