"""Canonical rate-limit configuration paths."""

from __future__ import annotations

from types import SimpleNamespace

import cara.facades as facades
from cara.middleware.http.ThrottleRequests import ThrottleRequests
from cara.rates.MemoryRateStore import resolve_fallback_mode


class _Config:
    values: dict[str, object] = {}

    @classmethod
    def get(cls, key: str, default=None):
        return cls.values.get(key, default)


def test_fallback_mode_uses_only_the_canonical_key(monkeypatch) -> None:
    monkeypatch.setattr(facades, "Config", _Config)

    _Config.values = {"rate.fallback_mode": "memory", "rate.fail_open": True}
    assert resolve_fallback_mode() == "memory"

    _Config.values = {"rate.fail_open": True}
    assert resolve_fallback_mode() == "closed"


def test_trusted_ips_use_only_the_lowercase_loaded_path(monkeypatch) -> None:
    monkeypatch.setattr(facades, "Config", _Config)
    middleware = ThrottleRequests.__new__(ThrottleRequests)
    request = SimpleNamespace(ip=lambda: "127.0.0.1")

    _Config.values = {"rate.trusted_ips": ["127.0.0.1"]}
    assert middleware._is_trusted_ip(request) is True

    _Config.values = {"rate.TRUSTED_IPS": ["127.0.0.1"]}
    assert middleware._is_trusted_ip(request) is False
