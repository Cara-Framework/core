"""Canonical rate-limit configuration paths."""

from __future__ import annotations

from importlib import import_module
from types import SimpleNamespace

from cara.middleware.http.ThrottleRequests import ThrottleRequests

throttle_requests = import_module("cara.middleware.http.ThrottleRequests")


class _Config:
    values: dict[str, object] = {}

    @classmethod
    def get(cls, key: str, default=None):
        return cls.values.get(key, default)


def test_trusted_ips_use_only_the_lowercase_loaded_path(monkeypatch) -> None:
    monkeypatch.setattr(throttle_requests.facades, "Config", _Config)
    middleware = ThrottleRequests.__new__(ThrottleRequests)
    request = SimpleNamespace(ip=lambda: "127.0.0.1")

    _Config.values = {"rate.trusted_ips": ["127.0.0.1"]}
    assert middleware._is_trusted_ip(request) is True

    _Config.values = {"rate.TRUSTED_IPS": ["127.0.0.1"]}
    assert middleware._is_trusted_ip(request) is False
