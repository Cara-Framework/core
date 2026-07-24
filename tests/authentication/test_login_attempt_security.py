from __future__ import annotations

import importlib

import pytest

from cara.exceptions import AuthenticationConfigurationException


def tracker_module():
    return importlib.import_module("cara.authentication.LoginAttemptTracker")


def test_identifier_digest_is_normalized_keyed_and_stable(monkeypatch) -> None:
    module = tracker_module()
    monkeypatch.setattr(
        module,
        "config",
        lambda key, default=None: (
            "x" * 48 if key == "security.identifier_hmac_key" else default
        ),
    )

    first = module.LoginAttemptTracker.identifier_digest(" User@Example.com ")
    second = module.LoginAttemptTracker.identifier_digest("user@example.com")

    assert first == second
    assert "user@example.com" not in first
    assert len(first) == 64


def test_identifier_digest_rejects_weak_key(monkeypatch) -> None:
    module = tracker_module()
    monkeypatch.setattr(
        module,
        "config",
        lambda key, default=None: (
            "weak" if key == "security.identifier_hmac_key" else default
        ),
    )

    with pytest.raises(AuthenticationConfigurationException, match="32 bytes"):
        module.LoginAttemptTracker.identifier_digest("user@example.com")
