from __future__ import annotations

import importlib
from types import SimpleNamespace

import pytest

from cara.exceptions import (
    AuthenticationConfigurationException,
    ServiceUnavailableException,
)


def tracker_module():
    return importlib.import_module("cara.authentication.LoginAttemptTracker")


def test_identifier_digest_is_normalized_keyed_and_stable(monkeypatch) -> None:
    module = tracker_module()
    monkeypatch.setattr(
        module,
        "config",
        lambda key, default=None: "x" * 48 if key == "app.key" else default,
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
        lambda key, default=None: "weak" if key == "app.key" else default,
    )

    with pytest.raises(AuthenticationConfigurationException, match="32 bytes"):
        module.LoginAttemptTracker.identifier_digest("user@example.com")


def _configure_tracker(module, monkeypatch) -> None:
    monkeypatch.setattr(
        module,
        "config",
        lambda key, default=None: "x" * 48 if key == "app.key" else default,
    )


def test_lock_read_failure_denies_authentication(monkeypatch) -> None:
    module = tracker_module()
    _configure_tracker(module, monkeypatch)
    monkeypatch.setattr(
        module,
        "Cache",
        SimpleNamespace(get=lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError())),
    )

    with pytest.raises(ServiceUnavailableException) as exc_info:
        module.LoginAttemptTracker.assert_unlocked("user@example.com")

    assert exc_info.value.status_code == 503
    assert exc_info.value.retry_after == 5


def test_failure_counter_backend_error_is_not_treated_as_zero(monkeypatch) -> None:
    module = tracker_module()
    _configure_tracker(module, monkeypatch)
    monkeypatch.setattr(
        module,
        "Cache",
        SimpleNamespace(
            increment=lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError())
        ),
    )

    with pytest.raises(ServiceUnavailableException):
        module.LoginAttemptTracker.record_failure(
            "user@example.com",
            "203.0.113.10",
        )


def test_malformed_ip_security_state_denies_authentication(monkeypatch) -> None:
    module = tracker_module()
    _configure_tracker(module, monkeypatch)
    monkeypatch.setattr(
        module,
        "Cache",
        SimpleNamespace(get=lambda *_args, **_kwargs: '["not-a-digest"]'),
    )

    with pytest.raises(ServiceUnavailableException):
        module.LoginAttemptTracker._read_ip_set("user@example.com")


@pytest.mark.parametrize("value", [0, -1, True, "5"])
def test_invalid_login_security_threshold_is_rejected(monkeypatch, value) -> None:
    module = tracker_module()
    monkeypatch.setattr(module, "config", lambda *_args, **_kwargs: value)

    with pytest.raises(AuthenticationConfigurationException, match="positive integer"):
        module.LoginAttemptTracker._max_failures()
