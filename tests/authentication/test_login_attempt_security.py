from __future__ import annotations

import importlib
from types import SimpleNamespace

import pytest

from cara.authentication import LoginLocked
from cara.exceptions import (
    AuthenticationConfigurationException,
    ServiceUnavailableException,
)
from cara.testing import CacheFake


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
        module.LoginAttemptTracker.assert_unlocked("user@example.com", "203.0.113.10")

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


_OWNER = "owner@example.com"
_ATTACKER_IP = "203.0.113.10"
_OWNER_IP = "198.51.100.20"


@pytest.fixture
def tracker(monkeypatch):
    module = tracker_module()
    _configure_tracker(module, monkeypatch)
    monkeypatch.setattr(module, "Cache", CacheFake())
    monkeypatch.setattr(module, "Log", SimpleNamespace(warning=lambda *_a, **_k: None))
    return module.LoginAttemptTracker


def _fail(tracker, ip: str, times: int = 5) -> None:
    for _ in range(times):
        tracker.record_failure(_OWNER, ip)


def test_a_source_past_the_limit_locks_itself_and_not_the_owner(tracker) -> None:
    _fail(tracker, _ATTACKER_IP)

    with pytest.raises(LoginLocked):
        tracker.assert_unlocked(_OWNER, _ATTACKER_IP)
    tracker.assert_unlocked(_OWNER, _OWNER_IP)


def test_a_few_sources_cannot_lock_the_owner_out(tracker) -> None:
    # Two addresses used to be enough for the account-wide lock.
    for index in range(3):
        _fail(tracker, f"203.0.113.{index}")

    tracker.assert_unlocked(_OWNER, _OWNER_IP)


def test_a_wide_pool_of_sources_still_locks_the_account(tracker) -> None:
    for index in range(10):
        _fail(tracker, f"203.0.113.{index}")

    with pytest.raises(LoginLocked):
        tracker.assert_unlocked(_OWNER, _OWNER_IP)


def test_a_password_reset_opens_a_locked_source_after_the_window_passed(
    tracker,
) -> None:
    _fail(tracker, _ATTACKER_IP)
    Cache = tracker_module().Cache
    Cache.forget(tracker._ip_set_key(_OWNER))  # the failure window ended first

    tracker.clear_lockout(_OWNER)

    tracker.assert_unlocked(_OWNER, _ATTACKER_IP)


def test_the_owners_sign_in_elsewhere_keeps_the_other_source_locked(tracker) -> None:
    _fail(tracker, _ATTACKER_IP)

    tracker.record_success(_OWNER)

    with pytest.raises(LoginLocked):
        tracker.assert_unlocked(_OWNER, _ATTACKER_IP)
