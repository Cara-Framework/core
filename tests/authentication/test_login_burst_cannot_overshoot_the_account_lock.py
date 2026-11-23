"""A burst of sign-ins cannot overshoot the account lock.

Live defect: ``LoginAttemptTracker.assert_unlocked`` was check-then-act.
Attempts between the check and their recorded outcome were counted nowhere,
so a burst from many sources passed the check before the first failure
landed: a repro admitted 800 checks against a 50-failure account lock.

The check now reserves an in-flight slot against the account limit; a
failure turns it into a counted failure, a success releases it, and a slot
whose attempt never reports back frees itself with its window.
"""

from __future__ import annotations

import importlib
from types import SimpleNamespace

import pytest

from cara.authentication import LoginLocked
from cara.exceptions import ServiceUnavailableException
from cara.http import current_request
from cara.testing import CacheFake

_OWNER = "owner@example.com"


def _module():
    return importlib.import_module("cara.authentication.LoginAttemptTracker")


class _Clock:
    def __init__(self) -> None:
        self.now = 1_000_000.0

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def env(monkeypatch):
    module = _module()
    settings = {"app.key": "x" * 48}
    monkeypatch.setattr(
        module, "config", lambda key, default=None: settings.get(key, default)
    )
    cache = CacheFake()
    monkeypatch.setattr(module, "Cache", cache)
    monkeypatch.setattr(module, "Log", SimpleNamespace(warning=lambda *_a, **_k: None))
    clock = _Clock()
    monkeypatch.setattr(module, "_clock", clock)
    token = module._HELD_OUTSIDE_REQUEST.set(None)
    yield SimpleNamespace(
        tracker=module.LoginAttemptTracker,
        settings=settings,
        cache=cache,
        clock=clock,
    )
    module._HELD_OUTSIDE_REQUEST.reset(token)


class _Request:
    """Stands in for one HTTP request: what the check and the outcome share."""


def _in(request, call, *args):
    token = current_request.set(request)
    try:
        return call(*args)
    finally:
        current_request.reset(token)


def _admitted(tracker, request, ip: str) -> bool:
    try:
        _in(request, tracker.assert_unlocked, _OWNER, ip)
    except LoginLocked:
        return False
    return True


def test_a_burst_from_many_sources_is_admitted_only_up_to_the_account_limit(
    env,
) -> None:
    requests = [_Request() for _ in range(800)]
    admitted = [
        request
        for index, request in enumerate(requests)
        if _admitted(env.tracker, request, f"10.0.{index // 250}.{index % 250}")
    ]

    assert len(admitted) == 50
    for index, request in enumerate(admitted):
        _in(request, env.tracker.record_failure, _OWNER, f"10.9.0.{index}")
    with pytest.raises(LoginLocked):
        env.tracker.assert_unlocked(_OWNER, "198.51.100.1")
    assert env.cache.all()[env.tracker._failure_key(_OWNER)] == 50


def test_a_success_releases_its_slot(env) -> None:
    env.settings["security.login_account_lock_failures"] = 3
    first, second, third, fourth = (_Request() for _ in range(4))
    for request in (first, second, third):
        assert _admitted(env.tracker, request, "203.0.113.1")
    assert not _admitted(env.tracker, fourth, "203.0.113.2")

    _in(first, env.tracker.record_success, _OWNER)

    assert _admitted(env.tracker, fourth, "203.0.113.2")


def test_a_failure_turns_its_slot_into_a_counted_failure(env) -> None:
    env.settings["security.login_account_lock_failures"] = 3
    first, second, third, fourth = (_Request() for _ in range(4))
    for request in (first, second, third):
        assert _admitted(env.tracker, request, "203.0.113.1")

    _in(first, env.tracker.record_failure, _OWNER, "203.0.113.1")

    # One failure and two attempts in flight: the limit is still spent.
    assert not _admitted(env.tracker, fourth, "203.0.113.2")
    _in(second, env.tracker.record_success, _OWNER)
    assert _admitted(env.tracker, fourth, "203.0.113.2")


def test_a_slot_whose_attempt_never_reports_back_expires_with_its_window(
    env,
) -> None:
    env.settings["security.login_account_lock_failures"] = 2
    crashed, waiting = _Request(), _Request()
    assert _admitted(env.tracker, crashed, "203.0.113.1")
    assert _admitted(env.tracker, _Request(), "203.0.113.1")
    assert not _admitted(env.tracker, waiting, "203.0.113.1")

    env.clock.now += 2 * env.tracker._reservation_seconds()

    assert _admitted(env.tracker, waiting, "203.0.113.1")


def test_a_slot_stays_counted_across_one_window_boundary(env) -> None:
    env.settings["security.login_account_lock_failures"] = 1
    assert _admitted(env.tracker, _Request(), "203.0.113.1")

    env.clock.now += env.tracker._reservation_seconds()

    assert not _admitted(env.tracker, _Request(), "203.0.113.1")


def test_a_refusal_for_full_slots_writes_no_lock_and_hints_the_window(env) -> None:
    env.settings["security.login_account_lock_failures"] = 1
    assert _admitted(env.tracker, _Request(), "203.0.113.1")

    with pytest.raises(LoginLocked) as refused:
        _in(_Request(), env.tracker.assert_unlocked, _OWNER, "203.0.113.2")

    assert refused.value.retry_after_seconds == env.tracker._reservation_seconds()
    assert env.cache.get(env.tracker._lock_key(_OWNER)) is None


def test_an_owner_with_a_few_parallel_tabs_is_neither_refused_nor_locked(
    env,
) -> None:
    tabs = [_Request() for _ in range(5)]
    assert all(_admitted(env.tracker, tab, "198.51.100.7") for tab in tabs)

    for tab in tabs:
        _in(tab, env.tracker.record_success, _OWNER)

    env.tracker.assert_unlocked(_OWNER, "198.51.100.7")
    assert env.cache.get(env.tracker._lock_key(_OWNER)) is None


def test_a_check_without_a_source_reserves_nothing(env) -> None:
    env.settings["security.login_account_lock_failures"] = 1
    for _ in range(10):
        env.tracker.assert_unlocked(_OWNER, None)

    assert _admitted(env.tracker, _Request(), "203.0.113.1")


def test_an_outcome_releases_only_a_slot_its_own_request_took(env) -> None:
    env.settings["security.login_account_lock_failures"] = 1
    holder, bystander = _Request(), _Request()
    assert _admitted(env.tracker, holder, "203.0.113.1")

    _in(bystander, env.tracker.record_success, _OWNER)

    assert not _admitted(env.tracker, _Request(), "203.0.113.2")


def test_a_cache_outage_during_the_reservation_fails_closed(env, monkeypatch) -> None:
    def broken_increment(*_args, **_kwargs):
        raise OSError("cache down")

    monkeypatch.setattr(env.cache, "increment", broken_increment)

    with pytest.raises(ServiceUnavailableException) as refused:
        env.tracker.assert_unlocked(_OWNER, "203.0.113.1")

    assert refused.value.status_code == 503
