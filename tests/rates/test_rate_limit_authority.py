"""Rate-limit accounting is global, atomic, and unavailable rather than local."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from cara.exceptions import (
    RateLimitConfigurationException,
    ServiceUnavailableException,
)
from cara.rates import Limit, RateLimitDecision
from cara.rates import RateLimitAuthority as authority


@pytest.fixture(autouse=True)
def reset_health() -> None:
    authority._reset_for_tests()


def _install_cell(monkeypatch, state: object, calls: list | None = None) -> None:
    def throttle(key, **kwargs):
        if calls is not None:
            calls.append((key, kwargs))
        return state

    monkeypatch.setattr(authority.facades, "Cache", SimpleNamespace(throttle=throttle))
    monkeypatch.setattr(
        authority.facades, "Log", SimpleNamespace(warning=lambda *_a, **_k: None)
    )


def test_the_bucket_is_spent_under_the_rate_namespace_at_the_limit_s_pace(
    monkeypatch,
) -> None:
    calls: list = []
    _install_cell(monkeypatch, (True, 4, 0, 1500), calls)

    authority.attempt_rate_limit(Limit.per_minute(120, burst=6).by("user:7"))

    assert calls == [
        ("rate:user:7", {"emission_interval_ms": 500, "burst": 6, "cost": 1})
    ]


def test_an_allowed_spend_reports_whole_seconds_rounded_up(monkeypatch) -> None:
    _install_cell(monkeypatch, (True, 4, 0, 1500))

    decision = authority.attempt_rate_limit(Limit.per_minute(120, burst=6).by("user:7"))

    assert decision == RateLimitDecision(
        allowed=True,
        limit=120,
        period_seconds=60,
        remaining=4,
        retry_after=0,
        reset_after=2,
    )


def test_a_refusal_says_when_the_next_request_fits(monkeypatch) -> None:
    _install_cell(monkeypatch, (False, 0, 201, 3000))

    decision = authority.attempt_rate_limit(Limit.per_minute(120, burst=6).by("user:7"))

    assert decision.allowed is False
    # 201 ms rounds UP: a client told "0" and refused again was lied to.
    assert decision.retry_after == 1
    assert decision.reset_after == 3


@pytest.mark.parametrize(
    "state",
    [
        None,
        (True, 1, 0),
        ("yes", 1, 0, 0),
        (True, -1, 0, 0),
        (True, 1, True, 0),
        (True, 1, 0, "0"),
    ],
    ids=["none", "short", "non-bool-decision", "negative", "bool-measure", "text"],
)
def test_an_answer_that_is_not_one_denies(monkeypatch, state: object) -> None:
    _install_cell(monkeypatch, state)

    with pytest.raises(ServiceUnavailableException):
        authority.attempt_rate_limit(Limit.per_minute(5).by("ip:198.51.100.1"))


def test_backend_failure_denies_without_a_process_local_counter(monkeypatch) -> None:
    def down(*_args, **_kwargs):
        raise ConnectionError("down")

    monkeypatch.setattr(authority.facades, "Cache", SimpleNamespace(throttle=down))
    monkeypatch.setattr(
        authority.facades, "Log", SimpleNamespace(warning=lambda *_a, **_k: None)
    )

    with pytest.raises(ServiceUnavailableException):
        authority.attempt_rate_limit(Limit.per_minute(5).by("ip:198.51.100.1"))


def test_every_backend_fault_lands_on_the_counter_alerting_reads(monkeypatch) -> None:
    """The warning is logged once per outage; the counter must see every refusal."""
    faults: list[int] = []

    def down(*_args, **_kwargs):
        raise ConnectionError("down")

    monkeypatch.setattr(authority.facades, "Cache", SimpleNamespace(throttle=down))
    monkeypatch.setattr(
        authority.facades, "Log", SimpleNamespace(warning=lambda *_a, **_k: None)
    )
    monkeypatch.setattr(
        authority, "counter", lambda *_a, **_k: SimpleNamespace(inc=lambda: faults.append(1))
    )

    for _ in range(3):
        with pytest.raises(ServiceUnavailableException):
            authority.attempt_rate_limit(Limit.per_minute(5).by("ip:198.51.100.1"))

    assert len(faults) == 3


@pytest.mark.parametrize(
    ("limit", "cost"),
    [
        ("rate:key", 1),
        (Limit.none(), 1),
        (Limit.per_minute(5), 1),
        (Limit.per_minute(5).by("ip:198.51.100.1"), 0),
        (Limit.per_minute(5).by("ip:198.51.100.1"), True),
        (Limit.per_minute(5, burst=2).by("ip:198.51.100.1"), 3),
    ],
    ids=["not-a-limit", "unlimited", "unkeyed", "zero-cost", "bool-cost", "over-burst"],
)
def test_a_spend_that_can_never_be_valid_is_a_configuration_error(
    monkeypatch, limit: object, cost: object
) -> None:
    _install_cell(monkeypatch, (True, 0, 0, 0))

    with pytest.raises(RateLimitConfigurationException):
        authority.attempt_rate_limit(limit, cost=cost)  # type: ignore[arg-type]
