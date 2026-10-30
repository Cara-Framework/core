"""A heartbeat lease lives exactly as long as the run that holds it.

Live, 2026-10-30: a worker restart mid-pull left a 3900 s idempotency lease,
a timeout-sized concurrency slot and a ``processing`` delivery row behind,
and the channel's order lane sat still for over an hour. The heartbeat keeps
a short lease alive from the running process instead, so these pin: it
renews on cadence while the run lives, stops for good once the lease is not
the run's anymore (it never re-takes one), rides out a transient renewal
fault, renews outside the run's transaction context, and hands every live
lease back when the worker is forced to exit.
"""

from __future__ import annotations

import contextvars
import sys
import threading
import time
from types import SimpleNamespace

import pytest

from cara.queues.LeaseHeartbeat import LeaseHeartbeat

lease_module = sys.modules["cara.queues.LeaseHeartbeat"]

_RUN_SCOPE: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "lease_heartbeat_test_run_scope", default=None
)


@pytest.fixture(autouse=True)
def _no_leftover_heartbeats():
    yield
    for heartbeat in LeaseHeartbeat.live():
        heartbeat.stop()


def _wait_until(predicate, timeout: float = 2.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.005)
    return predicate()


def test_renews_on_cadence_until_the_run_stops_it():
    renewals: list[float] = []
    heartbeat = LeaseHeartbeat(
        "lease:a",
        lease_seconds=0.06,
        renew=lambda: renewals.append(time.monotonic()) or True,
    ).start()

    assert heartbeat.interval_seconds == pytest.approx(0.02)
    assert _wait_until(lambda: len(renewals) >= 3)
    heartbeat.stop()
    settled = len(renewals)
    time.sleep(0.08)

    assert len(renewals) <= settled + 1, "renewal continued after stop()"
    assert heartbeat not in LeaseHeartbeat.live()
    assert heartbeat.lost is False


def test_a_lease_that_is_no_longer_ours_stops_the_heartbeat_for_good():
    calls: list[int] = []

    def _renew() -> bool:
        calls.append(1)
        return False  # the compare-and-set missed: another run holds it

    heartbeat = LeaseHeartbeat("lease:b", lease_seconds=0.03, renew=_renew).start()

    assert _wait_until(lambda: heartbeat.lost)
    time.sleep(0.06)
    assert calls == [1], "a lost lease must never be renewed (or re-taken) again"
    assert heartbeat not in LeaseHeartbeat.live()


def test_a_transient_renewal_fault_keeps_the_cadence():
    outcomes = iter([ConnectionError("redis blip"), True, True])
    renewed = threading.Event()

    def _renew() -> bool:
        outcome = next(outcomes, True)
        if isinstance(outcome, Exception):
            raise outcome
        renewed.set()
        return outcome

    heartbeat = LeaseHeartbeat("lease:c", lease_seconds=0.03, renew=_renew).start()

    assert renewed.wait(2.0)
    assert heartbeat.lost is False
    heartbeat.stop()


class _InheritingThread(threading.Thread):
    """A thread that inherits its starter's context, as free-threaded builds
    (``sys.flags.thread_inherit_context``) do by default."""

    def __init__(self, *args, **kwargs):
        kwargs.setdefault("context", contextvars.copy_context())
        super().__init__(*args, **kwargs)


def test_renewal_never_runs_inside_the_runs_context(monkeypatch):
    """The ORM's open-transaction registry is a ContextVar; a renewal that
    inherited the run's context would write inside the run's transaction."""
    monkeypatch.setattr(
        lease_module,
        "threading",
        SimpleNamespace(
            Thread=_InheritingThread,
            Event=threading.Event,
            Lock=threading.Lock,
        ),
    )
    seen: list[str | None] = []
    token = _RUN_SCOPE.set("inside-the-run")
    try:
        heartbeat = LeaseHeartbeat(
            "lease:d",
            lease_seconds=0.03,
            renew=lambda: seen.append(_RUN_SCOPE.get()) or True,
        ).start()

        assert _wait_until(lambda: bool(seen))
        heartbeat.stop()
    finally:
        _RUN_SCOPE.reset(token)
    assert seen[0] is None


def test_release_all_hands_back_every_live_lease_before_a_forced_exit():
    released: list[str] = []
    live = LeaseHeartbeat(
        "lease:live",
        lease_seconds=60,
        renew=lambda: True,
        release=lambda: released.append("live"),
    ).start()
    finished = LeaseHeartbeat(
        "lease:finished",
        lease_seconds=60,
        renew=lambda: True,
        release=lambda: released.append("finished"),
    ).start()
    finished.stop()  # its run ended and released normally

    count = LeaseHeartbeat.release_all(timeout_seconds=2.0)

    assert count == 1
    assert released == ["live"]
    assert live.stopped is True
    assert LeaseHeartbeat.live() == []


def test_release_all_is_bounded_and_survives_a_failing_release():
    gate = threading.Event()

    def _hangs() -> None:
        gate.wait(5.0)

    def _fails() -> None:
        raise ConnectionError("database gone")

    LeaseHeartbeat(
        "lease:fails", lease_seconds=60, renew=lambda: True, release=_fails
    ).start()
    LeaseHeartbeat(
        "lease:hangs", lease_seconds=60, renew=lambda: True, release=_hangs
    ).start()

    started = time.monotonic()
    LeaseHeartbeat.release_all(timeout_seconds=0.1)
    gate.set()

    assert time.monotonic() - started < 1.0, "an unreachable backend held the exit"
    assert LeaseHeartbeat.live() == []


@pytest.mark.parametrize("lease", [0, -1, True])
def test_a_lease_needs_a_positive_length(lease):
    with pytest.raises(ValueError):
        LeaseHeartbeat("lease:e", lease_seconds=lease, renew=lambda: True)
