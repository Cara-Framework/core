"""Failure injection, cycle 2: the outbox watchdog page when the pager itself
is down for a moment.

An outbox stall and a paging outage are correlated (a network partition takes
the relay's broker AND PagerDuty/Slack egress at the same time). ``AlertSink.fire``
reports that with ``False`` (nothing acknowledged), but the watchdog ignores the
answer:

* stall: the renotify gate (``Cache.add``) is taken BEFORE the page is sent, so a
  page nobody received silences the next ``stall_renotify_seconds`` (15 min) of
  ticks — the operator learns of a relay outage a quarter hour late;
* recovery: the incident is dropped from memory and cache BEFORE the resolve is
  sent, so a resolve nobody received is never retried — the PagerDuty incident
  the stall opened stays triggered forever ("alerts fire and clear" fails).
"""

from __future__ import annotations

import pytest

from cara.queues.delivery import QueueOutboxHealth
from cara.testing.fakes import CacheFake

module = __import__(
    "cara.queues.delivery.QueueOutboxHealth", fromlist=["QueueOutboxHealth"]
)


class _FlakySink:
    """Acknowledges only while ``up`` is True, like a pager behind a partition."""

    def __init__(self) -> None:
        self.up = True
        self.calls: list[dict] = []

    def fire(self, **kwargs) -> bool:
        self.calls.append({**kwargs, "acked": self.up})
        return self.up


@pytest.fixture
def wired(monkeypatch):
    cache = CacheFake()
    sink = _FlakySink()
    monkeypatch.setattr(module, "Cache", cache)
    monkeypatch.setattr(module, "AlertSink", sink)
    monkeypatch.setattr(QueueOutboxHealth, "_unresolved_incidents", set())
    monkeypatch.setattr(QueueOutboxHealth, "_resolve_attempted_at", {})
    for level in ("error", "info", "warning"):
        monkeypatch.setattr(module.Log, level, lambda *a, **k: None)
    return cache, sink


_STALLED = {
    "due_pending": 1200.0,
    "oldest_due_age": 3600.0,
    "last_publish_age": 3600.0,
    "hook_due_pending": 0.0,
    "hook_oldest_due_age": 0.0,
}
_DRAINED = {**_STALLED, "due_pending": 0.0, "oldest_due_age": 0.0}


def test_a_stall_page_the_pager_did_not_take_is_sent_again_next_tick(wired) -> None:
    _cache, sink = wired
    sink.up = False
    QueueOutboxHealth.announce(_STALLED, stalled=True)  # partition: page lost
    sink.up = True
    QueueOutboxHealth.announce(_STALLED, stalled=True)  # next minute, pager back

    acked = [c for c in sink.calls if c["severity"] == "critical" and c["acked"]]
    assert acked, (
        "the stall page was lost and the next tick was throttled: the operator "
        f"hears nothing for the renotify window; calls={sink.calls}"
    )


def test_a_resolve_the_pager_did_not_take_is_sent_again_next_tick(wired) -> None:
    _cache, sink = wired
    QueueOutboxHealth.announce(_STALLED, stalled=True)  # page delivered
    sink.up = False
    QueueOutboxHealth.announce(_DRAINED, stalled=False)  # resolve lost
    sink.up = True
    QueueOutboxHealth.announce(_DRAINED, stalled=False)  # next tick

    acked = [c for c in sink.calls if c["severity"] == "resolved" and c["acked"]]
    assert acked, (
        "the resolve was lost and never retried: the incident stays open on the "
        f"pager after the outbox drained; calls={sink.calls}"
    )


def test_a_resolve_no_pager_ever_takes_stops_retrying_after_its_window(
    wired, monkeypatch
) -> None:
    """With no pager reachable (or none configured, as in local dev) the
    resolve is retried for its window, then the watchdog closes its record
    instead of logging a recovery every tick for the life of the scheduler."""
    _cache, sink = wired
    clock = [1_000_000.0]
    monkeypatch.setattr(module.time, "time", lambda: clock[0])
    QueueOutboxHealth.announce(_STALLED, stalled=True)
    sink.up = False

    assert QueueOutboxHealth.announce(_DRAINED, stalled=False) == "resolve_pending"
    clock[0] += 60
    assert QueueOutboxHealth.announce(_DRAINED, stalled=False) == "resolve_pending"

    clock[0] += QueueOutboxHealth.stall_renotify_seconds() * (
        QueueOutboxHealth.RESOLVE_RETRY_WINDOW_MULTIPLE
    )
    assert QueueOutboxHealth.announce(_DRAINED, stalled=False) == "resolved"
    assert QueueOutboxHealth.announce(_DRAINED, stalled=False) == "quiet"
