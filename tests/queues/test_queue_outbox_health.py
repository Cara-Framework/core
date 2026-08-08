"""Outbox stall detection — the 2026-07-20 silent-backlog incident.

1250 jobs sat ``pending`` in ``queue_job_delivery`` because ``queue:relay``
was not running. Nothing said a word. These tests pin the framework half
of what must never regress:

* the watchdog alarms on AGE, so ordinary bursts stay quiet;
* both due predicates come from the store, so the watchdog and the
  workers can never disagree about what "due" means;
* an ongoing stall keeps speaking, but not once a minute;
* the two halves (publication, terminal hooks) page and resolve
  independently;
* a broken cache pages rather than muting.

Gauge publication is the product seam and is pinned by each product's
own suite.
"""

from __future__ import annotations

import pytest

from cara.queues.delivery import QueueJobDeliveryStore, QueueOutboxHealth
from cara.queues.delivery import QueueOutboxHealth as _module_home

module = __import__(
    "cara.queues.delivery.QueueOutboxHealth", fromlist=["QueueOutboxHealth"]
)


class _FakeCache:
    """Minimal Cache double with real ``add`` set-if-absent semantics."""

    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.ttls: dict[str, int | None] = {}

    def add(self, key, value, ttl=None) -> bool:
        if key in self.store:
            return False
        self.store[key] = value
        self.ttls[key] = ttl
        return True

    def put(self, key, value, ttl=None) -> None:
        self.store[key] = value

    def get(self, key, default=None):
        return self.store.get(key, default)

    def forget(self, key) -> bool:
        return self.store.pop(key, None) is not None


class _RecordingSink:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def fire(self, **kwargs) -> bool:
        self.calls.append(kwargs)
        return True


@pytest.fixture
def wired(monkeypatch):
    cache = _FakeCache()
    sink = _RecordingSink()
    logs: list[tuple[str, tuple]] = []

    monkeypatch.setattr(module, "Cache", cache)
    monkeypatch.setattr(module, "AlertSink", sink)
    for level in ("error", "info", "warning"):
        monkeypatch.setattr(
            module.Log,
            level,
            lambda msg, *args, _level=level, **kw: logs.append((_level, args)),
        )
    return cache, sink, logs


def _snapshot(
    *,
    due=1200.0,
    age=3600.0,
    last_publish=3600.0,
    hook_due=0.0,
    hook_age=0.0,
) -> dict:
    return {
        "due_pending": due,
        "oldest_due_age": age,
        "last_publish_age": last_publish,
        "hook_due_pending": hook_due,
        "hook_oldest_due_age": hook_age,
    }


# ── the watchdog must read the same rows the workers claim ───────────


def test_hook_due_predicate_has_exactly_one_home() -> None:
    """Both the hooks claim scan and the watchdog format ONE template.

    A copy of this predicate is how the two halves start disagreeing —
    the watchdog then reports a backlog the hooks runner does not see, or
    (worse) stays quiet about one it does.
    """
    template = QueueJobDeliveryStore._HOOK_DUE_FILTER_TEMPLATE
    assert "{now}" in template, "the clock must stay the caller's to bind"
    assert "post_hooks_completed_at IS NULL" in template
    assert "post_hooks_quarantined_at IS NULL" in template
    assert template.format(now="NOW()").endswith("NOW())")
    assert template.format(now="%s").endswith("%s)")


def test_sample_reads_the_store_not_a_restated_query() -> None:
    seen = {}

    class _Store:
        def outbox_health_metrics(self):
            seen["called"] = True
            return _snapshot(due=3.0, age=4.0)

    snapshot = QueueOutboxHealth.sample(_Store())
    assert seen == {"called": True}
    assert snapshot["due_pending"] == 3.0


# ── judgement: age gates, count gates, config-driven ─────────────────


def test_a_large_fresh_burst_is_not_a_stall() -> None:
    assert QueueOutboxHealth.is_stalled(_snapshot(due=5000.0, age=2.0)) is False


def test_aged_backlog_is_a_stall() -> None:
    assert QueueOutboxHealth.is_stalled(_snapshot(due=1.0, age=3600.0)) is True


def test_count_gate_can_tolerate_a_chronic_tail(monkeypatch) -> None:
    monkeypatch.setattr(QueueOutboxHealth, "stall_min_pending", staticmethod(lambda: 5))
    assert QueueOutboxHealth.is_stalled(_snapshot(due=2.0, age=3600.0)) is False
    assert QueueOutboxHealth.is_stalled(_snapshot(due=9.0, age=3600.0)) is True


def test_a_large_fresh_hook_burst_is_not_a_stall() -> None:
    snapshot = _snapshot(due=0.0, age=0.0, hook_due=5000.0, hook_age=5.0)
    assert QueueOutboxHealth.is_hook_stalled(snapshot) is False


def test_aged_hook_backlog_is_a_stall() -> None:
    snapshot = _snapshot(due=0.0, age=0.0, hook_due=1.0, hook_age=7200.0)
    assert QueueOutboxHealth.is_hook_stalled(snapshot) is True


def test_hook_stall_budget_defaults_looser_than_publication(monkeypatch) -> None:
    """One ordinary 60s hooks retry cycle must not page."""
    monkeypatch.setattr(module, "config", lambda key, default=None: default)
    assert (
        QueueOutboxHealth.hook_stall_age_seconds()
        > QueueOutboxHealth.stall_age_seconds()
    )


def test_thresholds_come_from_config_not_constants(monkeypatch) -> None:
    monkeypatch.setattr(
        module,
        "config",
        lambda key, default=None: {"queue.outbox_stall_age_seconds": 30}.get(
            key, default
        ),
    )
    assert QueueOutboxHealth.stall_age_seconds() == 30
    assert QueueOutboxHealth.is_stalled(_snapshot(due=1.0, age=45.0)) is True


def test_thresholds_are_floored_at_one(monkeypatch) -> None:
    """A zero budget would page on every healthy tick."""
    monkeypatch.setattr(module, "config", lambda key, default=None: 0)
    assert QueueOutboxHealth.stall_age_seconds() >= 1
    assert QueueOutboxHealth.stall_min_pending() >= 1
    assert QueueOutboxHealth.stall_renotify_seconds() >= 1
    assert QueueOutboxHealth.hook_stall_age_seconds() >= 1
    assert QueueOutboxHealth.hook_stall_min_pending() >= 1


# ── alert lifecycle ──────────────────────────────────────────────────


def test_first_stall_pages_immediately(wired) -> None:
    _cache, sink, _logs = wired
    assert QueueOutboxHealth.announce(_snapshot(), True) == "fired"
    assert sink.calls[0]["severity"] == "critical"
    assert sink.calls[0]["dedup_key"] == QueueOutboxHealth.ALERT_DEDUP_KEY


def test_repeat_stall_inside_the_window_is_throttled(wired) -> None:
    _cache, sink, _logs = wired
    QueueOutboxHealth.announce(_snapshot(), True)
    assert QueueOutboxHealth.announce(_snapshot(), True) == "throttled"
    assert len(sink.calls) == 1


def test_an_ongoing_stall_speaks_again_after_the_window(wired) -> None:
    cache, sink, _logs = wired
    QueueOutboxHealth.announce(_snapshot(), True)
    # The notify key expiring is what lets a CONTINUING stall re-page.
    cache.store.pop(QueueOutboxHealth.NOTIFY_CACHE_KEY)
    assert QueueOutboxHealth.announce(_snapshot(), True) == "fired"
    assert len(sink.calls) == 2


def test_every_stall_tick_is_logged_even_when_throttled(wired) -> None:
    _cache, sink, logs = wired
    QueueOutboxHealth.announce(_snapshot(), True)
    QueueOutboxHealth.announce(_snapshot(), True)
    assert len([entry for entry in logs if entry[0] == "error"]) == 2
    assert len(sink.calls) == 1, "the log is the forensic trail, not the pager"


def test_recovery_resolves_the_open_incident(wired) -> None:
    _cache, sink, _logs = wired
    QueueOutboxHealth.announce(_snapshot(), True)
    assert QueueOutboxHealth.announce(_snapshot(due=0.0, age=0.0), False) == "resolved"
    assert sink.calls[-1]["severity"] == "resolved"


def test_healthy_ticks_never_page(wired) -> None:
    _cache, sink, _logs = wired
    assert QueueOutboxHealth.announce(_snapshot(due=0.0, age=0.0), False) == "quiet"
    assert sink.calls == []


def test_recovery_after_resolve_stays_quiet(wired) -> None:
    _cache, sink, _logs = wired
    QueueOutboxHealth.announce(_snapshot(), True)
    QueueOutboxHealth.announce(_snapshot(due=0.0, age=0.0), False)
    assert QueueOutboxHealth.announce(_snapshot(due=0.0, age=0.0), False) == "quiet"
    assert len(sink.calls) == 2


def test_a_broken_cache_pages_rather_than_muting(monkeypatch, wired) -> None:
    _cache, sink, _logs = wired

    class _DeadCache:
        def add(self, *_a, **_kw):
            raise ConnectionError("redis down")

        def put(self, *_a, **_kw):
            raise ConnectionError("redis down")

        def get(self, *_a, **_kw):
            raise ConnectionError("redis down")

    monkeypatch.setattr(module, "Cache", _DeadCache())
    assert QueueOutboxHealth.announce(_snapshot(), True) == "fired"
    assert QueueOutboxHealth.announce(_snapshot(), True) == "fired"
    assert len(sink.calls) == 2, "rather page twice than not at all"


def test_never_published_ledger_says_so_in_the_body(wired) -> None:
    _cache, sink, _logs = wired
    QueueOutboxHealth.announce(_snapshot(last_publish=-1.0), True)
    assert "Nothing has ever been published" in sink.calls[0]["body"]


# ── the two halves are independent ───────────────────────────────────


def test_hooks_stall_pages_with_its_own_dedup_and_body(wired) -> None:
    _cache, sink, _logs = wired
    snapshot = _snapshot(due=0.0, age=0.0, hook_due=40.0, hook_age=7200.0)
    assert QueueOutboxHealth.announce_hooks(snapshot, True) == "fired"
    assert sink.calls[0]["dedup_key"] == QueueOutboxHealth.HOOK_ALERT_DEDUP_KEY
    assert "queue:hooks" in sink.calls[0]["body"]


def test_an_open_publication_incident_does_not_throttle_the_first_hooks_page(
    wired,
) -> None:
    _cache, sink, _logs = wired
    snapshot = _snapshot(hook_due=40.0, hook_age=7200.0)
    QueueOutboxHealth.announce(snapshot, True)
    assert QueueOutboxHealth.announce_hooks(snapshot, True) == "fired"
    assert len(sink.calls) == 2


def test_hooks_recovery_does_not_resolve_an_open_publication_incident(wired) -> None:
    _cache, sink, _logs = wired
    stalled_both = _snapshot(hook_due=40.0, hook_age=7200.0)
    QueueOutboxHealth.announce(stalled_both, True)
    QueueOutboxHealth.announce_hooks(stalled_both, True)

    healthy_hooks = _snapshot(hook_due=0.0, hook_age=0.0)
    assert QueueOutboxHealth.announce_hooks(healthy_hooks, False) == "resolved"
    # The publication incident is still open — a second publication tick
    # must still be throttled, not treated as a first page.
    assert QueueOutboxHealth.announce(healthy_hooks, True) == "throttled"


# ── orchestration + the product seam ─────────────────────────────────


def test_observe_returns_both_outcomes(monkeypatch) -> None:
    class _Store:
        def outbox_health_metrics(self):
            return _snapshot(hook_due=40.0, hook_age=7200.0)

    monkeypatch.setattr(module, "Cache", _FakeCache())
    monkeypatch.setattr(module, "AlertSink", _RecordingSink())
    monkeypatch.setattr(module.Log, "error", lambda *a, **kw: None)

    result = QueueOutboxHealth.observe(_Store())
    assert result["stalled"] is True
    assert result["hook_stalled"] is True
    assert result["outcome"] == "fired"
    assert result["hook_outcome"] == "fired"


def test_publish_metrics_is_an_overridable_no_op_seam(monkeypatch) -> None:
    """Gauge names are product-owned; the base class must not invent any."""
    seen: list[tuple] = []

    class _ProductProbe(QueueOutboxHealth):
        @classmethod
        def publish_metrics(cls, snapshot, stalled, hook_stalled):
            seen.append((snapshot["due_pending"], stalled, hook_stalled))
            return True

    class _Store:
        def outbox_health_metrics(self):
            return _snapshot(due=7.0)

    monkeypatch.setattr(module, "Cache", _FakeCache())
    monkeypatch.setattr(module, "AlertSink", _RecordingSink())
    monkeypatch.setattr(module.Log, "error", lambda *a, **kw: None)

    assert QueueOutboxHealth.publish_metrics(_snapshot(), True, True) is True
    _ProductProbe.observe(_Store())
    assert seen == [(7.0, True, False)]


def test_the_watchdog_lives_beside_the_ledger_it_observes() -> None:
    """It must stay importable from the delivery package, not an app tree."""
    assert _module_home is QueueOutboxHealth
    assert QueueOutboxHealth.__module__.startswith("cara.queues.delivery")
