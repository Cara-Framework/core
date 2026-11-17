"""The scheduler heartbeat: tick, stale verdict off the scheduler, one page."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

import cara.scheduling.SchedulerHeartbeat as heartbeat


class _Cache:
    def __init__(self) -> None:
        self.store: dict[str, object] = {}

    def put(self, key, value, ttl=None):
        self.store[key] = value

    def get(self, key, default=None):
        return self.store.get(key, default)

    def add(self, key, value, ttl=None):
        if key in self.store:
            return False
        self.store[key] = value
        return True


class _Sink:
    def __init__(self) -> None:
        self.fired: list[dict] = []

    def fire(self, **kwargs):
        self.fired.append(kwargs)
        return True


@pytest.fixture
def env(monkeypatch):
    cache, sink = _Cache(), _Sink()
    monkeypatch.setattr(heartbeat, "Cache", cache)
    monkeypatch.setattr(heartbeat, "AlertSink", sink)
    heartbeat.reset_scheduler_status_memo()
    yield cache, sink
    heartbeat.reset_scheduler_status_memo()


NOW = datetime(2026, 11, 17, 12, 0, tzinfo=UTC)


def test_fresh_tick_is_not_delayed(env) -> None:
    heartbeat.record_scheduler_tick(NOW - timedelta(minutes=1))
    status = heartbeat.scheduler_status(NOW, alert_family="x")
    assert status == {
        "delayed": False,
        "last_tick_at": (NOW - timedelta(minutes=1)).isoformat(),
    }
    assert env[1].fired == []


def test_never_ticked_is_delayed_and_pages_once(env) -> None:
    _, sink = env
    assert heartbeat.scheduler_status(NOW, alert_family="sched")["delayed"]
    assert heartbeat.scheduler_status(NOW, alert_family="sched")["delayed"]
    assert len(sink.fired) == 1
    assert sink.fired[0]["severity"] == "critical"
    assert sink.fired[0]["dedup_key"] == "sched:heartbeat"


def test_stale_tick_pages_once_per_outage(env) -> None:
    _, sink = env
    heartbeat.record_scheduler_tick(NOW - timedelta(minutes=11))
    status = heartbeat.scheduler_status(NOW, alert_family="s", consequence="Prices.")
    assert status["delayed"] is True
    heartbeat.scheduler_status(NOW + timedelta(minutes=5), alert_family="s")
    assert len(sink.fired) == 1
    assert "11 minutes" in sink.fired[0]["body"]
    assert "Prices." in sink.fired[0]["body"]
    # A new outage (a later last tick) pages again.
    heartbeat.record_scheduler_tick(NOW)
    heartbeat.scheduler_status(NOW + timedelta(minutes=20), alert_family="s")
    assert len(sink.fired) == 2


def test_no_family_means_no_page(env) -> None:
    assert heartbeat.scheduler_status(NOW)["delayed"] is True
    assert env[1].fired == []


def test_unreadable_cache_reads_not_delayed(env, monkeypatch) -> None:
    class _Broken:
        def get(self, *_a, **_k):
            raise ConnectionError("redis down")

    monkeypatch.setattr(heartbeat, "Cache", _Broken())
    assert heartbeat.scheduler_status(NOW, alert_family="s") == {
        "delayed": False,
        "last_tick_at": None,
    }
    assert env[1].fired == []


def test_live_read_is_memoised(env) -> None:
    cache, _ = env
    first = heartbeat.scheduler_status()
    assert first["delayed"] is True
    heartbeat.record_scheduler_tick()
    assert heartbeat.scheduler_status() == first  # memo window
    heartbeat.reset_scheduler_status_memo()
    assert heartbeat.scheduler_status()["delayed"] is False


def test_naive_time_is_refused(env) -> None:
    with pytest.raises(ValueError):
        heartbeat.record_scheduler_tick(datetime(2026, 1, 1))


def test_exported_from_the_layer_barrel() -> None:
    import cara.scheduling as scheduling

    assert scheduling.scheduler_status is heartbeat.scheduler_status
    assert scheduling.record_scheduler_tick is heartbeat.record_scheduler_tick
