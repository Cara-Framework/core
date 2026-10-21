"""Relay-owned ledger gauges refresh on a cadence, not once per relay cycle.

``relay_publish_once`` and the hooks loop refresh the delivery gauges after
every iteration — four times a second idle, back to back under load — and
the snapshot aggregates the whole retained ledger. Per iteration, that scan
ran three times (its lane query was duplicated verbatim) and set the pace of
publication itself, since a queue's broker window refills once per cycle.
"""

from __future__ import annotations

import importlib
import time
from types import SimpleNamespace

from cara.queues.delivery import QueueJobDeliveryStore
from cara.queues.drivers.AMQPDriver import AMQPDriver

_delivery_module = importlib.import_module("cara.queues.drivers._AMQPDelivery")


class _LedgerDB:
    def __init__(self):
        self.aggregates = 0
        self.lane_queries: list[tuple[str, list]] = []

    def select_one(self, _sql, _params):
        self.aggregates += 1
        return {"broker_max_outstanding": 1}

    def select(self, sql, params):
        self.lane_queries.append((sql, params))
        return [{"queue": "sync", "pending": 1, "completed_5m": 3}]


class _Application:
    def __init__(self, db):
        self.db = db

    def has(self, key):
        return key == "DB"

    def make(self, key):
        assert key == "DB"
        return self.db


def _store(db: _LedgerDB) -> QueueJobDeliveryStore:
    return QueueJobDeliveryStore(
        _Application(db),
        SimpleNamespace(),
        {"canonical_queues": ("sync", "connector")},
    )


def _driver(db: _LedgerDB, monkeypatch, clock: list[float]) -> AMQPDriver:
    monkeypatch.setattr(
        _delivery_module,
        "time",
        SimpleNamespace(monotonic=lambda: clock[0], time=time.time),
    )
    driver = AMQPDriver.__new__(AMQPDriver)
    driver._delivery_store = _store(db)
    driver._delivery_metrics_refreshed_at = None
    driver._delivery_metrics_snapshot = None
    return driver


def test_the_ledger_is_scanned_once_per_cadence(monkeypatch):
    db = _LedgerDB()
    clock = [1000.0]
    driver = _driver(db, monkeypatch, clock)
    cadence = AMQPDriver.DELIVERY_METRICS_REFRESH_SECONDS

    first = driver.refresh_delivery_metrics()
    clock[0] += cadence / 3
    served = driver.refresh_delivery_metrics()

    assert db.aggregates == 1
    assert served is first

    clock[0] += cadence
    driver.refresh_delivery_metrics()

    assert db.aggregates == 2


def test_an_operator_retry_refreshes_at_once(monkeypatch):
    db = _LedgerDB()
    clock = [1000.0]
    driver = _driver(db, monkeypatch, clock)
    retried: list[str] = []
    driver._delivery_store.retry_quarantined_terminal_hooks = lambda job_id, **_audit: (
        retried.append(job_id)
    )

    driver.refresh_delivery_metrics()
    driver.retry_quarantined_terminal_hooks("job-id", operator="ops", reason="fixed")

    assert retried == ["job-id"]
    assert db.aggregates == 2


def test_lane_backlog_is_one_scan_over_open_and_recent_rows():
    db = _LedgerDB()

    snapshot = _store(db).delivery_metrics()

    assert len(db.lane_queries) == 1
    sql, params = db.lane_queries[0]
    assert (
        "WHERE queue = ANY(%s) AND (status IN (%s, %s) OR (status = %s AND "
        "completed_at >= NOW() - INTERVAL '5 minutes')) GROUP BY queue"
    ) in sql
    assert params[-4:] == [
        ["connector", "sync"],
        QueueJobDeliveryStore.STATUS_PENDING,
        QueueJobDeliveryStore.STATUS_PROCESSING,
        QueueJobDeliveryStore.STATUS_COMPLETED,
    ]
    assert snapshot["lane_backlog"]["sync"]["pending"] == 1
    assert snapshot["lane_backlog"]["sync"]["throughput_per_second"] == 0.01
