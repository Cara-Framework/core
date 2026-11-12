"""A row whose broker copy is gone does not hold the broker window for 2 h.

Live (db-chaos stress, 2026-11-12): the vhost was deleted and recreated
mid-burst. The relay had published four rows on ``sync``, which filled the
queue's broker window of 4, and their broker copies went with the vhost. The
rows stayed ``pending``/``published``. The only path back to the outbox was
``recover_stale_executions``'s "unclaimed" pass, and that pass waits a full
``execution_lease_seconds`` (7,200 s). So 197 jobs of 20 tenants waited with
two idle workers. The same thing happens when the broker drops a queue's
messages for any other reason: a node lost without its data, a purge, or a
retired queue deleted by ``queue:topology``.

Workers consume with ``prefetch_count=1`` and claim a copy as soon as they
receive it. So a row that has stayed published and unclaimed for a heartbeat
lease, on a queue with NO ready messages, has no copy waiting for it, and the
relay returns it to the outbox at once. A queue whose depth the relay cannot
read, or that still holds ready messages, stays on the 2-h recovery.
"""

from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from cara.queues.delivery import QueueJobDeliveryStore


class _Application:
    def __init__(self, db):
        self.db = db

    def has(self, key):
        return key == "DB"

    def make(self, key):
        return self.db


class _LedgerDB:
    """Answers the aged-published scan and records the requeue."""

    def __init__(self, aged: dict[str, int]):
        self.aged = aged
        self.updates: list[tuple[str, list]] = []

    @contextmanager
    def transaction(self):
        yield self

    def select(self, sql, params):
        assert "GROUP BY queue" in sql
        return [{"queue": queue, "aged": count} for queue, count in self.aged.items()]

    def statement(self, sql, params):
        self.updates.append((sql, params))
        queues = params[3]
        return sum(self.aged[queue] for queue in queues)


class _Driver:
    def __init__(self, ready=None, error: Exception | None = None):
        self.ready = ready or {}
        self.error = error
        self.asked: list[list[str]] = []

    def broker_ready_counts(self, queues):
        self.asked.append(sorted(queues))
        if self.error is not None:
            raise self.error
        return {queue: self.ready[queue] for queue in queues if queue in self.ready}


def _store(db, driver) -> QueueJobDeliveryStore:
    store = QueueJobDeliveryStore(
        _Application(db), driver, {"canonical_queues": ("sync", "orders")}
    )
    store._lost_copy_checked_at = float("-inf")
    return store


def test_aged_published_rows_on_an_empty_broker_queue_return_to_the_outbox():
    db = _LedgerDB({"sync": 4})
    driver = _Driver(ready={"sync": 0})

    assert _store(db, driver).requeue_lost_broker_copies() == 4

    assert driver.asked == [["sync"]]
    ((sql, params),) = db.updates
    assert "SET publish_status = %s" in sql and params[0] == "pending"
    assert params[3] == ["sync"]


def test_a_queue_that_still_holds_ready_copies_waits_for_its_workers():
    db = _LedgerDB({"sync": 4, "orders": 2})
    driver = _Driver(ready={"sync": 3, "orders": 0})

    assert _store(db, driver).requeue_lost_broker_copies() == 2

    ((_sql, params),) = db.updates
    assert params[3] == ["orders"]


def test_an_unreadable_broker_leaves_the_two_hour_recovery_in_charge():
    db = _LedgerDB({"sync": 4})
    driver = _Driver(error=ConnectionError("broker down"))

    assert _store(db, driver).requeue_lost_broker_copies() == 0
    assert db.updates == []


def test_no_aged_rows_never_asks_the_broker():
    db = _LedgerDB({})
    driver = _Driver(ready={"sync": 0})

    assert _store(db, driver).requeue_lost_broker_copies() == 0
    assert driver.asked == []


def test_one_relay_asks_the_broker_at_most_once_per_interval():
    db = _LedgerDB({"sync": 4})
    driver = _Driver(ready={"sync": 5})
    store = _store(db, driver)

    store.requeue_lost_broker_copies()
    store.requeue_lost_broker_copies()

    assert len(driver.asked) == 1


def test_a_driver_without_broker_depth_is_left_alone():
    db = _LedgerDB({"sync": 4})
    store = _store(db, SimpleNamespace())

    assert store.requeue_lost_broker_copies() == 0
    assert db.updates == []


@pytest.mark.parametrize("ready", [None])
def test_publish_due_runs_the_lost_copy_pass(ready):
    calls: list[str] = []
    store = _store(_LedgerDB({}), _Driver())
    store.recover_stale_executions = lambda _n: (
        calls.append("stale")
        or {
            "requeued": 0,
            "reconciled": 0,
            "unclaimed": 0,
        }
    )
    store.requeue_lost_broker_copies = lambda _n: calls.append("lost") or 3
    store.expire_due = lambda _n: 0
    store._claim_publish_batch = lambda _limit: None

    result = store.publish_due()

    assert calls == ["stale", "lost"]
    assert result["lost_requeued"] == 3
