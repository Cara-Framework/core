"""``queue:purge`` never deletes a dead letter whose ``failed()`` hook is unsettled.

Live defect: a dead-lettered or expired ledger row is the outbox row of its
job's ``failed()`` callback (the product's compensation) until that callback
completed or was quarantined. The retention prune waits for that; the
operator purge did not, so it deleted rows whose hook was still due, or
running under a live hook lease, and the compensation never fired. Before the
fix (``git show HEAD:cara/queues/delivery/QueueOperationsStore.py``) both
counts below were 4 and the due/running rows were gone.

The store's own WHERE clause is portable SQL; it runs here on an in-memory
SQLite table with the columns it reads. No PostgreSQL.
"""

from __future__ import annotations

import importlib
import sqlite3

import pytest

_STORE_MODULE = "cara.queues.delivery.QueueOperationsStore"
_OLD = "2026-11-01T00:00:00+00:00"
_CUTOFF = "2026-11-20T00:00:00+00:00"


class _SqliteDB:
    """The two ``DB`` facade calls the purge issues, on SQLite."""

    def __init__(self) -> None:
        self.connection = sqlite3.connect(":memory:")
        self.connection.row_factory = sqlite3.Row
        self.connection.execute(
            "CREATE TABLE queue_job_delivery (job_id TEXT PRIMARY KEY, "
            "queue TEXT, status TEXT, updated_at TEXT, "
            "post_hooks_completed_at TEXT, post_hooks_quarantined_at TEXT, "
            "post_hooks_lease_expires_at TEXT, replay_of TEXT)"
        )

    def add(self, job_id: str, **columns) -> None:
        row = {
            "queue": "sync",
            "status": "dead_lettered",
            "updated_at": _OLD,
            "post_hooks_completed_at": None,
            "post_hooks_quarantined_at": None,
            "post_hooks_lease_expires_at": None,
            "replay_of": None,
            **columns,
        }
        self.connection.execute(
            "INSERT INTO queue_job_delivery (job_id, queue, status, updated_at, "
            "post_hooks_completed_at, post_hooks_quarantined_at, "
            "post_hooks_lease_expires_at, replay_of) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [job_id, *row.values()],
        )

    def select_one(self, query: str, bindings=()):
        found = self.connection.execute(query.replace("%s", "?"), list(bindings))
        row = found.fetchone()
        return dict(row) if row is not None else None

    def statement(self, query: str, bindings=()) -> int:
        return self.connection.execute(query.replace("%s", "?"), list(bindings)).rowcount

    def job_ids(self) -> set[str]:
        rows = self.connection.execute("SELECT job_id FROM queue_job_delivery")
        return {row["job_id"] for row in rows}


@pytest.fixture
def ledger(monkeypatch: pytest.MonkeyPatch):
    module = importlib.import_module(_STORE_MODULE)
    database = _SqliteDB()
    monkeypatch.setattr(module, "DB", database)
    return module.QueueOperationsStore(), database


def test_purge_deletes_only_rows_whose_hook_completed_or_was_quarantined(
    ledger,
) -> None:
    store, database = ledger
    database.add("hook-due")
    database.add("hook-running", post_hooks_lease_expires_at="2099-01-01T00:00:00")
    database.add("hook-done", post_hooks_completed_at=_OLD)
    database.add("hook-quarantined", status="expired", post_hooks_quarantined_at=_OLD)

    counted = store.count_dead_lettered_older_than(cutoff=_CUTOFF)
    deleted = store.delete_terminal_deliveries_older_than(cutoff=_CUTOFF)

    assert counted == deleted == 2
    assert database.job_ids() == {"hook-due", "hook-running"}


def test_the_count_and_the_delete_keep_their_binding_order_per_queue(ledger) -> None:
    store, database = ledger
    database.add("sync-done", post_hooks_completed_at=_OLD)
    database.add("mail-done", queue="mail", post_hooks_completed_at=_OLD)
    database.add("mail-due", queue="mail")

    assert store.count_dead_lettered_older_than(cutoff=_CUTOFF, queue="mail") == 1
    assert store.delete_terminal_deliveries_older_than(cutoff=_CUTOFF, queue="mail") == 1
    assert database.job_ids() == {"sync-done", "mail-due"}
