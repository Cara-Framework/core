"""The execution claim marks the tracker ``processing`` in its own transaction.

Measured 2026-11-13 (Synkronus capacity.md, one worker slot, 5 ms jobs): a
job paid three transactions — the claim, a separate ``JobTracker`` write
(read, save, read back) and the settlement. Every commit is a WAL flush that
queues behind the rest of the host's writes, so that third flush was the
largest fixed cost of a short job. The stamp now rides the claim.
"""

from __future__ import annotations

import pytest

from cara.configuration import Configuration
from cara.exceptions import QueueException
from tests.queues.test_queue_delivery_crash_recovery import (
    _CrashDB,
    _envelope,
    _PublishingDriver,
    _store,
)


@pytest.fixture(autouse=True)
def _isolated_configuration(monkeypatch):
    monkeypatch.setattr(Configuration, "_instance", None)
    Configuration.empty()


class _TrackedDB(_CrashDB):
    """Answers the tracker stamp the way Postgres would, guard included."""

    def __init__(self, envelope, payload, *, tracker_status, tracker_exists=True):
        super().__init__(envelope, payload)
        self.tracker_status = tracker_status
        self.tracker_exists = tracker_exists
        self.tracker_stamped_in: list[int] = []

    def statement(self, sql, params):
        if sql.startswith("UPDATE job SET status = %s, started_at = %s"):
            self.events.append("tracker")
            excluded = params[4] if len(params) > 4 else []
            if not self.tracker_exists or self.tracker_status in excluded:
                return 0
            self.tracker_status = params[0]
            return 1
        return super().statement(sql, params)


def _claim(row_status: str, **db_options):
    body, payload = _envelope()
    db = _TrackedDB(body, payload, **db_options)
    db.row["status"] = row_status
    if row_status == "pending":
        db.row.update({"lease_token": None, "lease_expires_at": None})
    store = _store(db, _PublishingDriver())
    return db, store.claim_execution(body=body, payload=payload)


def test_a_fresh_claim_stamps_processing_before_its_commit():
    db, claim = _claim("pending", tracker_status="pending")

    assert claim.outcome == "claimed" and not claim.reclaimed
    assert db.tracker_status == "processing"
    assert db.events == ["begin", "tracker", "commit"]


def test_a_retried_job_moves_from_retrying_to_processing():
    db, claim = _claim("pending", tracker_status="retrying")

    assert claim.outcome == "claimed"
    assert db.tracker_status == "processing"


def test_a_fresh_claim_without_its_tracker_rolls_back():
    body, payload = _envelope()
    db = _TrackedDB(body, payload, tracker_status="pending", tracker_exists=False)
    db.row.update({"status": "pending", "lease_token": None, "lease_expires_at": None})
    store = _store(db, _PublishingDriver())

    with pytest.raises(QueueException, match="does not exist"):
        store.claim_execution(body=body, payload=payload)

    assert db.events == ["begin", "tracker", "rollback"]


def test_a_reclaimed_row_leaves_a_finished_tracker_finished():
    # A crashed run whose body finished: the worker's completion lookup
    # settles it without running the body again, so the claim must not
    # reopen the tracker.
    db, claim = _claim("processing", tracker_status="completed")

    assert claim.outcome == "claimed" and claim.reclaimed
    assert db.tracker_status == "completed"
    assert db.events == ["begin", "tracker", "commit"]


def test_a_reclaimed_row_whose_body_never_finished_is_processing_again():
    db, claim = _claim("processing", tracker_status="pending")

    assert claim.reclaimed
    assert db.tracker_status == "processing"
