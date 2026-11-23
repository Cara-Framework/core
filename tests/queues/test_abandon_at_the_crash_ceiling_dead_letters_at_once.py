"""A forced exit that keeps a spent attempt at the crash ceiling dead-letters.

Live defect: a forced worker exit keeps the attempt spent for a run that had
outlived its timeout (``abandon_execution(..., refund_attempt=False)``), but
the abandoned row still went back to ``pending``. The crash ceiling was only
checked when a lease LAPSED (recovery, or a broker copy reclaiming the row),
so every forced-exit abandon handed the row out again and ``attempts`` grew
past the ceiling until some lease happened to lapse without a release.

Now the abandon settles such a row as a crash loop in its own transaction,
through the ONE routine recovery and reclaim use, guarded by the caller's
lease token. The fake ledger below answers the statements the way PostgreSQL
would, guards included (it extends the crash-recovery suite's fake).
"""

from __future__ import annotations

import importlib

import pendulum
import pytest

from cara.configuration import Configuration
from cara.queues.delivery import DeliveryClaim, DeliveryLeaseLost, QueueJobDeliveryStore
from tests.queues.test_queue_delivery_crash_recovery import (
    _CrashDB,
    _envelope,
    _PublishingDriver,
    _store,
)

_LEASE = importlib.import_module("cara.commands.core._DeliveryLease")
_CEILING = QueueJobDeliveryStore.CRASH_RECLAIM_CEILING
_TOKEN = "crashed-worker-lease"


@pytest.fixture(autouse=True)
def _isolated_configuration(monkeypatch):
    monkeypatch.setattr(Configuration, "_instance", None)
    Configuration.empty()


class _LedgerDB(_CrashDB):
    """One delivery row and its tracker, with the crash-loop statements."""

    def __init__(self, envelope, payload, *, attempts, tracker_status="processing"):
        super().__init__(envelope, payload)
        self.row.update(
            {"attempts": attempts, "completed_at": None, "post_hooks_completed_at": None}
        )
        self.tracker_status = tracker_status

    def select(self, sql, params):
        rows = super().select(sql, params)
        if "WHERE delivery.status = %s" in sql:
            for row in rows:
                row["attempts"] = self.row["attempts"]
        return rows

    def select_one(self, sql, params=None):
        if sql.startswith("SELECT status FROM job WHERE id = %s"):
            return {"status": self.tracker_status}
        if sql.startswith("SELECT db_job_id, attempts FROM"):
            held = (
                self.row["status"] == params[1] and self.row["lease_token"] == params[2]
            )
            return dict(self.row) if held else None
        if sql.startswith("SELECT status, lease_token FROM"):
            return {"status": self.row["status"], "lease_token": self.row["lease_token"]}
        return super().select_one(sql, params)

    def _holds(self, sql, guard):
        expiry = self.row["lease_expires_at"]
        if "lease_token = %s AND lease_expires_at <= %s" in sql:
            return self.row["lease_token"] == guard[0] and expiry <= guard[1]
        if "(lease_expires_at IS NULL OR lease_expires_at <= %s)" in sql:
            return expiry is None or expiry <= guard[0]
        if sql.endswith("AND lease_token = %s"):
            return self.row["lease_token"] == guard[0]
        raise AssertionError(f"Unknown crash-loop guard: {sql}")

    def statement(self, sql, params):
        if "terminal_reason = %s" in sql and "AND status = %s AND" in sql:
            if "post_hooks_completed_at" in sql:
                raise AssertionError("the crash loop must not touch the hook stamp")
            matches = (
                self.row["job_id"] == params[4]
                and self.row["status"] == params[5]
                and self._holds(sql, params[6:])
            )
            if matches:
                self.events.append("crash_loop")
                self.row.update(
                    {
                        "status": params[0],
                        "completed_at": params[1],
                        "terminal_reason": params[2],
                        "lease_token": None,
                        "lease_expires_at": None,
                    }
                )
            return int(matches)
        if sql.startswith("UPDATE job SET status = %s, completed_at = COALESCE("):
            if self.tracker_status not in params[4]:
                return 0
            self.tracker_status = params[0]
            return 1
        if "SET attempts = attempts - 1" in sql:
            held = self.row["lease_token"] == params[2] and self.row["attempts"] > 0
            if held:
                self.row["attempts"] -= 1
            return int(held)
        if "SET status = %s, lease_token = NULL" in sql:
            held = (
                self.row["status"] == params[5] and self.row["lease_token"] == params[6]
            )
            if held:
                self.events.append("released")
                self.row.update(
                    {"status": params[0], "lease_token": None, "lease_expires_at": None}
                )
            return int(held)
        return super().statement(sql, params)


def _ledger(*, attempts, live_lease=True, tracker_status="processing"):
    body, payload = _envelope()
    db = _LedgerDB(body, payload, attempts=attempts, tracker_status=tracker_status)
    if live_lease:
        # A forced exit abandons its OWN live lease.
        db.row["lease_expires_at"] = pendulum.now("UTC").add(seconds=60)
    return db, _store(db, _PublishingDriver()), body, payload


def _crash_loop_settled(db) -> bool:
    return (
        db.row["status"] == "dead_lettered"
        and db.row["terminal_reason"] == QueueJobDeliveryStore.CRASH_LOOP_REASON
        and db.row["completed_at"] is not None
        and db.row["lease_token"] is None
        and db.row["post_hooks_completed_at"] is None  # failed() still due
        and db.tracker_status == "failed"
    )


# ── the abandon ──────────────────────────────────────────────────────────
def test_an_overdue_abandon_at_the_ceiling_is_dead_lettered_at_once():
    db, store, _body, _payload = _ledger(attempts=_CEILING)

    settled = store.abandon_execution(db.row["job_id"], _TOKEN, refund_attempt=False)

    assert settled is True
    assert _crash_loop_settled(db)
    assert db.row["attempts"] == _CEILING
    assert db.events == ["begin", "crash_loop", "commit"]


def test_an_overdue_abandon_below_the_ceiling_is_still_handed_back():
    db, store, _body, _payload = _ledger(attempts=_CEILING - 1)

    settled = store.abandon_execution(db.row["job_id"], _TOKEN, refund_attempt=False)

    assert settled is False
    assert (db.row["status"], db.row["attempts"]) == ("pending", _CEILING - 1)
    assert db.events == ["begin", "released", "commit"]


def test_a_graceful_abandon_at_the_ceiling_refunds_and_is_handed_back():
    db, store, _body, _payload = _ledger(attempts=_CEILING)

    settled = store.abandon_execution(db.row["job_id"], _TOKEN)

    assert settled is False
    assert (db.row["status"], db.row["attempts"]) == ("pending", _CEILING - 1)


def test_a_finished_tracker_at_the_ceiling_is_not_a_crash_loop():
    db, store, _body, _payload = _ledger(attempts=_CEILING, tracker_status="completed")

    settled = store.abandon_execution(db.row["job_id"], _TOKEN, refund_attempt=False)

    assert settled is False
    assert db.row["status"] == "pending"
    assert db.tracker_status == "completed"


def test_a_row_another_holder_took_is_never_settled():
    db, store, _body, _payload = _ledger(attempts=_CEILING)
    db.row["lease_token"] = "another-workers-lease"

    with pytest.raises(DeliveryLeaseLost):
        store.abandon_execution(db.row["job_id"], _TOKEN, refund_attempt=False)

    assert (db.row["status"], db.row["lease_token"]) == (
        "processing",
        "another-workers-lease",
    )
    assert db.tracker_status == "processing"
    assert "crash_loop" not in db.events


# ── one routine for all three ways in ────────────────────────────────────
def test_recovery_reclaim_and_abandon_settle_the_same_row_the_same_way():
    recovered, recovery_store, _body, _payload = _ledger(
        attempts=_CEILING, live_lease=False
    )
    result = recovery_store.recover_stale_executions()

    reclaimed, reclaim_store, body, payload = _ledger(attempts=_CEILING, live_lease=False)
    claim = reclaim_store.claim_execution(body=body, payload=payload)

    abandoned, abandon_store, _body, _payload = _ledger(attempts=_CEILING)
    abandon_store.abandon_execution(_payload["job_id"], _TOKEN, refund_attempt=False)

    assert result["reconciled"] == 1 and result["requeued"] == 0
    assert claim == DeliveryClaim(
        "dead_lettered", terminal_reason=QueueJobDeliveryStore.CRASH_LOOP_REASON
    )
    for db in (recovered, reclaimed, abandoned):
        assert _crash_loop_settled(db)
        assert "crash_loop" in db.events


def test_recovery_below_the_ceiling_still_requeues():
    db, store, _body, _payload = _ledger(attempts=_CEILING - 1, live_lease=False)

    result = store.recover_stale_executions()

    assert result["requeued"] == 1
    assert db.row["status"] == "pending"
    assert "crash_loop" not in db.events


# ── the worker's forced-exit release ─────────────────────────────────────
class _SettlingStore:
    execution_heartbeat_seconds = 600

    def renew_execution(self, *_args, **_kwargs) -> bool:
        return True

    def abandon_execution(self, *_args, **_kwargs) -> bool:
        return True


class _Tracker:
    def __init__(self) -> None:
        self.statuses: list[tuple] = []

    def update_job_status_strict(self, db_job_id, status) -> None:
        self.statuses.append((db_job_id, status))


def test_the_release_does_not_reopen_a_tracker_the_store_just_failed():
    tracker = _Tracker()
    claim = DeliveryClaim(
        "claimed", _TOKEN, lease_deadline=pendulum.now("UTC").subtract(seconds=1)
    )
    budget = _LEASE._RunBudget(claim.lease_deadline)
    _LEASE._hold_delivery_lease(_SettlingStore(), "job-1", claim, tracker, 91, budget)

    assert _LEASE.LeaseHeartbeat.release_all(timeout_seconds=5.0) == 1
    assert tracker.statuses == []
