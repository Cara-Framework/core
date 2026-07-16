import importlib
import os
import subprocess
from contextlib import contextmanager
from types import SimpleNamespace

import pendulum
import pytest

from cara.commands.core.QueueRelayCommand import (
    QueueHooksCommand,
    QueueRelayCommand,
)
from cara.commands.core.QueueStatsCommand import QueueStatsCommand
from cara.exceptions import QueueException
from cara.queues.delivery import QueueJobDeliveryStore
from cara.queues.tracking import JobTracker


def test_relay_readiness_tracks_runtime_failure_not_quarantined_work_item():
    command = QueueRelayCommand.__new__(QueueRelayCommand)

    assert command._iteration_is_healthy(
        None,
        {"published": 1, "retried": 0, "quarantined": 0},
    )
    assert not command._iteration_is_healthy(
        None,
        {"published": 0, "retried": 1, "quarantined": 0},
    )
    assert command._iteration_is_healthy(
        None,
        {"published": 0, "retried": 0, "quarantined": 1},
    )
    assert command._iteration_has_failures(
        {"published": 0, "retried": 0, "quarantined": 1}
    )


def test_hook_readiness_separates_service_health_from_failed_work_items():
    command = QueueHooksCommand.__new__(QueueHooksCommand)

    assert command._iteration_is_healthy(
        None,
        {"completed": 1, "failed": 0},
    )
    failed = {"completed": 0, "failed": 1, "quarantined": 1}
    assert command._iteration_is_healthy(None, failed)
    assert command._iteration_has_failures(failed)


def test_hook_timeout_is_immediately_deferred_instead_of_waiting_for_stale_lease(
    monkeypatch,
):
    deferred = []
    driver = SimpleNamespace(
        delivery_store=SimpleNamespace(hook_timeout_seconds=5),
        due_terminal_hook_ids=lambda: ["hook-job-id"],
        defer_terminal_hook_process_failure=lambda job_id, *, error: (
            deferred.append((job_id, error)) or "deferred"
        ),
        refresh_delivery_metrics=lambda: {
            "hooks": {"failed": 1, "stale": 0, "quarantined": 0}
        },
    )
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            subprocess.TimeoutExpired("queue:hook", 20)
        ),
    )

    result = QueueHooksCommand._run_isolated_hooks(driver)

    assert deferred == [
        ("hook-job-id", "isolated terminal-hook process timed out")
    ]
    assert result["failed"] == 1
    assert result["deferred"] == 1


def test_hook_live_lease_child_exit_is_skipped_without_stealing_lease(
    monkeypatch,
):
    driver = SimpleNamespace(
        delivery_store=SimpleNamespace(hook_timeout_seconds=5),
        due_terminal_hook_ids=lambda: ["hook-job-id"],
        defer_terminal_hook_process_failure=lambda *_args, **_kwargs: (
            (_ for _ in ()).throw(
                AssertionError("live lease must not be deferred")
            )
        ),
        refresh_delivery_metrics=lambda: {
            "hooks": {"failed": 0, "stale": 0, "quarantined": 0}
        },
    )
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(
            returncode=getattr(os, "EX_TEMPFAIL", 75)
        ),
    )

    result = QueueHooksCommand._run_isolated_hooks(driver)

    assert result["skipped"] == 1
    assert result["failed"] == 0


class _HookDB:
    def __init__(self, *, attempts=0, last_error=None):
        self.row = {
            "post_hooks_completed_at": None,
            "post_hooks_lease_token": "lease-token",
            "post_hooks_attempts": attempts,
            "post_hooks_quarantined_at": None,
            "post_hooks_last_error": last_error,
        }
        self.statements = []

    @contextmanager
    def transaction(self):
        yield self

    def select_one(self, _sql, _params=None):
        return dict(self.row)

    def statement(self, sql, params):
        self.statements.append((sql, params))
        self.row["post_hooks_attempts"] = params[0]
        if "post_hooks_quarantined_at = %s" in sql:
            self.row["post_hooks_quarantined_at"] = params[1]
        else:
            self.row["post_hooks_last_error"] = params[2]
        return 1


class _Application:
    def __init__(self, db):
        self.db = db

    def has(self, key):
        return key == "DB"

    def make(self, key):
        assert key == "DB"
        return self.db


def _store(db, **options):
    return QueueJobDeliveryStore(
        _Application(db),
        SimpleNamespace(),
        {"canonical_queues": ("sync", "connector"), **options},
    )


def test_hook_process_failure_counts_new_attempt_after_claim_clears_old_error():
    db = _HookDB(attempts=1, last_error=None)
    store = _store(db)

    outcome = store.defer_terminal_hook_process_failure(
        "hook-job-id",
        error="Authorization: Bearer secret-token\x00",
    )

    assert outcome == "deferred"
    assert db.row["post_hooks_attempts"] == 2
    persisted_error = db.statements[0][1][2]
    assert "secret-token" not in persisted_error
    assert "\x00" not in persisted_error


def test_hook_parent_does_not_double_count_child_self_deferral():
    db = _HookDB(attempts=2, last_error="child already deferred")
    store = _store(db)

    outcome = store.defer_terminal_hook_process_failure(
        "hook-job-id",
        error="child exited nonzero",
    )

    assert outcome == "already_deferred"
    assert db.statements == []


def test_hook_failure_is_quarantined_at_bounded_attempt_limit():
    db = _HookDB(attempts=1)
    store = _store(db, delivery_hook_max_attempts=2)

    outcome = store.defer_terminal_hook_process_failure(
        "hook-job-id",
        error="permanent failure",
    )

    assert outcome == "quarantined"
    assert db.row["post_hooks_attempts"] == 2
    assert isinstance(db.row["post_hooks_quarantined_at"], pendulum.DateTime)


class _PublishReleaseDB:
    def __init__(self, affected):
        self.affected = affected
        self.params = None

    def statement(self, _sql, params):
        self.params = params
        return self.affected


def test_publish_retry_release_reports_cas_loss_and_redacts_persisted_error():
    db = _PublishReleaseDB(0)
    store = _store(db)

    released = store._release_publish(
        "publish-job-id",
        "publish-lease",
        0,
        "api_key=super-secret\x00",
    )

    assert released is False
    assert "super-secret" not in db.params[3]
    assert "\x00" not in db.params[3]


class _PriorityClaimDB:
    def __init__(self):
        self.sql = None
        self.params = None

    @contextmanager
    def transaction(self):
        yield self

    def select_one(self, sql, params):
        if sql.startswith("SELECT pg_advisory_xact_lock"):
            return {"locked": None}
        self.sql = sql
        self.params = params
        return None


def test_priority_claim_uses_bounded_indexed_queue_lane_heads():
    db = _PriorityClaimDB()
    store = _store(db, delivery_priority_aging_seconds=120)

    assert store._claim_next_publish() is None
    assert db.sql.startswith("WITH settings AS")
    assert db.sql.count("FOR UPDATE OF delivery SKIP LOCKED LIMIT 1") == 1
    assert "priority_lanes(priority, base_rank) AS (VALUES" in db.sql
    assert "('critical', 0)" in db.sql
    assert "('high', 1)" in db.sql
    assert "('default', 2)" in db.sql
    assert "('low', 3)" in db.sql
    assert "CROSS JOIN LATERAL" in db.sql
    assert "delivery.queue = eligible_queues.queue" in db.sql
    assert "delivery.priority = priority_lanes.priority" in db.sql
    assert "UPDATE queue_job_delivery AS delivery" in db.sql
    assert db.params[1] == 120
    assert db.params[2] == 2
    assert "eligible_queues AS" in db.sql
    assert "unnest(%s::varchar[])" in db.sql
    assert db.params[3] == ["connector", "sync"]
    assert "outstanding.publish_lease_expires_at > settings.now" in db.sql
    assert "< settings.broker_window" in db.sql
    assert "WHERE EXISTS" not in db.sql
    assert "status IN ('pending', 'processing')" in db.sql


class _DeliveryMetricsDB:
    def __init__(self):
        self.sql = None
        self.params = None

    def select_one(self, sql, params):
        self.sql = sql
        self.params = params
        return {
            "priority_critical_pending": 2,
            "priority_critical_oldest_due_age": 45,
            "priority_high_pending": 3,
            "priority_high_oldest_due_age": 90,
            "priority_default_pending": 5,
            "priority_default_oldest_due_age": 180,
            "priority_low_pending": 8,
            "priority_low_oldest_due_age": 360,
            "broker_max_outstanding": 2,
        }


def test_delivery_metrics_expose_bounded_priority_slo_and_broker_window():
    db = _DeliveryMetricsDB()
    snapshot = _store(
        db,
        delivery_priority_aging_seconds=120,
        delivery_broker_window_per_queue=2,
    ).delivery_metrics()

    assert snapshot["priority_backlog"] == {
        "critical": {
            "pending": 2,
            "oldest_due_age": 45.0,
            "latency_budget": 120,
        },
        "high": {
            "pending": 3,
            "oldest_due_age": 90.0,
            "latency_budget": 240,
        },
        "default": {
            "pending": 5,
            "oldest_due_age": 180.0,
            "latency_budget": 360,
        },
        "low": {
            "pending": 8,
            "oldest_due_age": 360.0,
            "latency_budget": 480,
        },
    }
    assert snapshot["broker_window"] == {
        "max_outstanding": 2,
        "limit": 2,
    }
    assert "MIN(available_at) FILTER" in db.sql
    assert "GROUP BY window_row.queue" in db.sql


class _DeliveryStatsDB:
    def __init__(self):
        self.calls = []

    def select_one(self, sql, params):
        self.calls.append((sql, params))
        if "AS active_total" in sql:
            return {
                "active_total": 3,
                "pending": 2,
                "processing": 1,
                "due_unpublished": 2,
                "oldest_due_age": 45,
                "publish_processing": 1,
                "stale_publish": 0,
                "stale_execution": 1,
            }
        if "AS terminal_recent_total" in sql:
            return {
                "terminal_recent_total": 9,
                "completed": 7,
                "retry_scheduled": 1,
                "dead_lettered": 1,
                "expired": 0,
            }
        if "AS hook_quarantined" in sql:
            return {
                "hook_pending": 1,
                "hook_processing": 2,
                "hook_stale": 3,
                "hook_failed": 4,
                "hook_quarantined": 0,
            }
        raise AssertionError(f"Unexpected stats query: {sql}")


def test_delivery_stats_apply_queue_and_recent_window_to_ledger():
    db = _DeliveryStatsDB()

    stats = _store(db).delivery_stats("sync", recent_hours=24)

    assert stats["queue"] == "sync"
    assert stats["recent_hours"] == 24
    assert stats["active_total"] == 3
    assert stats["terminal_recent_total"] == 9
    assert stats["due_unpublished"] == 2
    assert stats["stale_leases"] == {"publish": 0, "execution": 1}
    assert stats["hooks"] == {
        "pending": 1,
        "processing": 2,
        "stale": 3,
        "failed": 4,
        "quarantined": 0,
    }
    active_sql, active_params = db.calls[0]
    terminal_sql, terminal_params = db.calls[1]
    hooks_sql, hooks_params = db.calls[2]
    assert "WHERE queue = %s" in active_sql
    assert "created_at >=" not in active_sql
    assert "make_interval" not in active_sql
    assert "status IN ('pending', 'processing')" in active_sql
    assert active_params[-1] == "sync"
    assert "completed_at >= NOW() - make_interval(hours => %s)" in terminal_sql
    assert terminal_params[-2:] == ["sync", 24]
    assert "post_hooks_lease_token IS NULL" in hooks_sql
    assert "post_hooks_lease_expires_at > NOW()" in hooks_sql
    assert "post_hooks_lease_expires_at <= NOW()" in hooks_sql
    assert "post_hooks_last_error IS NOT NULL" in hooks_sql
    assert hooks_params == [
        "sync",
        list(QueueJobDeliveryStore.HOOK_TERMINAL_STATUSES),
    ]


def test_queue_stats_propagates_runtime_failure_instead_of_exit_zero(
    monkeypatch,
):
    module = importlib.import_module(
        "cara.commands.core.QueueStatsCommand"
    )
    command = QueueStatsCommand.__new__(QueueStatsCommand)
    monkeypatch.setattr(
        module,
        "Queue",
        SimpleNamespace(
            driver=lambda *_args, **_kwargs: (_ for _ in ()).throw(
                ConnectionError("database unavailable")
            )
        ),
    )

    with pytest.raises(ConnectionError, match="database unavailable"):
        command._show_stats_once("sync", 24)


class _CreatingJobModel:
    STATUS_PENDING = "pending"

    def __init__(self):
        self.created = None

    @staticmethod
    def generate_public_id():
        return "JOB_TEST"

    def create(self, values):
        self.created = values
        return SimpleNamespace(id=91)


@pytest.mark.parametrize("execution_mode", ["sync", "queued", "scheduler"])
def test_job_tracker_records_explicit_execution_mode_without_mutating_metadata(
    execution_mode,
):
    model = _CreatingJobModel()
    metadata = {"source": "test"}

    job_id = JobTracker(model).create_job_record(
        job_name="ExampleJob",
        job_class="tests.ExampleJob",
        queue="sync",
        execution_mode=execution_mode,
        metadata=metadata,
    )

    assert job_id == 91
    assert model.created["metadata"] == {
        "source": "test",
        "execution_mode": execution_mode,
    }
    assert metadata == {"source": "test"}


def test_job_tracker_rejects_ambiguous_execution_mode():
    with pytest.raises(ValueError, match="execution_mode"):
        JobTracker(_CreatingJobModel()).create_job_record(
            job_name="ExampleJob",
            job_class="tests.ExampleJob",
            queue="sync",
            execution_mode="maybe",
        )


def test_publish_outage_stops_after_one_broker_attempt():
    store = _store(SimpleNamespace(), delivery_claim_batch=100)
    claim_calls = []
    release_calls = []
    row = {
        "job_id": "publish-job-id",
        "publish_attempts": 0,
    }
    store.recover_stale_executions = lambda _limit: {
        "requeued": 0,
        "reconciled": 0,
    }
    store.expire_due = lambda _limit: 0
    store._claim_next_publish = lambda: (
        claim_calls.append(True) or (row, "publish-token")
    )
    store._publish_claimed = lambda *_args: (_ for _ in ()).throw(
        ConnectionError("broker unavailable")
    )
    store._release_publish = lambda *args: (
        release_calls.append(args) or True
    )

    result = store.publish_due()

    assert len(claim_calls) == 1
    assert len(release_calls) == 1
    assert result["claimed"] == 1
    assert result["retried"] == 1


class _InvalidEnvelopeDB:
    def __init__(self):
        self.row = {
            "job_id": "invalid-job-id",
            "db_job_id": 91,
            "status": "processing",
            "terminal_reason": None,
            "publish_status": "processing",
            "post_hooks_completed_at": None,
            "post_hooks_quarantined_at": None,
            "post_hooks_attempts": 0,
            "post_hooks_last_error": None,
        }
        self.tracker_status = "processing"
        self.statements = []

    @contextmanager
    def transaction(self):
        yield self

    def select_one(self, sql, params=None):
        if sql.startswith("UPDATE queue_job_delivery SET status = %s"):
            self.row.update(
                {
                    "status": params[0],
                    "terminal_reason": params[2],
                    "publish_status": params[3],
                    "post_hooks_quarantined_at": params[5],
                    "post_hooks_last_error": params[6],
                }
            )
            return {"db_job_id": self.row["db_job_id"]}
        if sql.startswith(
            "SELECT status, terminal_reason, post_hooks_completed_at"
        ):
            return dict(self.row)
        raise AssertionError(f"Unexpected SELECT ONE: {sql}")

    def select(self, sql, _params):
        if sql.startswith("SELECT job_id FROM queue_job_delivery"):
            return (
                []
                if self.row["post_hooks_quarantined_at"] is not None
                else [{"job_id": self.row["job_id"]}]
            )
        raise AssertionError(f"Unexpected SELECT: {sql}")

    def statement(self, sql, params):
        self.statements.append((sql, params))
        if sql.startswith("UPDATE job SET status = %s"):
            self.tracker_status = params[0]
            return 1
        raise AssertionError(f"Unexpected STATEMENT: {sql}")


def test_invalid_signed_envelope_quarantines_and_skips_terminal_hooks():
    db = _InvalidEnvelopeDB()
    store = _store(db)

    store._quarantine_publish(
        "invalid-job-id",
        "publish-token",
        "signature mismatch",
    )

    assert db.row["status"] == "dead_lettered"
    assert db.row["publish_status"] == "failed"
    assert db.row["terminal_reason"].startswith("publish_envelope_invalid:")
    assert db.row["post_hooks_quarantined_at"] is not None
    assert db.row["post_hooks_last_error"] == (
        "terminal hooks skipped: signed envelope is invalid"
    )
    assert store.due_terminal_hook_ids() == []
    with pytest.raises(
        QueueException,
        match="Invalid signed envelopes cannot execute terminal hooks",
    ):
        store.retry_quarantined_terminal_hooks(
            "invalid-job-id",
            operator="root:USR_ROOT",
            reason="Verified recovery attempt",
        )
