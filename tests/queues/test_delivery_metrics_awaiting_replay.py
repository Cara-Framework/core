"""The ledger gauge counts dead letters AWAITING REPLAY, like the operator tools.

``QueueDeliveryTerminalFailure`` alerts on
``queue_delivery_ledger_jobs{status=~"dead_lettered|expired"} > 0``. The
gauge counted every terminal row in the 90-day retained ledger, and a replay
inserts a child while its source keeps ``dead_lettered``, so one dead letter
— even one replayed successfully — kept the alert firing for 90 days.
``queue:monitor`` / ``queue:retry`` already read "dead-lettered" as "has no
replay child"; the gauge now reads that same one predicate.
"""

from __future__ import annotations

from types import SimpleNamespace

from cara.queues.delivery import QueueJobDeliveryStore
from cara.queues.delivery.QueueOperationsStore import QueueOperationsStore


class _LedgerDB:
    def __init__(self):
        self.aggregate: tuple[str, list] | None = None

    def select_one(self, sql, params):
        self.aggregate = (sql, params)
        return {"dead_lettered": 2, "expired": 1, "broker_max_outstanding": 0}

    def select(self, _sql, _params):
        return []


class _Application:
    def __init__(self, db):
        self.db = db

    def has(self, key):
        return key == "DB"

    def make(self, key):
        assert key == "DB"
        return self.db


def _awaiting(table: str) -> str:
    return QueueJobDeliveryStore._AWAITING_REPLAY_FILTER_TEMPLATE.format(table=table)


def test_terminal_totals_count_only_rows_without_a_replay_child():
    db = _LedgerDB()
    store = QueueJobDeliveryStore(
        _Application(db), SimpleNamespace(), {"canonical_queues": ("sync",)}
    )

    snapshot = store.delivery_metrics()

    sql, _params = db.aggregate
    predicate = _awaiting(store.table)
    # Both failure statuses are counted inside the awaiting-replay scope.
    assert f"WHERE status IN (%s, %s) AND {predicate}) AS failed_rows" in sql
    assert "COUNT(*) FILTER (WHERE status = %s) AS dead_lettered" in sql
    assert "COUNT(*) FILTER (WHERE status = %s) AS expired" in sql
    # Completed and retry_scheduled stay whole-ledger totals (every terminal
    # row carries completed_at: queue_job_delivery_execution_state_check).
    assert "COUNT(*) FILTER (WHERE status = %s) AS completed" in sql
    assert "WHERE status IN (%s, %s) AND completed_at IS NOT NULL" in sql
    assert "replay_child.replay_of = queue_job_delivery.job_id" in predicate
    assert snapshot["statuses"]["dead_lettered"] == 2
    assert snapshot["statuses"]["expired"] == 1


def test_operator_tools_and_the_gauge_share_one_predicate():
    operations = QueueOperationsStore.__new__(QueueOperationsStore)
    assert operations._awaiting_replay_condition() == _awaiting(
        QueueOperationsStore.ledger_table
    )
