"""``queue:monitor`` reads every queue's tracker counts in ONE grouped query.

Live defect: the monitor issued five ``COUNT(*) … WHERE queue = %s`` queries
per queue (N+1), and a NULL tracker queue — listed as ``default`` by
``list_distinct_queues_since`` — was then counted with ``queue = %s``, which
never matches NULL, so the ``default`` line always read zero. Before the fix
(``git show HEAD:cara/commands/core/QueueMonitorCommand.py``) no grouped
query existed, the per-queue counts were issued, and ``default`` printed
``Pending:    0``.

The REAL ``QueueOperationsStore`` runs against a recording fake ``DB`` that
answers the grouped statement from rows; no database.
"""

from __future__ import annotations

import importlib

import pendulum
import pytest

from cara.commands.core.QueueMonitorCommand import QueueMonitorCommand

_STORE_MODULE = "cara.queues.delivery.QueueOperationsStore"


class _FakeDB:
    def __init__(self, jobs: list[dict]) -> None:
        self.jobs = jobs
        self.queries: list[str] = []

    def select(self, query: str, bindings=()):
        self.queries.append(query)
        if "GROUP BY 1, 2" in query:
            cutoff, queues = bindings
            grouped: dict[tuple, list[int]] = {}
            for row in self.jobs:
                queue = row["queue"] or "default"
                if queue in queues:
                    cell = grouped.setdefault((queue, row["status"]), [0, 0])
                    cell[0] += 1
                    cell[1] += int(row["created_at"] >= cutoff)
            return [
                {"queue": q, "status": s, "total": total, "recent": recent}
                for (q, s), (total, recent) in grouped.items()
            ]
        if "SELECT DISTINCT" in query:
            return [{"queue": row["queue"] or "default"} for row in self.jobs]
        return []  # recent failure samples

    def select_one(self, query: str, bindings=()):
        self.queries.append(query)
        return {"total": 0}


def _job(queue, status, created_at) -> dict:
    return {"queue": queue, "status": status, "created_at": created_at}


@pytest.fixture
def monitor(monkeypatch: pytest.MonkeyPatch):
    now = pendulum.now("UTC")
    jobs = [
        _job(None, "pending", now),
        _job(None, "pending", now.subtract(days=3)),
        _job(None, "completed", now),
        _job(None, "failed", now),
        _job(None, "failed", now.subtract(days=3)),  # outside the 24 h window
        _job("sync", "processing", now),
        _job("sync", "success", now),
        _job("sync", "retrying", now),
        _job("mail", "pending", now),
    ]
    module = importlib.import_module(_STORE_MODULE)
    database = _FakeDB(jobs)
    monkeypatch.setattr(module, "DB", database)
    command = QueueMonitorCommand(application=None)
    lines: list[str] = []
    monkeypatch.setattr(command, "line", lambda text="", *_a, **_k: lines.append(text))
    return command, module.QueueOperationsStore(), database, lines


def _queue_block(lines: list[str], queue: str) -> str:
    start = next(i for i, line in enumerate(lines) if f"Queue: {queue}<" in line)
    return "\n".join(lines[start : start + 3])


def test_three_queues_cost_one_grouped_tracker_count(monitor) -> None:
    command, store, database, _lines = monitor

    assert command.handle(queue=None, limit=20, store=store) == 0

    grouped = [q for q in database.queries if "GROUP BY 1, 2" in q]
    per_queue_counts = [
        q
        for q in database.queries
        if "COUNT(*) AS total FROM job" in q and "queue = %s" in q
    ]
    assert len(grouped) == 1
    assert per_queue_counts == []


def test_a_null_queue_is_counted_under_default(monitor) -> None:
    command, store, _database, lines = monitor

    assert command.handle(queue=None, limit=20, store=store) == 0

    block = _queue_block(lines, "default")
    assert "Pending:    2 | Processing:   0 | Completed:     1 | Failed:    1" in block
    assert "(1/2)" in block
    sync = _queue_block(lines, "sync")
    assert "Processing:   1 | Completed:     1 | Failed:    0 | Retrying:   1" in sync


def test_the_grouped_query_coalesces_null_and_groups_by_queue_and_status(
    monitor,
) -> None:
    _command, store, database, _lines = monitor
    cutoff = pendulum.now("UTC").subtract(hours=24)

    counts = store.count_jobs_by_queue_and_status(
        queues=["default", "sync"], created_at_gte=cutoff
    )

    (query,) = database.queries
    assert query.startswith("SELECT COALESCE(queue, 'default') AS queue, status")
    assert "COALESCE(queue, 'default') = ANY(%s::varchar[])" in query
    assert query.endswith("GROUP BY 1, 2")
    assert counts["default"]["pending"] == (2, 1)
    assert counts["default"]["failed"] == (2, 1)
    assert "mail" not in counts
