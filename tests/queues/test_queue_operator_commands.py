"""queue:monitor / queue:retry / queue:purge / queue:flush — operator CLI.

No database and no broker. The REAL :class:`QueueOperationsStore` runs against
a fake ``DB`` module global rebound with ``monkeypatch.setattr`` (rebinding the
module name, never patching a model attribute — that can trigger application
boot). Commands are instantiated directly and ``handle()`` is called with an
explicit ``store=`` / ``application=``.

What is pinned:

(a) monitor — counts every tracker status through the real store, and a
    missing/unreachable broker degrades to a printed line rather than failing
    the command (``application=None`` makes the probe raise, which the
    best-effort probe swallows). The dead-letter figure comes from the LEDGER,
    scoped per queue — not from failed tracker rows.
(b) retry — the driver seam is called with ``(job_id, operator=, reason=)`` and
    nothing else: no payload mutation and no tracker writes. Non-terminal
    ledger rows never reach the driver, and a per-row ``QueueException`` does
    not abort the batch.
(c) purge — cutoff math, refusal without confirmation unless ``--force``,
    terminal-only ledger deletion, and the FK-freed re-query: a tracker job
    blocked by its OWN ledger row becomes purgeable in the SAME run once that
    ledger row is gone.
(d) flush — refuses without ``--force``, refuses in production, purges exactly
    the driver's canonical inventory plus the dead-letter queue, and a missing
    queue (404) does not abort the sweep.
"""

from __future__ import annotations

import importlib
from types import SimpleNamespace

import pendulum
import pika
import pytest

from cara.commands.core.QueueFlushCommand import QueueFlushCommand
from cara.commands.core.QueueMonitorCommand import QueueMonitorCommand
from cara.commands.core.QueuePurgeCommand import QueuePurgeCommand
from cara.commands.core.QueueRetryCommand import QueueRetryCommand
from cara.exceptions import QueueException
from cara.queues.Topology import DEAD_LETTER_QUEUE

_STORE_MODULE = "cara.queues.delivery.QueueOperationsStore"


class _FakeDB:
    """Mirrors the exact query shapes ``QueueOperationsStore`` issues.

    Deliberately not a SQL parser: it dispatches on the distinctive marker of
    each statement, which keeps the fake honest about binding ORDER without
    pretending to be a database.
    """

    def __init__(self, *, jobs: list[dict], deliveries: list[dict]) -> None:
        self.jobs = [dict(row) for row in jobs]
        self.deliveries = [dict(row) for row in deliveries]
        self.deleted_jobs: list[int] = []
        self.calls: list[tuple[str, str, list]] = []

    # ── helpers ──────────────────────────────────────────────────────
    def _unreferenced_jobs(self) -> list[dict]:
        referenced = {row["db_job_id"] for row in self.deliveries}
        return [row for row in self.jobs if row["id"] not in referenced]

    def _terminal(self, statuses: set[str]) -> list[dict]:
        return [row for row in self.deliveries if row["status"] in statuses]

    def _without_replay_children(self, rows: list[dict]) -> list[dict]:
        replayed = {row["replay_of"] for row in self.deliveries if row.get("replay_of")}
        return [row for row in rows if row["job_id"] not in replayed]

    # ── DB facade surface ────────────────────────────────────────────
    def select(self, query: str, bindings=()) -> list[dict]:
        bindings = list(bindings)
        self.calls.append(("select", query, bindings))
        if " FROM job " in query:
            if "SELECT DISTINCT" in query:
                rows = [row for row in self.jobs if row["created_at"] >= bindings[0]]
                return [{"queue": row["queue"] or "default"} for row in rows]
            if "created_at < %s" in query:
                status, cutoff = bindings[0], bindings[1]
                rows = [
                    row
                    for row in self._unreferenced_jobs()
                    if row["status"] == status and row["created_at"] < cutoff
                ]
                if "AND queue = %s" in query:
                    rows = [row for row in rows if row["queue"] == bindings[2]]
                return sorted(rows, key=lambda row: row["created_at"])
            queue, status, cutoff, limit = bindings
            rows = [
                row
                for row in self.jobs
                if row["queue"] == queue
                and row["status"] == status
                and row["created_at"] >= cutoff
            ]
            rows = sorted(rows, key=lambda row: row["created_at"], reverse=True)
            return rows[: int(limit)]

        rows = self._terminal({bindings[0], bindings[1]})
        if "AND queue = %s" in query:
            rows = [row for row in rows if row["queue"] == bindings[2]]
        rows = sorted(rows, key=lambda row: row["updated_at"])
        return rows[: int(bindings[-1])]

    def select_one(self, query: str, bindings=()) -> dict | None:
        bindings = list(bindings)
        self.calls.append(("select_one", query, bindings))
        if " FROM job " in query:
            rows = list(self.jobs)
            index = 0
            if "AND queue = %s" in query:
                rows = [row for row in rows if row["queue"] == bindings[index]]
                index += 1
            if "AND status = %s" in query:
                rows = [row for row in rows if row["status"] == bindings[index]]
                index += 1
            if "AND created_at >= %s" in query:
                rows = [row for row in rows if row["created_at"] >= bindings[index]]
            return {"total": len(rows)}

        if "WHERE job_id = %s" in query:
            job_id, status_a, status_b = bindings
            for row in self.deliveries:
                if row["job_id"] == job_id and row["status"] in {status_a, status_b}:
                    return dict(row)
            return None

        rows = self._terminal({bindings[0], bindings[1]})
        index = 2
        if "updated_at < %s" in query:
            rows = [row for row in rows if row["updated_at"] < bindings[index]]
            index += 1
        if "NOT EXISTS" in query:
            rows = self._without_replay_children(rows)
        if "AND queue = %s" in query:
            rows = [row for row in rows if row["queue"] == bindings[index]]
        return {"total": len(rows)}

    def statement(self, query: str, bindings=()) -> int:
        bindings = list(bindings)
        self.calls.append(("statement", query, bindings))
        if query.strip().upper().startswith("DELETE FROM JOB"):
            job_id = int(bindings[0])
            before = len(self.jobs)
            self.jobs = [row for row in self.jobs if row["id"] != job_id]
            if before != len(self.jobs):
                self.deleted_jobs.append(job_id)
            return before - len(self.jobs)

        assert query.strip().upper().startswith("DELETE FROM QUEUE_JOB_DELIVERY")
        rows = self._terminal({bindings[0], bindings[1]})
        rows = [row for row in rows if row["updated_at"] < bindings[2]]
        rows = self._without_replay_children(rows)
        if "AND queue = %s" in query:
            rows = [row for row in rows if row["queue"] == bindings[3]]
        matched = {row["job_id"] for row in rows}
        self.deliveries = [row for row in self.deliveries if row["job_id"] not in matched]
        return len(matched)


def _job(row_id: int, *, queue: str, status: str, created_at, error=None) -> dict:
    return {
        "id": row_id,
        "public_id": f"JOB{row_id}",
        "name": f"Job{row_id}",
        "queue": queue,
        "status": status,
        "error": error,
        "created_at": created_at,
    }


def _delivery(
    job_id: str,
    *,
    db_job_id: int,
    queue: str,
    status: str,
    updated_at,
    replay_of: str | None = None,
) -> dict:
    return {
        "job_id": job_id,
        "db_job_id": db_job_id,
        "queue": queue,
        "status": status,
        "terminal_reason": None,
        "updated_at": updated_at,
        "replay_of": replay_of,
    }


def _store(monkeypatch: pytest.MonkeyPatch, *, jobs=None, deliveries=None):
    module = importlib.import_module(_STORE_MODULE)
    fake = _FakeDB(jobs=list(jobs or []), deliveries=list(deliveries or []))
    monkeypatch.setattr(module, "DB", fake)
    return module.QueueOperationsStore(), fake


def _driver_application(driver) -> SimpleNamespace:
    return SimpleNamespace(
        make=lambda _name: SimpleNamespace(driver=lambda _amqp: driver)
    )


# ── (a) queue:monitor ────────────────────────────────────────────────────
def test_monitor_counts_every_status_and_degrades_without_a_broker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = pendulum.now("UTC")
    jobs = [
        _job(1, queue="alpha", status="pending", created_at=now),
        _job(2, queue="alpha", status="processing", created_at=now),
        _job(3, queue="alpha", status="success", created_at=now),
        _job(4, queue="alpha", status="failed", created_at=now, error="ValueError: x"),
        _job(5, queue="alpha", status="retrying", created_at=now),
    ]
    store, _db = _store(monkeypatch, jobs=jobs)

    command = QueueMonitorCommand(application=None)
    assert command.handle(queue=None, limit=20, store=store) == 0

    assert store.count_jobs(queue="alpha", status="pending") == 1
    assert store.count_jobs(queue="alpha", status="processing") == 1
    assert store.count_jobs(queue="alpha", status="success") == 1
    assert store.count_jobs(queue="alpha", status="failed") == 1
    assert store.count_jobs(queue="alpha", status="retrying") == 1
    # No application -> the probe raises -> swallowed, never failing the command.
    assert command._broker_dead_letter_depth() is None


def test_monitor_dead_letter_figure_comes_from_the_ledger_not_failed_tracker_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = pendulum.now("UTC")
    jobs = [_job(1, queue="alpha", status="failed", created_at=now)]
    deliveries = [
        _delivery(
            "d-a", db_job_id=1, queue="alpha", status="dead_lettered", updated_at=now
        ),
        _delivery(
            "d-b", db_job_id=2, queue="beta", status="dead_lettered", updated_at=now
        ),
    ]
    store, _db = _store(monkeypatch, jobs=jobs, deliveries=deliveries)

    command = QueueMonitorCommand(application=None)
    assert command.handle(queue="alpha", limit=20, store=store) == 0

    assert store.count_dead_lettered(queue="alpha") == 1
    assert store.count_dead_lettered(queue=None) == 2


def test_monitor_broker_probe_reads_the_dead_letter_queue_when_reachable() -> None:
    class _FakeChannel:
        def queue_declare(self, *, queue: str, passive: bool):
            assert queue == DEAD_LETTER_QUEUE
            assert passive is True
            return SimpleNamespace(method=SimpleNamespace(message_count=3))

        def close(self) -> None:
            pass

    class _FakeConnection:
        def close(self) -> None:
            pass

    class _FakeDriver:
        def open_topology_connection(self):
            return _FakeConnection(), _FakeChannel()

    command = QueueMonitorCommand(application=_driver_application(_FakeDriver()))
    assert command._broker_dead_letter_depth() == 3


# ── (b) queue:retry ──────────────────────────────────────────────────────
def test_retry_replays_only_dead_lettered_or_expired_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = pendulum.now("UTC")
    deliveries = [
        _delivery(
            "job-a",
            db_job_id=1,
            queue="alpha",
            status="dead_lettered",
            updated_at=now.subtract(hours=2),
        ),
        _delivery(
            "job-b",
            db_job_id=2,
            queue="alpha",
            status="expired",
            updated_at=now.subtract(hours=1),
        ),
        _delivery(
            "job-c", db_job_id=3, queue="alpha", status="completed", updated_at=now
        ),
    ]
    store, db = _store(monkeypatch, deliveries=deliveries)

    replay_calls: list[tuple[str, dict]] = []

    def _replay(job_id: str, *, operator: str, reason: str) -> str:
        replay_calls.append((job_id, {"operator": operator, "reason": reason}))
        return f"replay-{job_id}"

    driver = SimpleNamespace(replay_delivery=_replay)
    command = QueueRetryCommand(application=_driver_application(driver))

    assert command.handle(all=True, limit=50, reason="test reason", store=store) == 0
    assert {job_id for job_id, _kwargs in replay_calls} == {"job-a", "job-b"}
    for _job_id, kwargs in replay_calls:
        assert set(kwargs) == {"operator", "reason"}
        assert kwargs["reason"] == "test reason"
        assert kwargs["operator"].startswith("cli:")
    # Replay is the only side effect: nothing was written or deleted.
    assert db.deleted_jobs == []
    assert not any(call[0] == "statement" for call in db.calls)


def test_retry_by_job_id_passes_job_id_operator_reason_and_nothing_else(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = pendulum.now("UTC")
    deliveries = [
        _delivery(
            "job-a", db_job_id=1, queue="alpha", status="dead_lettered", updated_at=now
        ),
    ]
    store, _db = _store(monkeypatch, deliveries=deliveries)

    calls: list[tuple[tuple, dict]] = []

    def _replay(*args, **kwargs) -> str:
        calls.append((args, kwargs))
        return "replay-child-1"

    driver = SimpleNamespace(replay_delivery=_replay)
    command = QueueRetryCommand(application=_driver_application(driver))

    assert command.handle(job_id="job-a", reason="manual queue:retry", store=store) == 0
    assert len(calls) == 1
    args, kwargs = calls[0]
    assert args == ("job-a",)
    assert set(kwargs) == {"operator", "reason"}
    assert kwargs["reason"] == "manual queue:retry"
    assert kwargs["operator"].startswith("cli:")


def test_retry_reports_a_per_row_queue_exception_without_aborting_the_batch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = pendulum.now("UTC")
    deliveries = [
        _delivery(
            "job-a",
            db_job_id=1,
            queue="alpha",
            status="dead_lettered",
            updated_at=now.subtract(hours=1),
        ),
        _delivery(
            "job-b", db_job_id=2, queue="alpha", status="dead_lettered", updated_at=now
        ),
    ]
    store, _db = _store(monkeypatch, deliveries=deliveries)

    def _replay(job_id: str, *, operator: str, reason: str) -> str:
        if job_id == "job-a":
            raise QueueException("Replay child is terminal (dead_lettered).")
        return f"replay-{job_id}"

    driver = SimpleNamespace(replay_delivery=_replay)
    command = QueueRetryCommand(application=_driver_application(driver))

    assert command.handle(all=True, store=store) == 0


def test_retry_requires_at_least_one_selector() -> None:
    command = QueueRetryCommand(application=None)
    assert command.handle() == 1


# ── (c) queue:purge ──────────────────────────────────────────────────────
def test_purge_cutoff_math_confirmation_and_terminal_only_deletion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = pendulum.now("UTC")
    jobs = [
        _job(1, queue="alpha", status="failed", created_at=now.subtract(hours=48)),
        _job(2, queue="alpha", status="failed", created_at=now.subtract(hours=1)),
    ]
    deliveries = [
        _delivery(
            "ledger-old",
            db_job_id=99,
            queue="alpha",
            status="dead_lettered",
            updated_at=now.subtract(hours=48),
        ),
        _delivery(
            "ledger-recent",
            db_job_id=98,
            queue="alpha",
            status="expired",
            updated_at=now.subtract(hours=1),
        ),
    ]
    store, db = _store(monkeypatch, jobs=jobs, deliveries=deliveries)
    command = QueuePurgeCommand(application=None)

    monkeypatch.setattr(command, "confirm", lambda *_a, **_k: False)
    assert command.handle(older_than=24, force=False, store=store) == 0
    assert db.deleted_jobs == []
    assert len(db.deliveries) == 2

    assert command.handle(older_than=24, force=True, store=store) == 0
    assert db.deleted_jobs == [1]
    assert [row["job_id"] for row in db.deliveries] == ["ledger-recent"]


def test_purge_frees_a_tracker_job_blocked_by_its_own_ledger_row_in_the_same_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = pendulum.now("UTC")
    jobs = [_job(1, queue="alpha", status="failed", created_at=now.subtract(hours=48))]
    deliveries = [
        _delivery(
            "ledger-x",
            db_job_id=1,
            queue="alpha",
            status="dead_lettered",
            updated_at=now.subtract(hours=48),
        ),
    ]
    store, db = _store(monkeypatch, jobs=jobs, deliveries=deliveries)
    command = QueuePurgeCommand(application=None)

    assert command.handle(older_than=24, force=True, store=store) == 0
    # Not eligible in the pre-delete preview (db_job_id FK), but the ledger
    # delete frees it for the same-run re-query.
    assert db.deleted_jobs == [1]
    assert db.deliveries == []


def test_purge_broker_flag_empties_the_dead_letter_queue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, _db = _store(monkeypatch)
    purge_calls: list[str] = []

    class _FakeChannel:
        def queue_purge(self, queue: str):
            purge_calls.append(queue)
            return SimpleNamespace(method=SimpleNamespace(message_count=7))

        def close(self) -> None:
            pass

    class _FakeConnection:
        is_open = True

        def close(self) -> None:
            pass

    class _FakeDriver:
        def open_topology_connection(self):
            return _FakeConnection(), _FakeChannel()

    command = QueuePurgeCommand(application=_driver_application(_FakeDriver()))
    assert command.handle(older_than=24, broker=True, force=True, store=store) == 0
    assert purge_calls == [DEAD_LETTER_QUEUE]


def test_purge_nothing_older_than_cutoff_is_a_no_op(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = pendulum.now("UTC")
    store, db = _store(
        monkeypatch,
        jobs=[_job(1, queue="alpha", status="failed", created_at=now)],
    )
    command = QueuePurgeCommand(application=None)

    assert command.handle(older_than=24, force=False, store=store) == 0
    assert db.deleted_jobs == []


# ── (d) queue:flush ──────────────────────────────────────────────────────
def test_flush_refuses_without_force(monkeypatch: pytest.MonkeyPatch) -> None:
    command = QueueFlushCommand(application=None)
    monkeypatch.setattr(command, "_is_production", lambda: False)
    assert command.handle(force=False) == 0


def test_flush_refuses_in_production(monkeypatch: pytest.MonkeyPatch) -> None:
    command = QueueFlushCommand(application=None)
    monkeypatch.setattr(command, "_is_production", lambda: True)
    assert command.handle(force=True) == 1


def test_flush_targets_are_the_driver_canonical_inventory_plus_dead_letter_queue() -> (
    None
):
    driver = SimpleNamespace(canonical_queues=frozenset({"alpha", "beta"}))
    assert QueueFlushCommand.purge_targets(driver) == frozenset(
        {"alpha", "beta", DEAD_LETTER_QUEUE}
    )


def test_flush_purges_every_target_and_survives_a_missing_queue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    purge_calls: list[str] = []

    class _FakeChannel:
        def __init__(self) -> None:
            self.is_open = True

        def queue_purge(self, queue: str):
            purge_calls.append(queue)
            if queue == "beta":
                self.is_open = False
                raise pika.exceptions.ChannelClosedByBroker(404, "NOT_FOUND")

        def close(self) -> None:
            self.is_open = False

    class _FakeConnection:
        def __init__(self) -> None:
            self.is_open = True

        def channel(self) -> _FakeChannel:
            return _FakeChannel()

        def close(self) -> None:
            self.is_open = False

    connection = _FakeConnection()
    first_channel = connection.channel()

    class _FakeDriver:
        canonical_queues = frozenset({"alpha", "beta"})

        def open_topology_connection(self):
            return connection, first_channel

    command = QueueFlushCommand(application=_driver_application(_FakeDriver()))
    monkeypatch.setattr(command, "_is_production", lambda: False)

    assert command.handle(force=True) == 0
    assert set(purge_calls) == QueueFlushCommand.purge_targets(_FakeDriver())
