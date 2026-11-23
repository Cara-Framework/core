"""A cancelled inline (sync-mode) run settles its tracked row; the cancel propagates.

``Bus`` settled an inline run's row on success, on a throttle and on an
``Exception``. ``asyncio.CancelledError`` is a BaseException: when the task
awaiting the inline run was cancelled (a parent job's timeout or shutdown
cancel, a lost lease fence, a dropped request, Ctrl+C) no handler ran and the
row stayed ``processing`` for ever — nothing redelivers an inline run. It is
now ``cancelled`` with ``cancel_reason = "interrupted: CancelledError"``, and
the caller still sees the cancellation.
"""

from __future__ import annotations

import asyncio
import builtins
from typing import Any

import pytest

import cara.facades as facades
from cara.context import ExecutionContext, Tenancy
from cara.queues import Bus
from cara.queues.contracts import BaseJob
from cara.queues.tracking import JobTracker, Trackable


class _Row:
    def __init__(self, table: dict[int, dict[str, Any]], data: dict[str, Any]) -> None:
        self.__dict__["_table"] = table
        self.__dict__.update(data)

    def save(self) -> None:
        self._table[self.id] = {k: v for k, v in vars(self).items() if k != "_table"}


class _Query:
    def __init__(self, table: dict[int, dict[str, Any]], rows: list[dict]) -> None:
        self._table = table
        self._rows = rows

    def where(self, column: str, *args: Any) -> _Query:
        operator, value = ("=", args[0]) if len(args) == 1 else args
        kept = [
            row for row in self._rows if (row.get(column) == value) == (operator == "=")
        ]
        return _Query(self._table, kept)

    def where_in(self, column: str, values: list[Any]) -> _Query:
        return _Query(
            self._table, [row for row in self._rows if row.get(column) in values]
        )

    def first(self) -> _Row | None:
        return _Row(self._table, dict(self._rows[0])) if self._rows else None

    def get(self) -> list[_Row]:
        return [_Row(self._table, dict(row)) for row in self._rows]


class _JobModel:
    STATUS_PENDING = "pending"
    STATUS_PROCESSING = "processing"
    STATUS_SUCCESS = "success"
    STATUS_COMPLETED = "completed"
    STATUS_CANCELLED = "cancelled"
    STATUS_FAILED = "failed"
    STATUS_RETRYING = "retrying"
    rows: dict[int, dict[str, Any]] = {}

    @staticmethod
    def generate_public_id() -> str:
        return "JOB_probe"

    @classmethod
    def create(cls, data: dict[str, Any]) -> _Row:
        row = dict(data, id=len(cls.rows) + 1)
        cls.rows[row["id"]] = row
        return _Row(cls.rows, dict(row))

    @classmethod
    def find(cls, job_id: int) -> _Row | None:
        row = cls.rows.get(job_id)
        return _Row(cls.rows, dict(row)) if row is not None else None

    @classmethod
    def where(cls, *args: Any) -> _Query:
        return _Query(cls.rows, list(cls.rows.values())).where(*args)


class _QuietLog:
    def __init__(self) -> None:
        self.warnings: list[str] = []

    def warning(self, message: str, *args: Any, **_kwargs: Any) -> None:
        self.warnings.append(message % args if args else message)

    def __getattr__(self, name: str) -> Any:
        return lambda *args, **kwargs: None


class _Application:
    def __init__(self, tracker: Any, log: _QuietLog) -> None:
        self._bindings = {"JobTracker": tracker, "logger": log}

    def has(self, name: str) -> bool:
        return name in self._bindings

    def make(self, name: str) -> Any:
        return self._bindings[name]

    def call(self, method: Any, *args: Any) -> Any:
        return method(*args)


class _DB:
    def transaction_level(self) -> int:
        return 0

    def commit_transactions_above(self, level: int) -> None:
        return None

    def rollback_transactions_above(self, level: int) -> None:
        return None


class _SlowJob(Trackable, BaseJob):
    central_job = True
    started: asyncio.Event

    async def handle(self) -> None:
        type(self).started.set()
        await asyncio.Event().wait()  # until cancelled


def _install(monkeypatch, tracker: Any) -> _QuietLog:
    log = _QuietLog()
    application = _Application(tracker, log)
    monkeypatch.setattr(builtins, "app", lambda: application, raising=False)
    monkeypatch.setattr(facades, "DB", _DB(), raising=False)
    return log


@pytest.fixture(autouse=True)
def _clean_table():
    _JobModel.rows = {}
    yield
    _JobModel.rows = {}


def _dispatch_then_cancel() -> bool:
    """Cancel the task awaiting the inline run; whether the caller saw it."""

    async def _go() -> bool:
        _SlowJob.started = asyncio.Event()
        with Tenancy.central(), ExecutionContext.sync():
            task = asyncio.create_task(Bus.dispatch(_SlowJob()))
            await asyncio.wait_for(_SlowJob.started.wait(), timeout=10)
            assert _JobModel.rows[1]["status"] == _JobModel.STATUS_PROCESSING
            task.cancel()
            try:
                await asyncio.wait_for(task, timeout=10)
            except asyncio.CancelledError:
                return True
            return False

    return asyncio.run(_go())


def test_a_cancelled_inline_run_is_settled_cancelled(monkeypatch):
    _install(monkeypatch, JobTracker(job_model=_JobModel))

    assert _dispatch_then_cancel() is True, "the cancellation must reach the caller"

    (row,) = _JobModel.rows.values()
    assert row["status"] == _JobModel.STATUS_CANCELLED  # was ``processing`` for ever
    assert row["cancelled_at"] is not None
    assert row["metadata"]["cancel_reason"] == "interrupted: CancelledError"


def test_a_tracker_that_cannot_write_never_replaces_the_cancellation(monkeypatch):
    class _BrokenTracker(JobTracker):
        def track_job_interrupted(self, job_id: int, reason: str | None = None) -> None:
            raise RuntimeError("the job table refused the write")

    log = _install(monkeypatch, _BrokenTracker(job_model=_JobModel))

    assert _dispatch_then_cancel() is True

    assert any("Failed to record interrupted sync job" in line for line in log.warnings)
