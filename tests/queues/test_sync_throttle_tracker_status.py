"""A throttled inline dispatch settles its tracked row with a declared word.

``Bus`` answers a ``JobThrottledException`` on the sync path with a quiet
``None``: a gate refused the run, the body never started, there is no queue
to put it back on. It also recorded that on the tracked row — as the status
``throttled``, which no job model declares. Both products' ``job`` table
carries ``job_status_check`` (pending, processing, success, completed,
cancelled, failed, retrying), so the UPDATE was refused, the database error
left ``Bus.dispatch`` in place of the quiet ``None``, and the row stayed at
``processing`` for good (audit, 2026-11-23).

The model below enforces that same CHECK on ``save``, so an undeclared word
fails here exactly as it does in PostgreSQL.
"""

from __future__ import annotations

import asyncio
import builtins
from typing import Any

import pytest

import cara.facades as facades
from cara.context import ExecutionContext, Tenancy
from cara.queues import Bus
from cara.queues.contracts import BaseJob, JobThrottledException
from cara.queues.tracking import JobTracker, Trackable


class _CheckViolation(Exception):
    """PostgreSQL's answer to a status outside ``job_status_check``."""


class _Row:
    def __init__(self, table: dict[int, dict[str, Any]], data: dict[str, Any]) -> None:
        self.__dict__["_table"] = table
        self.__dict__.update(data)

    def save(self) -> None:
        if self.status not in _JobModel.DECLARED:
            raise _CheckViolation(
                'new row for relation "job" violates check constraint '
                f'"job_status_check" (status={self.status!r})'
            )
        data = {key: value for key, value in vars(self).items() if key != "_table"}
        self._table[self.id] = data


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
    """The tracked-job model surface ``JobTracker`` uses, CHECK included."""

    STATUS_PENDING = "pending"
    STATUS_PROCESSING = "processing"
    STATUS_SUCCESS = "success"
    STATUS_COMPLETED = "completed"
    STATUS_CANCELLED = "cancelled"
    STATUS_FAILED = "failed"
    STATUS_RETRYING = "retrying"
    DECLARED = frozenset(
        {"pending", "processing", "success", "completed", "cancelled", "failed"}
        | {"retrying"}
    )
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


class _RecordingLog:
    """The ``Log`` facade's service: keeps warnings, drops the rest."""

    def __init__(self) -> None:
        self.warnings: list[str] = []

    def warning(self, message: str, *args: Any, **_kwargs: Any) -> None:
        self.warnings.append(message % args if args else message)

    def __getattr__(self, name: str) -> Any:
        return lambda *args, **kwargs: None


class _Application:
    """The container surface the sync path touches: the tracker binding, the
    logger the ``Log`` facade resolves, and ``call`` for the handler."""

    def __init__(self, tracker: Any, log: _RecordingLog) -> None:
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


class _GateShut:
    """A job middleware that refuses the run, as RateLimited / KeyCooldown /
    WithoutOverlapping do."""

    async def handle(self, job: Any, next_fn: Any) -> Any:
        raise JobThrottledException("lock held", key="sweep", retry_after=300)


class _Sweep(Trackable, BaseJob):
    central_job = True
    ran = 0

    def middleware(self) -> list[Any]:
        return [_GateShut()]

    async def handle(self) -> str:
        type(self).ran += 1
        return "ran"


def _install(monkeypatch, tracker: Any) -> _RecordingLog:
    log = _RecordingLog()
    application = _Application(tracker, log)
    monkeypatch.setattr(builtins, "app", lambda: application, raising=False)
    monkeypatch.setattr(facades, "DB", _DB(), raising=False)
    return log


@pytest.fixture(autouse=True)
def _clean_table():
    _JobModel.rows = {}
    _Sweep.ran = 0
    yield
    _JobModel.rows = {}


def _dispatch_inline() -> Any:
    async def _go() -> Any:
        with Tenancy.central(), ExecutionContext.sync():
            return await Bus.dispatch(_Sweep())

    return asyncio.run(_go())


def test_a_throttled_inline_dispatch_is_a_quiet_skip_with_a_declared_status(
    monkeypatch,
):
    _install(monkeypatch, JobTracker(job_model=_JobModel))

    assert _dispatch_inline() is None, "a throttle is a skip, never an error"

    assert _Sweep.ran == 0
    (row,) = _JobModel.rows.values()
    # Not ``throttled`` (undeclared), not ``failed`` (a refusal is not an
    # incident), not ``processing`` (nothing will ever finish this row).
    assert row["status"] == _JobModel.STATUS_CANCELLED
    assert row["status"] in _JobModel.DECLARED
    assert row["cancelled_at"] is not None
    assert row["metadata"]["cancel_reason"] == "throttled: lock held"


def test_the_tracker_never_writes_a_word_the_model_does_not_declare(monkeypatch):
    """Every status a throttled inline run puts on the row is a declared one."""
    written: list[str] = []
    original_save = _Row.save

    def _recording_save(self) -> None:
        written.append(self.status)
        original_save(self)

    monkeypatch.setattr(_Row, "save", _recording_save)
    _install(monkeypatch, JobTracker(job_model=_JobModel))

    _dispatch_inline()

    assert written, "the tracked row was never written"
    assert set(written) <= _JobModel.DECLARED, written


def test_a_tracker_that_cannot_write_never_fails_the_dispatch(monkeypatch):
    """Tracking is observability: a tracker fault (a lost connection, a CHECK
    the app tightened) must leave the dispatch the quiet skip it is."""

    class _BrokenTracker(JobTracker):
        def track_job_throttled(self, job_id: int, reason: str | None = None) -> None:
            raise _CheckViolation("the job table refused the write")

    log = _install(monkeypatch, _BrokenTracker(job_model=_JobModel))

    assert _dispatch_inline() is None

    assert _Sweep.ran == 0
    assert any("Failed to record throttled sync job" in line for line in log.warnings)


def test_a_tracker_without_the_method_never_fails_the_dispatch(monkeypatch):
    """An app-supplied tracker predating ``track_job_throttled`` still skips."""

    class _OldTracker:
        def create_job_record(self, **_kwargs: Any) -> int:
            return 7

        def track_job_started(self, **_kwargs: Any) -> None:
            return None

        def track_job_processing(self, _job_uid: str) -> None:
            return None

    _install(monkeypatch, _OldTracker())

    assert _dispatch_inline() is None
    assert _Sweep.ran == 0
