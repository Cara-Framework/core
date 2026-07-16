"""Regression pins for after-commit job dispatch.

A job pushed from inside ``with DB.transaction():`` reaches the broker
immediately — a worker can consume it before the transaction commits
(the rows it needs don't exist yet), and a rollback leaves a ghost job
running against undone work. ``PendingDispatch.after_commit()`` and the
``ShouldDispatchAfterCommit`` marker route the push through
``DB.after_commit``: fired right after the outermost commit, discarded
on rollback, immediate when no transaction is open.
"""

from __future__ import annotations

import builtins
import gc
from types import SimpleNamespace

import pytest

from cara.eloquent import DatabaseManager
from cara.eloquent.connections.ConnectionResolver import (
    ConnectionResolver,
    _get_after_commit_registry,
    _get_registry,
)
from cara.queues.contracts import PendingDispatch, Queueable, ShouldDispatchAfterCommit


class _FakeConnection:
    def __init__(self):
        self.transaction_level = 0
        self.open = 1

    def begin(self):
        self.transaction_level += 1
        return self

    def commit(self):
        if self.transaction_level > 0:
            self.transaction_level -= 1
        return self

    def rollback(self):
        if self.transaction_level > 0:
            self.transaction_level -= 1
        return self

    def close_connection(self):
        pass


class _PlainJob:
    queue = "default"


class _AfterCommitJob(ShouldDispatchAfterCommit):
    queue = "default"


class _QueueableJob(Queueable):
    def __init__(self, item_id: int):
        super().__init__()
        self.item_id = item_id


class _RecordingDispatch(PendingDispatch):
    """PendingDispatch with the broker push stubbed to a recorder."""

    pushed: list

    def _push(self):  # noqa: D102 — test stub
        self.pushed.append(self.job)
        return "job-id-1"


@pytest.fixture
def harness(monkeypatch):
    """Wire DatabaseManager.after_commit at a resolver with a fake conn."""
    _get_registry().clear()
    _get_after_commit_registry().clear()

    conn = _FakeConnection()
    resolver = ConnectionResolver(database_manager=None)
    resolver._create_connection_instance = lambda name: conn

    dm = DatabaseManager.get_instance()
    monkeypatch.setattr(dm, "_resolve_connection_name", lambda c=None: "app")
    monkeypatch.setattr(dm, "_ensure_resolver", lambda: resolver)
    resolver.queue_driver = SimpleNamespace(
        durable_transactional_outbox=False
    )
    queue_service = SimpleNamespace(
        driver=lambda *_args, **_kwargs: resolver.queue_driver
    )
    monkeypatch.setattr(
        builtins,
        "app",
        lambda: SimpleNamespace(
            make=lambda key: (
                queue_service
                if key == "queue"
                else (_ for _ in ()).throw(KeyError(key))
            )
        ),
        raising=False,
    )

    yield resolver

    _get_registry().clear()
    _get_after_commit_registry().clear()


def _pending(job) -> _RecordingDispatch:
    dispatch = _RecordingDispatch(job)
    dispatch.pushed = []
    return dispatch


class TestAfterCommitDispatch:
    def test_marker_defers_push_until_commit(self, harness):
        dispatch = _pending(_AfterCommitJob())

        with harness.transaction("app"):
            result = dispatch.dispatch()
            assert result is None  # deferred — no job id yet
            assert dispatch.pushed == []  # nothing hit the broker

        assert len(dispatch.pushed) == 1  # pushed right after commit

    def test_fluent_after_commit_defers_push(self, harness):
        dispatch = _pending(_PlainJob()).after_commit()

        with harness.transaction("app"):
            dispatch.dispatch()
            assert dispatch.pushed == []

        assert len(dispatch.pushed) == 1

    def test_rollback_discards_the_push(self, harness):
        dispatch = _pending(_AfterCommitJob())

        with pytest.raises(RuntimeError), harness.transaction("app"):
            dispatch.dispatch()
            raise RuntimeError("boom")

        assert dispatch.pushed == []  # ghost job never queued

    def test_no_transaction_pushes_immediately(self, harness):
        dispatch = _pending(_AfterCommitJob())

        result = dispatch.dispatch()

        assert result == "job-id-1"
        assert len(dispatch.pushed) == 1

    def test_plain_job_still_pushes_inside_transaction(self, harness):
        dispatch = _pending(_PlainJob())

        with harness.transaction("app"):
            result = dispatch.dispatch()
            assert result == "job-id-1"
            assert len(dispatch.pushed) == 1  # unchanged default behavior

    def test_deferred_dispatch_is_idempotent(self, harness):
        dispatch = _pending(_AfterCommitJob())

        with harness.transaction("app"):
            dispatch.dispatch()
            dispatch.dispatch()

        assert len(dispatch.pushed) == 1

    def test_durable_outbox_registers_inside_the_open_transaction(
        self,
        harness,
    ):
        harness.queue_driver = SimpleNamespace(
            durable_transactional_outbox=True
        )
        dispatch = _pending(_AfterCommitJob())

        with harness.transaction("app"):
            result = dispatch.dispatch()
            assert result == "job-id-1"
            assert len(dispatch.pushed) == 1

        assert len(dispatch.pushed) == 1

    def test_send_alias_is_idempotent(self, harness):
        dispatch = _pending(_PlainJob())

        assert dispatch.send() == "job-id-1"
        assert dispatch.send() == "job-id-1"
        assert len(dispatch.pushed) == 1

    def test_garbage_collection_never_dispatches_implicitly(self, harness):
        pushed = []
        dispatch = _RecordingDispatch(_PlainJob())
        dispatch.pushed = pushed

        del dispatch
        gc.collect()

        assert pushed == []

    def test_dispatch_after_is_an_explicit_terminal_api(self, harness, monkeypatch):
        pushed = []

        def fake_push(dispatch):
            pushed.append((dispatch.job.item_id, dispatch._delay))
            return "job-id-delayed"

        monkeypatch.setattr(PendingDispatch, "_push", fake_push)

        assert _QueueableJob.dispatch_after(30, 7) == "job-id-delayed"
        assert pushed == [(7, 30)]
