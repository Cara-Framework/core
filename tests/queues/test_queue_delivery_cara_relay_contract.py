"""The relay's contract with its options, its outbox and its gauges."""

from __future__ import annotations

import importlib
import threading
from types import SimpleNamespace

import pendulum

from cara.commands.core.QueueHooksCommand import QueueHooksCommand
from cara.queues.delivery import QueueJobDeliveryStore

_provider = importlib.import_module("cara.queues.QueueProvider")
_hooks = importlib.import_module("cara.commands.core.QueueHooksCommand")


# ── The provider forwards the whole declaration ──────────────────────────────


def _boot_provider(monkeypatch, declared: dict) -> dict:
    """Boot the provider against a config that resolves dotted paths as the real one."""
    captured: dict = {}

    class _Driver:
        driver_name = "amqp"

        def __init__(self, application, options):
            captured.update(options)

    tree = {"queue": {"canonical_queues": ("sync",), "drivers": {"amqp": declared}}}

    def _config(key, default=None):
        node = tree
        for part in key.split("."):
            if not isinstance(node, dict) or part not in node:
                return default
            node = node[part]
        return node

    monkeypatch.setattr(_provider, "config", _config)
    monkeypatch.setattr(_provider, "AMQPDriver", _Driver)
    provider = _provider.QueueProvider.__new__(_provider.QueueProvider)
    provider.application = object()
    provider._add_amqp_driver(SimpleNamespace(add_driver=lambda *_args: None))
    return captured


def test_every_declared_key_reaches_the_driver_named_or_not(monkeypatch):
    declared = {
        "username": "u",
        "password": "p",
        "delivery_broker_window_per_queue": 4,
        "delivery_execution_heartbeat_seconds": 45,
        "a_key_added_after_this_test": "kept",
    }

    forwarded = _boot_provider(monkeypatch, declared)

    for key, value in declared.items():
        assert forwarded[key] == value


def test_an_undeclared_option_still_gets_its_default(monkeypatch):
    forwarded = _boot_provider(monkeypatch, {"username": "u", "password": "p"})

    assert forwarded["delivery_broker_window_per_queue"] == 2
    assert forwarded["delivery_hook_max_attempts"] == 10
    assert forwarded["delivery_table"] == "queue_job_delivery"
    assert forwarded["canonical_queues"] == ["sync"]


# ── Ledger writes ────────────────────────────────────────────────────────────


class _RecordingDB:
    def __init__(self):
        self.statements: list[tuple[str, list]] = []

    def statement(self, sql, params):
        self.statements.append((sql, params))
        return 1

    def select_one(self, _sql, _params):
        return None


class _Application:
    def __init__(self, db):
        self.db = db

    def has(self, key):
        return key == "DB"

    def make(self, key):
        assert key == "DB"
        return self.db


def _store(db) -> QueueJobDeliveryStore:
    return QueueJobDeliveryStore(
        _Application(db), SimpleNamespace(), {"canonical_queues": ("sync",)}
    )


def _assigned(sql: str, params: list) -> dict[str, object]:
    """The columns an ``UPDATE .. SET .. WHERE`` binds, read from its own text."""
    head = sql.split(" SET ", 1)[1].split(" WHERE ", 1)[0]
    values = iter(params)
    assigned: dict[str, object] = {}
    for clause in head.split(", "):
        column, value = clause.split(" = ", 1)
        assigned[column] = next(values) if "%s" in value else value
    return assigned


def test_an_abandoned_execution_goes_back_to_the_publication_outbox():
    """The broker copy may already be acked away: only the relay can restore it."""
    db = _RecordingDB()
    store = _store(db)

    importlib.import_module(
        "cara.queues.delivery._DeliverySettlement"
    )._delivery_abandon_execution(store, "job-1", "lease-1")

    assigned = _assigned(*db.statements[0])
    assert assigned["status"] == store.STATUS_PENDING
    assert assigned["publish_status"] == store.PUBLISH_PENDING
    assert assigned["published_at"] == "NULL"
    assert assigned["publish_lease_token"] == "NULL"


def test_the_relay_waits_out_the_process_that_abandoned_the_run():
    """The releasing process may still run the body until its forced exit."""
    db = _RecordingDB()
    store = _store(db)
    released_at = pendulum.now("UTC")

    importlib.import_module(
        "cara.queues.delivery._DeliverySettlement"
    )._delivery_abandon_execution(store, "job-1", "lease-1")

    hold = _assigned(*db.statements[0])["publish_retry_at"] - released_at
    assert hold.total_seconds() >= store.execution_heartbeat_seconds


def test_a_publish_error_with_no_text_still_leaves_a_reason(monkeypatch):
    released: list[str] = []
    store = _store(_RecordingDB())

    class _SilentError(Exception):
        def __str__(self) -> str:
            return ""

    monkeypatch.setattr(
        store,
        "recover_stale_executions",
        lambda _limit: {"requeued": 0, "reconciled": 0, "unclaimed": 0},
    )
    monkeypatch.setattr(store, "expire_due", lambda _limit: 0)
    monkeypatch.setattr(
        store,
        "_claim_next_publish",
        lambda: ({"job_id": "job-1", "publish_attempts": 0}, "token"),
    )

    def _boom(_row, _token):
        raise _SilentError()

    monkeypatch.setattr(store, "_publish_claimed", _boom)
    monkeypatch.setattr(
        store,
        "_release_publish",
        lambda _job, _token, _attempts, error: released.append(error) or True,
    )

    assert store.publish_due()["retried"] == 1
    assert released == ["_SilentError()"]


# ── The hooks gauge follows the drain ────────────────────────────────────────


def test_the_hook_gauges_refresh_during_a_batch_not_only_after_it(monkeypatch):
    """A batch of children lasts minutes; the gauge must not freeze for it."""
    events: list[str] = []
    driver = SimpleNamespace(
        delivery_store=SimpleNamespace(hook_timeout_seconds=5),
        due_terminal_hook_ids=lambda: ["a", "b", "c"],
        defer_terminal_hook_process_failure=lambda *_args, **_kwargs: "deferred",
        refresh_delivery_metrics=lambda: events.append("refresh") or {},
    )
    monkeypatch.setattr(
        _hooks, "_delivery_settle_callbackless_terminal_hook", lambda *_args: "isolate"
    )

    def _child(argv, **_kwargs):
        events.append(f"child:{argv[-1]}")
        return 0

    monkeypatch.setattr(_hooks, "_run_child", _child)

    result = QueueHooksCommand._run_isolated_hooks(driver, threading.Event())

    assert result["completed"] == 3
    assert events == [
        "refresh",
        "child:a",
        "refresh",
        "child:b",
        "refresh",
        "child:c",
        "refresh",
    ]


def test_a_gauge_scan_that_fails_does_not_stop_the_hooks(monkeypatch):
    """A failing scan before the first child would otherwise run no hook at all."""
    spawned: list[str] = []
    refreshes: list[int] = []

    def _refresh():
        refreshes.append(1)
        # The end-of-batch refresh is the one that reaches the relay loop.
        if len(refreshes) <= 2:
            raise TimeoutError("canceling statement due to statement timeout")
        return {}

    driver = SimpleNamespace(
        delivery_store=SimpleNamespace(hook_timeout_seconds=5),
        due_terminal_hook_ids=lambda: ["a", "b"],
        defer_terminal_hook_process_failure=lambda *_args, **_kwargs: "deferred",
        refresh_delivery_metrics=_refresh,
    )
    monkeypatch.setattr(
        _hooks, "_delivery_settle_callbackless_terminal_hook", lambda *_args: "isolate"
    )
    monkeypatch.setattr(
        _hooks, "_run_child", lambda argv, **_kwargs: spawned.append(argv[-1]) or 0
    )

    result = QueueHooksCommand._run_isolated_hooks(driver, threading.Event())

    assert spawned == ["a", "b"]
    assert result["completed"] == 2


def test_a_stopping_hook_runner_does_not_scan_the_ledger(monkeypatch):
    stop = threading.Event()
    stop.set()
    refreshes: list[int] = []
    driver = SimpleNamespace(
        delivery_store=SimpleNamespace(hook_timeout_seconds=5),
        due_terminal_hook_ids=lambda: ["a"],
        refresh_delivery_metrics=lambda: refreshes.append(1) or {},
    )

    QueueHooksCommand._run_isolated_hooks(driver, stop)

    assert refreshes == []
