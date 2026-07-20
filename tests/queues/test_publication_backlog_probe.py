"""The `queue:work` / `queue:relay` discoverability trap — 2026-07-20.

An operator started ``queue:work``, reasonably believed "the queue is
being processed", and 1250 dispatched jobs sat unpublished in the outbox
for hours because ``queue:relay`` — the only publisher — was never
started. Nothing said a word.

These tests pin the human-facing half of the fix:

* a worker that starts into an aged, undrained outbox says so, LOUDLY;
* it says so without ever being able to stop the worker from starting;
* ordinary bursts stay quiet, or the banner becomes noise and we have
  rebuilt the original silence with extra steps;
* the ledger being absent entirely (cheapa deploys no outbox) is silence,
  not a crash;
* each command's own ``--help`` names the OTHER processes it needs.
"""

from __future__ import annotations

import importlib
from types import SimpleNamespace

import pytest

from cara.commands.core.QueueRelayCommand import (
    QueueHooksCommand,
    QueueRelayCommand,
)
from cara.commands.core.QueueWorkCommand import QueueWorkCommand
from cara.commands.core.ScheduleWorkCommand import ScheduleWorkCommand
from cara.queues.delivery import (
    PublicationBacklogProbe,
    QueueJobDeliveryStore,
)

_BUDGET = 300
_MIN = 1


def _probe_module():
    """The submodule, not the class the package barrel re-exports.

    ``import cara.queues.delivery.PublicationBacklogProbe as module`` binds
    the CLASS here, because the barrel's re-export shadows the submodule
    attribute of the same name. ``importlib`` reads ``sys.modules`` and so
    always returns the module — the difference is invisible until a
    monkeypatch silently lands on the wrong object.
    """
    return importlib.import_module(
        "cara.queues.delivery.PublicationBacklogProbe"
    )


def _advisory(snapshot, *, budget=_BUDGET, minimum=_MIN):
    return PublicationBacklogProbe.advisory(
        snapshot,
        age_budget_seconds=budget,
        min_pending=minimum,
    )


# ── the verdict: age gates, count does not ───────────────────────────


def test_the_incident_shape_is_reported() -> None:
    message = _advisory({"count": 1250, "age": 4210.0})

    assert message is not None
    # The operator must be told WHICH command they are missing. A banner
    # that says "backlog" without naming queue:relay leaves them exactly
    # where the incident left them.
    assert "queue:relay" in message
    assert "1250" in message
    assert "4210" in message


def test_a_large_fresh_burst_says_nothing() -> None:
    """5000 rows dispatched seconds ago is healthy traffic, not an outage."""
    assert _advisory({"count": 5000, "age": 0.0}) is None
    assert _advisory({"count": 5000, "age": 299.0}) is None


def test_the_age_gate_opens_exactly_at_the_budget() -> None:
    assert _advisory({"count": 1, "age": 299.999}) is None
    assert _advisory({"count": 1, "age": 300.0}) is not None


def test_the_count_gate_can_tolerate_a_chronic_tail() -> None:
    assert _advisory({"count": 9, "age": 99999.0}, minimum=10) is None
    assert _advisory({"count": 10, "age": 99999.0}, minimum=10) is not None


def test_an_empty_or_absent_snapshot_says_nothing() -> None:
    assert _advisory(None) is None
    assert _advisory({}) is None
    assert _advisory({"count": 0, "age": 0.0}) is None


def test_a_garbage_snapshot_says_nothing_rather_than_raising() -> None:
    assert _advisory({"count": "many", "age": "old"}) is None


def test_the_budget_is_reported_so_the_number_can_be_argued_with() -> None:
    message = _advisory({"count": 5, "age": 900.0}, budget=600)
    assert "600" in message


# ── thresholds are shared with the continuous scheduler-side alarm ───


def test_thresholds_come_from_the_same_config_keys_as_the_watchdog() -> None:
    """One definition of "stalled" for the startup and continuous surfaces.

    ``app.support.QueueOutboxHealth`` reads these exact keys. If this
    probe grew its own knobs the two surfaces could disagree about
    whether the system is healthy, which is its own kind of silence.
    """
    assert (
        PublicationBacklogProbe.AGE_BUDGET_CONFIG_KEY
        == "queue.outbox_stall_age_seconds"
    )
    assert (
        PublicationBacklogProbe.MIN_PENDING_CONFIG_KEY
        == "queue.outbox_stall_min_pending"
    )


def test_thresholds_read_config_rather_than_hardcoding(monkeypatch) -> None:
    module = _probe_module()

    monkeypatch.setattr(
        module,
        "config",
        lambda key, default=None: {
            "queue.outbox_stall_age_seconds": 42,
            "queue.outbox_stall_min_pending": 7,
        }.get(key, default),
    )
    assert PublicationBacklogProbe.age_budget_seconds() == 42
    assert PublicationBacklogProbe.min_pending() == 7


def test_thresholds_are_floored_so_a_zero_cannot_alarm_on_everything(
    monkeypatch,
) -> None:
    module = _probe_module()

    monkeypatch.setattr(module, "config", lambda _key, _default=None: 0)
    assert PublicationBacklogProbe.age_budget_seconds() >= 1
    assert PublicationBacklogProbe.min_pending() >= 1


# ── the ledger may not exist at all (cheapa deploys no outbox) ───────


class _FakeDB:
    def __init__(self, *, present, backlog=None, explode=False):
        self.present = present
        self.backlog = backlog or {"count": 0, "age": 0.0}
        self.explode = explode
        self.queries: list[str] = []

    def select_one(self, query, bindings=None):
        self.queries.append(query)
        if self.explode:
            raise RuntimeError('relation "queue_job_delivery" does not exist')
        if "to_regclass" in query:
            return {"present": self.present}
        return {"count": self.backlog["count"], "age": self.backlog["age"]}


def _store(db) -> QueueJobDeliveryStore:
    application = SimpleNamespace(
        has=lambda key: key == "DB",
        make=lambda key: db,
    )
    return QueueJobDeliveryStore(
        application=application,
        driver=None,
        options={"canonical_queues": ("sync",)},
    )


def test_a_missing_ledger_is_reported_as_none_not_an_exception() -> None:
    """cheapa runs `queue:work` against a database with no outbox table.

    A probe that raised UndefinedTable here would take out every worker
    start in that product — trading one silent failure for a loud one in
    a codebase that never had the bug.
    """
    db = _FakeDB(present=False)
    assert _store(db).backlog_metrics_if_installed() is None
    # It must not have gone on to query the missing table.
    assert len(db.queries) == 1
    assert "to_regclass" in db.queries[0]


def test_an_installed_ledger_is_measured_with_the_relay_predicate() -> None:
    db = _FakeDB(present=True, backlog={"count": 1250, "age": 4210.0})
    assert _store(db).backlog_metrics_if_installed() == {
        "count": 1250,
        "age": 4210.0,
    }
    backlog_query = db.queries[-1]
    # Same definition of "due" the relay itself publishes on — this probe
    # must never be able to disagree with the publisher about what is
    # outstanding.
    assert "publish_status != %s" in backlog_query
    assert "available_at <= NOW()" in backlog_query


def test_a_freshly_migrated_empty_ledger_is_silent() -> None:
    """The table exists but has never held a row — a brand-new database.

    ``MIN(available_at)`` over zero rows is NULL, so the age COALESCEs to
    0 and the probe must read that as "healthy", not as "unknown" or a
    crash. This is the state right after `craft migrate` on an empty
    schema, which is exactly when a developer first types `queue:work`.
    """
    db = _FakeDB(present=True, backlog={"count": 0, "age": 0.0})
    assert _store(db).backlog_metrics_if_installed() == {
        "count": 0,
        "age": 0.0,
    }
    assert _advisory({"count": 0, "age": 0.0}) is None


def test_a_row_less_aggregate_response_does_not_crash() -> None:
    """Defensive: a driver that answers None instead of a zero row."""

    class _NullDB:
        def select_one(self, query, bindings=None):
            return {"present": True} if "to_regclass" in query else None

    assert _store(_NullDB()).backlog_metrics_if_installed() == {
        "count": 0,
        "age": 0.0,
    }


def test_an_unmigrated_database_is_silent_not_fatal() -> None:
    """No schema at all yet — `queue:work` before the first `craft migrate`."""

    class _EmptySchemaDB:
        def select_one(self, query, bindings=None):
            assert "to_regclass" in query, "must not touch the missing table"
            return {"present": False}

    assert _store(_EmptySchemaDB()).backlog_metrics_if_installed() is None


def test_existence_is_checked_without_a_throwing_select() -> None:
    """``to_regclass`` returns NULL for a missing relation.

    A plain ``SELECT ... FROM missing_table`` would raise AND abort any
    surrounding PostgreSQL transaction, so catching the error is not an
    equivalent implementation.
    """
    db = _FakeDB(present=False)
    _store(db).backlog_metrics_if_installed()
    assert "to_regclass" in db.queries[0]


def test_the_relays_own_backlog_read_still_fails_loud() -> None:
    """If the ledger vanishes under the PUBLISHER that is a real outage."""
    db = _FakeDB(present=True, explode=True)
    with pytest.raises(RuntimeError):
        _store(db).backlog_metrics()


# ── announce(): never fatal, always audible ──────────────────────────


def test_announce_emits_to_the_operator_channel(monkeypatch) -> None:
    monkeypatch.setattr(
        PublicationBacklogProbe,
        "sample",
        staticmethod(lambda: {"count": 1250, "age": 4210.0}),
    )
    seen: list[str] = []
    message = PublicationBacklogProbe.announce(emit=seen.append)

    assert message is not None
    assert seen == [message]


def test_announce_stays_quiet_when_the_outbox_is_draining(monkeypatch) -> None:
    monkeypatch.setattr(
        PublicationBacklogProbe,
        "sample",
        staticmethod(lambda: {"count": 3, "age": 1.0}),
    )
    seen: list[str] = []
    assert PublicationBacklogProbe.announce(emit=seen.append) is None
    assert seen == []


def test_a_broken_sample_is_swallowed(monkeypatch) -> None:
    """The DB being unreachable must not stop a worker from starting."""

    def _boom():
        raise RuntimeError("database is down")

    monkeypatch.setattr(
        PublicationBacklogProbe, "sample", staticmethod(_boom)
    )
    assert PublicationBacklogProbe.announce(emit=lambda _m: None) is None


def test_a_broken_console_still_returns_the_verdict(monkeypatch) -> None:
    monkeypatch.setattr(
        PublicationBacklogProbe,
        "sample",
        staticmethod(lambda: {"count": 1250, "age": 4210.0}),
    )

    def _broken(_message):
        raise RuntimeError("console is gone")

    assert PublicationBacklogProbe.announce(emit=_broken) is not None


def test_announce_works_without_any_console(monkeypatch) -> None:
    monkeypatch.setattr(
        PublicationBacklogProbe,
        "sample",
        staticmethod(lambda: {"count": 1250, "age": 4210.0}),
    )
    assert PublicationBacklogProbe.announce() is not None


def test_the_stall_is_logged_even_when_nobody_is_watching(monkeypatch) -> None:
    """The console scrolls away; the log is the postmortem trail."""
    module = _probe_module()

    monkeypatch.setattr(
        PublicationBacklogProbe,
        "sample",
        staticmethod(lambda: {"count": 1250, "age": 4210.0}),
    )
    logged: list[tuple] = []
    monkeypatch.setattr(
        module.Log,
        "warning",
        lambda msg, *args, **kw: logged.append((msg, args)),
    )
    PublicationBacklogProbe.announce()
    assert len(logged) == 1
    assert "queue:relay" in logged[0][1][0]


# ── the worker wires it in, and cannot be killed by it ───────────────


class _RecordingConsole:
    def __init__(self) -> None:
        self.lines: list[str] = []

    def print(self, text: str = "") -> None:
        self.lines.append(text)


def _worker() -> QueueWorkCommand:
    worker = QueueWorkCommand.__new__(QueueWorkCommand)
    worker.console = _RecordingConsole()
    return worker


def test_the_worker_prints_the_banner(monkeypatch) -> None:
    monkeypatch.setattr(
        PublicationBacklogProbe,
        "sample",
        staticmethod(lambda: {"count": 1250, "age": 4210.0}),
    )
    worker = _worker()
    assert worker._warn_when_nothing_is_publishing() is not None

    rendered = "\n".join(worker.console.lines)
    assert "queue:relay" in rendered
    assert "1250" in rendered


def test_the_worker_is_silent_on_a_healthy_outbox(monkeypatch) -> None:
    monkeypatch.setattr(
        PublicationBacklogProbe,
        "sample",
        staticmethod(lambda: {"count": 0, "age": 0.0}),
    )
    worker = _worker()
    assert worker._warn_when_nothing_is_publishing() is None
    assert worker.console.lines == []


def test_a_probe_that_explodes_outright_cannot_stop_the_worker(
    monkeypatch,
) -> None:
    """The 2026-07-20 relay could not start because a metrics port was
    contended. A startup diagnostic that can do the same thing to the
    worker is a worse bug than the one it exists to report."""

    def _boom(**_kwargs):
        raise RuntimeError("probe itself is broken")

    monkeypatch.setattr(
        PublicationBacklogProbe, "announce", classmethod(lambda _cls, **_kw: _boom())
    )
    assert _worker()._warn_when_nothing_is_publishing() is None


def test_the_worker_runs_the_probe_during_startup() -> None:
    """Pins the call site — an unwired probe is a probe that never fires."""
    import inspect

    source = inspect.getsource(QueueWorkCommand.handle)
    assert "_warn_when_nothing_is_publishing()" in source


# ── discoverability: each command's help names the others ────────────


def test_queue_work_help_says_it_is_not_enough_on_its_own() -> None:
    help_text = QueueWorkCommand.help
    assert "CONSUMER" in help_text
    # The RUNNABLE form, not a bare mention. Someone who has just learned
    # they are missing a process should be able to copy the next line out
    # of the help rather than go hunting for its spelling.
    assert "craft queue:relay" in help_text
    assert "craft schedule:work" in help_text


def test_queue_relay_help_says_it_is_not_enough_on_its_own() -> None:
    help_text = QueueRelayCommand.help
    assert "PUBLISHER" in help_text
    assert "craft queue:work" in help_text
    assert "craft schedule:work" in help_text


def test_schedule_work_help_names_the_other_two() -> None:
    help_text = ScheduleWorkCommand.help
    assert "queue:relay" in help_text
    assert "queue:work" in help_text


def test_queue_hooks_is_not_mistaken_for_the_publisher() -> None:
    help_text = QueueHooksCommand.help
    assert "queue:relay" in help_text


@pytest.mark.parametrize(
    "command",
    [QueueWorkCommand, QueueRelayCommand, ScheduleWorkCommand],
)
def test_the_first_help_line_names_the_role(command) -> None:
    """Click truncates the command list to the first line.

    That truncated line is the ONLY thing an operator scanning `craft
    --help` reads, so the role has to survive the cut.
    """
    first_line = command.help.splitlines()[0]
    assert any(
        role in first_line
        for role in ("CONSUMER", "PUBLISHER", "SCHEDULER")
    )
