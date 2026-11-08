"""``queue:hooks`` throughput and shutdown, proven against a real child process.

The fake ledger below emulates the row effect of each statement the runner
issues (a real PostgreSQL proves the SQL itself); the process side is real:
a genuine interpreter child, a genuine SIGTERM, a genuine reap.
"""

import importlib
import json
import os
import signal
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pendulum
import pytest

from cara.commands.core.QueueHooksCommand import QueueHooksCommand
from cara.queues.delivery import QueueJobDeliveryStore
from cara.queues.delivery.TerminalHookClaim import TerminalHookClaim

relay_module = importlib.import_module("cara.commands.core.QueueRelayCommand")
hooks_module = importlib.import_module("cara.commands.core.QueueHooksCommand")
serializer = importlib.import_module(
    "cara.queues.serializers.SignedJsonJobSerializer"
).SignedJsonJobSerializer


class _WithCallback:
    async def failed(self, _job_data, _error, *, idempotency_key):
        return idempotency_key


class _WithoutCallback:
    pass


class _Ledger:
    """One dict per ``queue_job_delivery`` row; asserts on unknown SQL."""

    def __init__(self, job_classes: dict[str, type]):
        self.rows = {
            job_id: {
                "job_id": job_id,
                "status": QueueJobDeliveryStore.STATUS_DEAD_LETTERED,
                "terminal_reason": "gave up",
                "signed_envelope": json.dumps({"job": job_id}).encode(),
                "post_hooks_completed_at": None,
                "post_hooks_lease_token": None,
                "post_hooks_lease_expires_at": None,
                "post_hooks_attempts": 0,
                "post_hooks_quarantined_at": None,
                "post_hooks_last_error": None,
            }
            for job_id in job_classes
        }
        self.completions: list[str] = []

    @contextmanager
    def transaction(self):
        yield self

    def select_one(self, sql, params):
        assert sql.startswith("SELECT"), sql
        return dict(self.rows[params[0]]) if params[0] in self.rows else None

    def statement(self, sql, params):
        if "SET post_hooks_completed_at = %s" in sql:
            now, _, job_id, token = params
            row = self.rows[job_id]
            if row["post_hooks_lease_token"] != token:
                return 0
            row.update(
                post_hooks_completed_at=now,
                post_hooks_lease_token=None,
                post_hooks_lease_expires_at=None,
                post_hooks_last_error=None,
            )
            self.completions.append(job_id)
            return 1
        if "SET post_hooks_lease_token = %s" in sql:
            token, expiry, _, job_id, _ = params
            self.rows[job_id].update(
                post_hooks_lease_token=token,
                post_hooks_lease_expires_at=expiry,
                post_hooks_last_error=None,
            )
            return 1
        if "post_hooks_lease_token = NULL" in sql and "expires_at >= %s" in sql:
            _, job_id, floor = params
            row = self.rows[job_id]
            # A fence counts only if the statement really carries it: a fake
            # more forgiving than the SQL would stay green with one deleted.
            fences = {
                "post_hooks_completed_at IS NULL": row["post_hooks_completed_at"] is None,
                "post_hooks_quarantined_at IS NULL": row["post_hooks_quarantined_at"]
                is None,
                "post_hooks_last_error IS NULL": row["post_hooks_last_error"] is None,
                "post_hooks_lease_token IS NOT NULL": row["post_hooks_lease_token"]
                is not None,
            }
            if (
                not all(holds for clause, holds in fences.items() if clause in sql)
                or row["post_hooks_lease_expires_at"] < floor
            ):
                return 0
            row.update(post_hooks_lease_token=None, post_hooks_lease_expires_at=None)
            return 1
        raise AssertionError(f"Unexpected statement: {sql}")


class _Application:
    def __init__(self, db):
        self.db = db

    def has(self, key):
        return key == "DB"

    def make(self, key):
        assert key == "DB"
        return self.db


def _resolve_job_classes(monkeypatch, job_classes: dict[str, type]) -> None:
    """Make envelope verification resolve each row to its declared class."""
    monkeypatch.setattr(
        serializer,
        "inspect_envelope",
        lambda envelope, **_kwargs: {"payload": json.loads(envelope)},
    )
    monkeypatch.setattr(
        serializer,
        "deserialize_verified",
        lambda payload, **_kwargs: {
            "obj": job_classes[payload["job"]],
            "args": (),
            "init_kwargs": {},
        },
    )


def _driver(ledger, job_ids, **overrides):
    store = QueueJobDeliveryStore(
        _Application(ledger),
        SimpleNamespace(),
        {"canonical_queues": ("sync", "connector")},
    )
    batches = iter([list(job_ids)])
    return SimpleNamespace(
        delivery_store=store,
        due_terminal_hook_ids=lambda: next(batches, []),
        defer_terminal_hook_process_failure=lambda *_a, **_k: pytest.fail(
            "an interrupted or classified row must not spend a hook attempt"
        ),
        refresh_delivery_metrics=lambda: {},
        **overrides,
    )


def _forbid_any_process(monkeypatch):
    def _spawned(*_args, **_kwargs):
        raise AssertionError("a row without a callback must not boot a process")

    monkeypatch.setattr(subprocess, "run", _spawned)
    monkeypatch.setattr(subprocess, "Popen", _spawned)


class _FinishedChild:
    """A child that already exited with ``returncode``."""

    def __init__(self, returncode=0):
        self.returncode = returncode
        self.pid = 4242

    def poll(self):
        return self.returncode

    def wait(self, timeout=None):
        return self.returncode

    def terminate(self):
        raise AssertionError("a finished child is never signalled")

    kill = terminate


def _record_spawns(monkeypatch, returncode=0):
    spawned: list[str] = []

    def _popen(argv, **_kwargs):
        spawned.append(argv[argv.index("--job-id") + 1])
        return _FinishedChild(returncode)

    monkeypatch.setattr(subprocess, "Popen", _popen)
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda argv, **_kwargs: (
            spawned.append(argv[argv.index("--job-id") + 1])
            or SimpleNamespace(returncode=returncode)
        ),
    )
    return spawned


def test_rows_without_a_callback_settle_exactly_once_and_boot_no_process(monkeypatch):
    classes = {"a": _WithoutCallback, "b": _WithoutCallback, "c": _WithoutCallback}
    _resolve_job_classes(monkeypatch, classes)
    _forbid_any_process(monkeypatch)
    ledger = _Ledger(classes)

    result = QueueHooksCommand._run_isolated_hooks(
        _driver(ledger, classes),
        threading.Event(),
    )

    assert ledger.completions == ["a", "b", "c"]
    assert all(r["post_hooks_lease_token"] is None for r in ledger.rows.values())
    assert result["claimed"] == 3
    assert result["completed"] == 3
    assert result["failed"] == 0


@pytest.mark.parametrize(
    "status",
    [QueueJobDeliveryStore.STATUS_DEAD_LETTERED, QueueJobDeliveryStore.STATUS_EXPIRED],
)
def test_both_statuses_that_fire_failed_go_to_a_child_and_are_not_settled(
    monkeypatch, status
):
    classes = {"row": _WithCallback}
    _resolve_job_classes(monkeypatch, classes)
    spawned = _record_spawns(monkeypatch)
    ledger = _Ledger(classes)
    ledger.rows["row"]["status"] = status

    QueueHooksCommand._run_isolated_hooks(_driver(ledger, classes), threading.Event())

    assert spawned == ["row"]
    assert ledger.completions == []


def test_a_status_that_never_fires_failed_settles_without_a_child(monkeypatch):
    classes = {"row": _WithCallback}
    _resolve_job_classes(monkeypatch, classes)
    _forbid_any_process(monkeypatch)
    ledger = _Ledger(classes)
    ledger.rows["row"]["status"] = QueueJobDeliveryStore.STATUS_COMPLETED

    QueueHooksCommand._run_isolated_hooks(_driver(ledger, classes), threading.Event())

    assert ledger.completions == ["row"]


def test_lease_lost_after_the_claim_skips_that_row_and_the_batch_goes_on(monkeypatch):
    classes = {"stolen": _WithoutCallback, "next": _WithoutCallback}
    _resolve_job_classes(monkeypatch, classes)
    _forbid_any_process(monkeypatch)
    ledger = _Ledger(classes)
    real_statement = ledger.statement

    def _steal_lease_before_completing(sql, params):
        if "SET post_hooks_completed_at = %s" in sql and params[2] == "stolen":
            ledger.rows["stolen"]["post_hooks_lease_token"] = "another-runner"
        return real_statement(sql, params)

    ledger.statement = _steal_lease_before_completing

    result = QueueHooksCommand._run_isolated_hooks(
        _driver(ledger, classes),
        threading.Event(),
    )

    assert ledger.completions == ["next"]
    assert ledger.rows["stolen"]["post_hooks_lease_token"] == "another-runner"
    assert result["skipped"] == 1
    assert result["completed"] == 1


def test_stop_before_classifying_leaves_every_row_due(monkeypatch):
    classes = {"a": _WithoutCallback, "b": _WithCallback}
    _resolve_job_classes(monkeypatch, classes)
    _forbid_any_process(monkeypatch)
    ledger = _Ledger(classes)
    stop = threading.Event()
    stop.set()

    result = QueueHooksCommand._run_isolated_hooks(_driver(ledger, classes), stop)

    assert ledger.completions == []
    assert all(r["post_hooks_lease_token"] is None for r in ledger.rows.values())
    assert result["claimed"] == 2
    assert result["completed"] == 0


def test_only_rows_with_a_callback_reach_a_child_and_wait_behind_the_rest(monkeypatch):
    classes = {"first": _WithCallback, "second": _WithoutCallback, "third": _WithCallback}
    _resolve_job_classes(monkeypatch, classes)
    spawned = _record_spawns(monkeypatch)
    ledger = _Ledger(classes)
    order: list[str] = []
    real_popen = subprocess.Popen

    def _popen(argv, **kwargs):
        order.append(f"spawn:{ledger.completions}")
        return real_popen(argv, **kwargs)

    monkeypatch.setattr(subprocess, "Popen", _popen)

    result = QueueHooksCommand._run_isolated_hooks(
        _driver(ledger, classes),
        threading.Event(),
    )

    assert spawned == ["first", "third"]
    assert ledger.completions == ["second"]
    assert order == ["spawn:['second']", "spawn:['second']"]
    assert result["completed"] == 3


def test_row_lease_held_elsewhere_is_left_alone_and_not_counted(monkeypatch):
    classes = {"busy": _WithoutCallback}
    _resolve_job_classes(monkeypatch, classes)
    _forbid_any_process(monkeypatch)
    ledger = _Ledger(classes)
    live_until = pendulum.now("UTC").add(seconds=120)
    ledger.rows["busy"].update(
        post_hooks_lease_token="another-runner",
        post_hooks_lease_expires_at=live_until,
    )

    result = QueueHooksCommand._run_isolated_hooks(
        _driver(ledger, classes),
        threading.Event(),
    )

    assert ledger.completions == []
    assert ledger.rows["busy"]["post_hooks_lease_token"] == "another-runner"
    assert result["completed"] == 0
    assert result["skipped"] == 1


def test_a_row_the_loop_cannot_verify_keeps_the_isolated_failure_accounting(monkeypatch):
    classes = {"tampered": _WithCallback}
    spawned = _record_spawns(monkeypatch)
    monkeypatch.setattr(
        serializer,
        "inspect_envelope",
        lambda *_a, **_k: (_ for _ in ()).throw(ValueError("bad signature")),
    )
    ledger = _Ledger(classes)

    QueueHooksCommand._run_isolated_hooks(
        _driver(ledger, classes),
        threading.Event(),
    )

    assert spawned == ["tampered"]
    assert ledger.completions == []
    assert ledger.rows["tampered"]["post_hooks_lease_token"] is None


def test_database_failure_while_classifying_is_systemic_not_a_spent_attempt(monkeypatch):
    classes = {"row": _WithoutCallback}
    _forbid_any_process(monkeypatch)
    ledger = _Ledger(classes)

    def _down(*_a, **_k):
        raise ConnectionError("database went away")

    ledger.select_one = _down

    with pytest.raises(ConnectionError):
        QueueHooksCommand._run_isolated_hooks(
            _driver(ledger, classes),
            threading.Event(),
        )


def test_child_exit_codes_keep_their_meaning(monkeypatch):
    classes = {"lost-race": _WithCallback}
    _resolve_job_classes(monkeypatch, classes)
    _record_spawns(monkeypatch, returncode=getattr(os, "EX_TEMPFAIL", 75))
    ledger = _Ledger(classes)

    result = QueueHooksCommand._run_isolated_hooks(
        _driver(ledger, classes),
        threading.Event(),
    )

    assert result["skipped"] == 1
    assert result["failed"] == 0


def test_child_past_its_budget_is_killed_and_deferred(monkeypatch):
    classes = {"hung": _WithCallback}
    _resolve_job_classes(monkeypatch, classes)
    ledger = _Ledger(classes)
    deferred: list[tuple[str, str]] = []
    killed: list[bool] = []

    class _Hung(_FinishedChild):
        def poll(self):
            return -9 if killed else None

        def kill(self):
            killed.append(True)

        def wait(self, timeout=None):
            return -9

    monkeypatch.setattr(subprocess, "Popen", lambda *_a, **_k: _Hung())
    driver = _driver(ledger, classes)
    driver.delivery_store.hook_timeout_seconds = -15
    driver.defer_terminal_hook_process_failure = lambda job_id, *, error: (
        deferred.append((job_id, error)) or "deferred"
    )

    result = QueueHooksCommand._run_isolated_hooks(driver, threading.Event())

    assert killed == [True]
    assert deferred == [("hung", "isolated terminal-hook process timed out")]
    assert result["failed"] == 1
    assert result["deferred"] == 1


# ---- Real SIGTERM, real child --------------------------------------------


_COOPERATIVE_CHILD = """
import os, sys, time
open(sys.argv[sys.argv.index('--job-id') + 1 + 1], 'w').write(str(os.getpid()))
time.sleep(120)
"""

_STUBBORN_CHILD = """
import os, signal, sys, time
signal.signal(signal.SIGTERM, signal.SIG_IGN)
open(sys.argv[sys.argv.index('--job-id') + 1 + 1], 'w').write(str(os.getpid()))
time.sleep(120)
"""


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def _run_loop_until_sigterm(monkeypatch, tmp_path, child_source, ledger, job_ids):
    """Run the real loop in the main thread and SIGTERM it mid-child."""
    pid_file = tmp_path / "child.pid"
    script = tmp_path / "child.py"
    script.write_text(child_source)
    monkeypatch.setattr(sys, "argv", [str(script)])

    spawned: list[tuple[str, subprocess.Popen]] = []
    real_popen = subprocess.Popen
    store_holder: list[QueueJobDeliveryStore] = []

    def _popen(argv, **kwargs):
        job_id = argv[argv.index("--job-id") + 1]
        # The stand-in child must not boot Cara; it takes the row's lease here
        # the way ``queue:hook`` would right after booting.
        claim = store_holder[0].claim_terminal_hooks(job_id)
        assert isinstance(claim, TerminalHookClaim)
        assert claim.outcome == "claimed"
        process = real_popen([*argv, str(pid_file)], **kwargs)
        spawned.append((job_id, process))
        return process

    monkeypatch.setattr(subprocess, "Popen", _popen)
    driver = _driver(ledger, job_ids)
    store_holder.append(driver.delivery_store)
    monkeypatch.setattr(relay_module, "Queue", SimpleNamespace(driver=lambda _n: driver))
    monkeypatch.setattr(relay_module, "start_http_server", lambda **_k: None)
    monkeypatch.setattr(relay_module, "config", lambda _key, default=0: default)

    def _sigterm_once_child_runs():
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if pid_file.exists() and pid_file.read_text():
                os.kill(os.getpid(), signal.SIGTERM)
                return
            time.sleep(0.02)

    previous = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
    sender = threading.Thread(target=_sigterm_once_child_runs, daemon=True)
    command = QueueHooksCommand.__new__(QueueHooksCommand)
    sender.start()
    try:
        started = time.monotonic()
        exit_code = command.handle(once=False, poll_interval=0.05)
        elapsed = time.monotonic() - started
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)
    sender.join(timeout=5)
    return exit_code, elapsed, spawned, pid_file


@pytest.mark.parametrize(
    ("child_source", "grace", "stops_within"),
    [(_COOPERATIVE_CHILD, 20.0, 12), (_STUBBORN_CHILD, 0.5, 12)],
    ids=["child-honours-sigterm", "child-ignores-sigterm-and-is-killed"],
)
def test_sigterm_mid_batch_stops_promptly_and_leaves_no_child(
    monkeypatch, tmp_path: Path, child_source, grace, stops_within
):
    classes = {
        "running": _WithCallback,
        "queued-1": _WithCallback,
        "queued-2": _WithCallback,
    }
    _resolve_job_classes(monkeypatch, classes)
    monkeypatch.setattr(hooks_module, "_CHILD_STOP_GRACE_SECONDS", grace)
    ledger = _Ledger(classes)

    exit_code, elapsed, spawned, pid_file = _run_loop_until_sigterm(
        monkeypatch, tmp_path, child_source, ledger, classes
    )

    pid = int(pid_file.read_text())
    assert exit_code == 0
    assert elapsed < stops_within
    assert not _alive(pid), "the stopped child must be reaped, not orphaned"
    assert [job_id for job_id, _process in spawned] == ["running"]

    interrupted = ledger.rows["running"]
    assert interrupted["post_hooks_lease_token"] is None
    assert interrupted["post_hooks_lease_expires_at"] is None
    assert interrupted["post_hooks_attempts"] == 0
    assert interrupted["post_hooks_last_error"] is None
    assert interrupted["post_hooks_quarantined_at"] is None
    assert interrupted["post_hooks_completed_at"] is None


def test_signal_landing_while_the_loop_holds_the_stop_lock_still_stops(monkeypatch):
    """``Condition.wait`` re-takes the lock when it times out, on the signal's thread."""
    driver = SimpleNamespace()
    monkeypatch.setattr(relay_module, "Queue", SimpleNamespace(driver=lambda _n: driver))
    monkeypatch.setattr(relay_module, "start_http_server", lambda **_k: None)
    monkeypatch.setattr(relay_module, "config", lambda _key, default=0: default)
    observed: list[bool] = []

    def _callback(_driver, stop):
        with stop._cond:
            os.kill(os.getpid(), signal.SIGTERM)
        observed.append(stop.wait(5))
        return {"claimed": 0}

    def _deadlocked(_signum, _frame):
        raise AssertionError("the signal handler deadlocked on the stop lock")

    signals = (signal.SIGINT, signal.SIGTERM, signal.SIGALRM)
    previous = {sig: signal.getsignal(sig) for sig in signals}
    signal.signal(signal.SIGALRM, _deadlocked)
    signal.setitimer(signal.ITIMER_REAL, 3)
    try:
        command = QueueHooksCommand.__new__(QueueHooksCommand)
        exit_code = command._handle_loop(
            once=True,
            poll_interval=0.05,
            callback=_callback,
        )
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        for sig, handler in previous.items():
            signal.signal(sig, handler)

    assert exit_code == 0
    assert observed == [True]


def test_stop_set_between_jobs_spawns_nothing_further(monkeypatch):
    classes = {"one": _WithCallback, "two": _WithCallback}
    _resolve_job_classes(monkeypatch, classes)
    ledger = _Ledger(classes)
    stop = threading.Event()
    spawned: list[str] = []

    def _popen(argv, **_kwargs):
        spawned.append(argv[argv.index("--job-id") + 1])
        stop.set()
        return _FinishedChild(0)

    monkeypatch.setattr(subprocess, "Popen", _popen)

    result = QueueHooksCommand._run_isolated_hooks(_driver(ledger, classes), stop)

    assert spawned == ["one"]
    assert result["completed"] == 1


def _release(ledger, job_id, spawned_at):
    settlement = importlib.import_module("cara.queues.delivery._DeliverySettlement")
    store = QueueJobDeliveryStore(
        _Application(ledger),
        SimpleNamespace(),
        {"canonical_queues": ("sync", "connector")},
    )
    return settlement._delivery_release_interrupted_terminal_hook(
        store,
        job_id,
        spawned_at=spawned_at,
    )


def test_release_never_takes_a_lease_older_than_the_stopped_child():
    ledger = _Ledger({"row": _WithCallback})
    spawned_at = pendulum.now("UTC")
    held_before = spawned_at.subtract(seconds=30).add(
        seconds=QueueJobDeliveryStore.POST_HOOK_LEASE_SECONDS
    )
    ledger.rows["row"].update(
        post_hooks_lease_token="another-runner",
        post_hooks_lease_expires_at=held_before,
    )

    assert _release(ledger, "row", spawned_at) is False
    assert ledger.rows["row"]["post_hooks_lease_token"] == "another-runner"


@pytest.mark.parametrize(
    "settled",
    ["post_hooks_completed_at", "post_hooks_quarantined_at"],
)
def test_release_never_touches_a_settled_row(settled):
    ledger = _Ledger({"row": _WithCallback})
    spawned_at = pendulum.now("UTC")
    lease = spawned_at.add(seconds=QueueJobDeliveryStore.POST_HOOK_LEASE_SECONDS + 1)
    ledger.rows["row"].update(
        post_hooks_lease_token="child",
        post_hooks_lease_expires_at=lease,
        **{settled: spawned_at},
    )

    assert _release(ledger, "row", spawned_at) is False
    assert ledger.rows["row"]["post_hooks_lease_token"] == "child"


def test_release_keeps_the_backoff_of_a_child_that_deferred_before_it_died():
    ledger = _Ledger({"row": _WithCallback})
    spawned_at = pendulum.now("UTC")
    backoff = spawned_at.add(hours=1)
    ledger.rows["row"].update(
        post_hooks_lease_token="child",
        post_hooks_lease_expires_at=backoff,
        post_hooks_attempts=1,
        post_hooks_last_error="hook raised",
    )

    assert _release(ledger, "row", spawned_at) is False
    assert ledger.rows["row"]["post_hooks_lease_expires_at"] == backoff
    assert ledger.rows["row"]["post_hooks_attempts"] == 1
