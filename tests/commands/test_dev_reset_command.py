"""dev:reset template — step order, the local-only refusal, the confirm gate.

Three things the framework owns and must never get wrong:

* ORDER. Workers stop before anything else; a live consumer re-populates the
  cache you just flushed and re-fills the queue you just purged.
* The local-only refusal (environment whitelist + a database on this
  machine), which no ``--yes`` can talk past.
* The confirm gate: ``--db`` without ``--yes`` deletes nothing.

And one thing it must never own: the truncate statement. The base declares
``_truncate`` abstract and a test here proves the base emits no SQL at all.
"""

from __future__ import annotations

import ast
import importlib
import pathlib

import pytest

from cara.commands.core.DevResetCommand import DevResetCommand
from cara.decorators import get_registered_commands

_MODULE = importlib.import_module("cara.commands.core.DevResetCommand")


class _RecordingReset(DevResetCommand):
    name = "dev:reset"

    def __init__(self) -> None:  # no application; pure step recording
        super().__init__(None)
        self.steps: list[str] = []
        self.messages: list[str] = []

    # Silence the console; record instead.
    def line(self, message: str = "") -> None:
        self.messages.append(message)

    def info(self, message: str) -> None:
        self.messages.append(message)

    def warning(self, message: str) -> None:
        self.messages.append(message)

    def error(self, message: str, **_kwargs) -> None:
        self.messages.append(message)

    def _kill_workers(self) -> None:
        self.steps.append("workers")

    def _flush_cache(self) -> None:
        self.steps.append("cache")
        self._extra_cache_steps()

    def _purge_queues(self) -> None:
        self.steps.append("queues")

    def _rebuild_dlx_queues(self) -> None:
        self.steps.append("dlx")

    def queue_names(self) -> set[str]:
        return {"alpha", "beta"}

    def _truncate(self) -> None:
        self.steps.append("truncate")

    def _extra_cache_steps(self) -> None:
        self.steps.append("extra-cache")

    def _pre_db_steps(self) -> list[str]:
        self.steps.append("pre-db")
        return ["payloads purged"]

    def _extra_db_steps(self) -> list[str]:
        self.steps.append("extra-db")
        return ["search index cleared"]


def _config(app_env: object, db_host: object = "127.0.0.1"):
    values = {
        "app.env": app_env,
        "database.default": "app",
        "database.drivers": {"app": {"host": db_host}},
    }
    return lambda key, default=None: values.get(key, default)


@pytest.fixture
def _dev_env(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(_MODULE, "config", _config("local"))


def test_the_base_is_not_a_registered_command() -> None:
    # No ``name`` and no ``@command`` decorator: discovery cannot reach it, so
    # a bare ``dev:reset`` can never resolve to the abstract template.
    assert "name" not in DevResetCommand.__dict__
    assert DevResetCommand not in get_registered_commands()


def test_default_run_stops_short_of_the_database(_dev_env) -> None:
    command = _RecordingReset()
    assert command.handle() == 0
    assert command.steps == ["cache", "extra-cache", "queues"]


def test_workers_are_stopped_before_cache_and_queues(_dev_env) -> None:
    command = _RecordingReset()
    assert command.handle(db=True, dlx=True, kill_workers=True, yes=True) == 0
    assert command.steps == [
        "workers",
        "cache",
        "extra-cache",
        "queues",
        "dlx",
        "pre-db",
        "truncate",
        "extra-db",
    ]


def test_keep_flags_skip_their_steps(_dev_env) -> None:
    command = _RecordingReset()
    assert command.handle(keep_cache=True, keep_queues=True) == 0
    assert command.steps == []


def test_db_without_yes_refuses_and_deletes_nothing(_dev_env) -> None:
    # Not even the cache or the queues: the confirm gate runs before any step.
    command = _RecordingReset()
    assert command.handle(db=True, kill_workers=True, dlx=True) == 1
    assert command.steps == []
    assert any("--yes" in message for message in command.messages)


@pytest.mark.parametrize(
    "app_env", ["production", "prod", "staging", "preprod", "ci", "", None, "lokal"]
)
def test_a_non_local_environment_refuses_every_step_even_with_yes(
    monkeypatch: pytest.MonkeyPatch, app_env: object
) -> None:
    # A whitelist, not a production blacklist: staging, a typo or an unset
    # APP_ENV must not flush caches, purge queues or truncate anything.
    monkeypatch.setattr(_MODULE, "config", _config(app_env))
    command = _RecordingReset()
    assert command.handle(db=True, yes=True, kill_workers=True, dlx=True) == 1
    assert command.steps == []
    assert any("not a local environment" in message for message in command.messages)


@pytest.mark.parametrize("db_host", ["db.internal", "10.0.0.5", "", None])
def test_db_refuses_a_remote_database_even_in_a_local_environment(
    monkeypatch: pytest.MonkeyPatch, db_host: object
) -> None:
    # A dev shell with APP_ENV=local pointed at a remote database: --db
    # refuses before ANY step runs, so nothing is half-reset either.
    monkeypatch.setattr(_MODULE, "config", _config("local", db_host))
    command = _RecordingReset()
    assert command.handle(db=True, yes=True) == 1
    assert command.steps == []
    assert any("not a local database" in message for message in command.messages)


def test_a_remote_database_still_allows_the_non_db_steps(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_MODULE, "config", _config("dev", "db.internal"))
    command = _RecordingReset()
    assert command.handle() == 0
    assert command.steps == ["cache", "extra-cache", "queues"]


@pytest.mark.parametrize("db_host", ["127.0.0.1", "localhost", " LOCALHOST ", "::1"])
def test_db_runs_against_a_local_database(
    monkeypatch: pytest.MonkeyPatch, db_host: str
) -> None:
    monkeypatch.setattr(_MODULE, "config", _config(" Local ", db_host))
    command = _RecordingReset()
    assert command.handle(db=True, yes=True, keep_cache=True, keep_queues=True) == 0
    assert command.steps == ["pre-db", "truncate", "extra-db"]


def test_a_product_may_whitelist_an_extra_environment_but_not_the_host(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _CiReset(_RecordingReset):
        EXTRA_RESET_ENVIRONMENTS = ("ci",)

    monkeypatch.setattr(_MODULE, "config", _config("ci"))
    allowed = _CiReset()
    assert allowed.handle(db=True, yes=True, keep_cache=True, keep_queues=True) == 0

    monkeypatch.setattr(_MODULE, "config", _config("ci", "db.internal"))
    refused = _CiReset()
    assert refused.handle(db=True, yes=True) == 1
    assert refused.steps == []


def test_pre_db_cleanup_runs_before_the_wipe_and_both_labels_are_reported(
    _dev_env,
) -> None:
    command = _RecordingReset()
    command.handle(db=True, yes=True, keep_cache=True, keep_queues=True)
    assert command.steps == ["pre-db", "truncate", "extra-db"]
    summary = command.messages[-1]
    assert summary.index("payloads purged") < summary.index("tables truncated")
    assert "search index cleared" in summary


def test_the_framework_emits_no_destructive_sql() -> None:
    # The single most expensive thing this base could get wrong is choosing
    # the destructive statement on the application's behalf. Prose may name
    # TRUNCATE (the help text has to); a string literal that IS one may not.
    module_path = pathlib.Path(__file__).resolve().parents[2] / (
        DevResetCommand.__module__.replace(".", "/") + ".py"
    )
    tree = ast.parse(module_path.read_text(), filename=str(module_path))
    verbs = ("TRUNCATE", "DELETE FROM", "DROP TABLE", "ALTER TABLE")
    offenders = [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and node.value.strip().upper().startswith(verbs)
    ]
    assert offenders == [], f"the reset template must not emit SQL: {offenders}"


def test_dlx_queue_names_default_to_the_purge_set() -> None:
    command = _RecordingReset()
    assert command.dlx_queue_names() == command.queue_names()


def test_canonical_dlx_defaults_to_the_framework_dead_letter_exchange() -> None:
    from cara.queues.QueueState import DEAD_LETTER_EXCHANGE

    assert _RecordingReset().canonical_dlx() == DEAD_LETTER_EXCHANGE


def test_queue_names_and_truncate_are_abstract() -> None:
    class _Bare(DevResetCommand):
        pass

    bare = _Bare(None)
    with pytest.raises(NotImplementedError):
        bare.queue_names()
    with pytest.raises(NotImplementedError):
        bare._truncate()
