"""``schema:verify`` — orchestration pins, no database.

The command's value is choreography: scratch created before anything runs,
the two subcommands in proof order, the scratch dropped on EVERY exit path,
and the failing step's exit code surfacing unchanged. All world-facing work
lives behind three seams (``_connection_params`` / ``_admin_sql`` /
``_run_craft``), so these tests replace the seams and pin the choreography.
"""

from __future__ import annotations

import importlib
import inspect
from pathlib import Path

import pytest

from cara.commands.core.SchemaVerifyCommand import SchemaVerifyCommand

_MODULE = importlib.import_module("cara.commands.core.SchemaVerifyCommand")

_PARAMS = {
    "driver": "postgres",
    "host": "127.0.0.1",
    "port": 5432,
    "user": "app",
    "password": "s3cret",
    "database": "synkronus",
    "options": {"sslmode": "disable"},
}


class _Verify(SchemaVerifyCommand):
    def __init__(self, options=None, craft_codes=None, params=None):
        self.application = None
        self._parsed_options = options or {}
        self.messages: list[str] = []
        self.admin_batches: list[list[str]] = []
        self.craft_calls: list[tuple[list[str], str]] = []
        self._craft_codes = list(craft_codes or [0, 0])
        self._params = dict(params or _PARAMS)

    def info(self, message: str) -> None:
        self.messages.append(message)

    def warning(self, message: str) -> None:
        self.messages.append(message)

    def error(self, message: str, **_kwargs) -> None:
        self.messages.append(message)

    def success(self, message: str) -> None:
        self.messages.append(message)

    def _connection_params(self) -> dict:
        return self._params

    def _admin_sql(self, params, statements) -> None:
        assert params is self._params
        self.admin_batches.append(list(statements))

    def _run_craft(self, arguments, scratch_database) -> int:
        self.craft_calls.append((list(arguments), scratch_database))
        return self._craft_codes.pop(0)


def _text(command) -> str:
    return "\n".join(command.messages)


# ── the happy path is the invariant ─────────────────────────────────────────


def test_proof_order_and_cleanup():
    command = _Verify()
    assert command.handle() == 0

    assert command.craft_calls == [
        (["migrate"], "synkronus_verify"),
        (["schema:check"], "synkronus_verify"),
    ]
    # First batch prepares the scratch (drop-if-exists then create); the last
    # batch is the unconditional cleanup drop.
    assert 'CREATE DATABASE "synkronus_verify"' in command.admin_batches[0][1]
    assert command.admin_batches[0][0].startswith("DROP DATABASE IF EXISTS")
    assert command.admin_batches[-1] == [
        'DROP DATABASE IF EXISTS "synkronus_verify" WITH (FORCE)'
    ]
    assert "migrated from zero equals the models" in _text(command)


# ── every failure keeps the exit code and still cleans up ───────────────────


def test_failed_migrate_propagates_and_skips_schema_check():
    command = _Verify(craft_codes=[3])
    assert command.handle() == 3
    assert command.craft_calls == [(["migrate"], "synkronus_verify")]
    # Cleanup drop still ran.
    assert command.admin_batches[-1][0].startswith("DROP DATABASE IF EXISTS")
    assert "does not install from zero" in _text(command)


def test_failed_schema_check_propagates_drift_exit():
    command = _Verify(craft_codes=[0, 1])
    assert command.handle() == 1
    assert [arguments for arguments, _ in command.craft_calls] == [
        ["migrate"],
        ["schema:check"],
    ]
    assert command.admin_batches[-1][0].startswith("DROP DATABASE IF EXISTS")


def test_keep_leaves_the_scratch_for_autopsy():
    command = _Verify(options={"keep": True})
    assert command.handle() == 0
    # One admin batch only: the create; no cleanup drop.
    assert len(command.admin_batches) == 1
    assert "left in place" in _text(command)


def test_scratch_creation_failure_stops_before_any_subcommand():
    class _Broken(_Verify):
        def _admin_sql(self, params, statements) -> None:
            raise RuntimeError("connection refused")

    command = _Broken()
    assert command.handle() == 2
    assert command.craft_calls == []


# ── refusals ────────────────────────────────────────────────────────────────


def test_refuses_to_target_the_configured_database():
    command = _Verify(options={"database": "synkronus"})
    assert command.handle() == 2
    assert command.admin_batches == []
    assert command.craft_calls == []


@pytest.mark.parametrize("name", ["x;drop", "Verify", "1abc", 'a"b'])
def test_refuses_unsafe_scratch_names(name):
    command = _Verify(options={"database": name})
    assert command.handle() == 2
    assert command.admin_batches == []


def test_refuses_in_production(monkeypatch):
    # ``config`` is bound at the command module's top — patch that name.
    monkeypatch.setattr(
        _MODULE,
        "config",
        lambda key, default=None: "production" if key == "app.env" else default,
    )
    command = _Verify()
    assert command.handle() == 2
    assert command.admin_batches == []
    assert command.craft_calls == []


def test_non_postgres_driver_is_refused_by_the_real_params_reader(monkeypatch):
    values = {
        "app.env": "local",
        "database.default": "app",
        "database.drivers": {"app": {"driver": "sqlite", "database": "cara.sqlite3"}},
    }
    monkeypatch.setattr(
        _MODULE,
        "config",
        lambda key, default=None: values.get(key, default),
    )

    class _RealParams(_Verify):
        _connection_params = SchemaVerifyCommand._connection_params

    command = _RealParams()
    assert command.handle() == 2
    assert "postgres driver" in _text(command)


def test_scratch_name_is_sanitised_from_an_exotic_configured_name():
    """``synkronus.io`` is a real configured database name — the derived
    scratch must still be a boring identifier, while an explicit --database
    keeps having to pass the safety check on its own."""
    command = _Verify(params={**_PARAMS, "database": "synkronus.io"})
    assert command.handle() == 0
    assert command.craft_calls[0] == (["migrate"], "synkronus_io_verify")


def test_scratch_name_gains_a_leading_letter_when_needed():
    command = _Verify(params={**_PARAMS, "database": "1shop"})
    assert command.handle() == 0
    assert command.craft_calls[0] == (["migrate"], "v_1shop_verify")


def test_every_flag_a_message_recommends_actually_exists():
    """A message naming a flag the parser rejects is worse than no message:
    the operator types exactly what they were told and gets "No such option".

    Cara's option names use underscores, so any hyphenated token whose
    underscore form IS a real cara option is a lie by construction — which is
    precisely how ``--allow-destructive`` reached four files and two products'
    documentation while ``--allow_destructive`` was the only thing that ran.
    The truth is read from the parser's own metadata so this test cannot
    disagree with it, and external binaries' flags (``pg_dump
    --schema-only``) stay legal because no cara option is named after them.
    """
    import re

    apply_module = importlib.import_module("cara.commands.core.SchemaApplyCommand")
    plan_module = importlib.import_module("cara.commands.core.SchemaPlanCommand")
    rollback_module = importlib.import_module("cara.commands.core.SchemaRollbackCommand")

    modules = (plan_module, apply_module, rollback_module)
    real: set[str] = set()
    for module in modules:
        for name in dir(module):
            value = getattr(module, name)
            options = inspect.getattr_static(value, "_cli_options", None) or []
            for declared in options:
                # {"name": "--c|--connection", ...} -> {"c", "connection"}
                option_name = declared["name"]
                for alias in option_name.split("|"):
                    alias = alias.lstrip("-")
                    real.add(alias)
    assert "allow_destructive" in real, "metadata not found — test is vacuous"

    for module in modules:
        source = Path(module.__file__).read_text(encoding="utf-8")
        for token in re.findall(r"--([a-z][a-z-]*[a-z])", source):
            if "-" not in token:
                continue
            assert token.replace("-", "_") not in real, (
                f"{module.__name__} tells the operator to type --{token}, "
                f"but the parser only accepts --{token.replace('-', '_')}"
            )


class _Registry:
    """Just enough of the command runner to answer "is X registered?"."""

    def __init__(self, names):
        self.runner = type(
            "_Runner",
            (),
            {
                "console_app": type(
                    "_Console",
                    (),
                    {
                        "registered_commands": [
                            type("_Cmd", (), {"name": name})() for name in names
                        ]
                    },
                )()
            },
        )()


class _Application:
    def __init__(self, names):
        self._registry = _Registry(names)

    def make(self, key):
        assert key == "commands"
        return self._registry


def _verify_with(names):
    command = _Verify()
    command.application = _Application(names)
    return command


def test_a_deployable_without_migrate_says_so_instead_of_blaming_the_directory():
    """Spawned from a worker repository, verify died on "No such command
    'migrate'" and reported "the generated directory does not install from
    zero" — a false accusation produced by a command that could not find its
    own dependency."""
    command = _verify_with(["schema:check", "queue:work"])

    assert command.handle() == 2
    text = _text(command)
    assert "does not carry migrate" in text
    assert "not about the migrations" in text
    assert "does not install from zero" not in text


def test_the_dependency_check_asks_the_registry_not_the_repository_name():
    """cheapa's services keeps `migrate` for its reset workflow and
    synkronus' does not — the strip list is each product's decision, so
    guessing from the deployable's name would be wrong in one of them."""
    command = _verify_with(["migrate", "schema:check"])

    assert command.handle() == 0


def test_no_registry_to_ask_means_proceed_rather_than_refuse():
    """Absence of evidence is not evidence of absence: with no runner to
    query, let the child process speak for itself."""
    command = _Verify()  # no .application at all

    assert command.handle() == 0
