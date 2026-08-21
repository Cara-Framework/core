"""``PSQL_BIN`` / ``PG_DUMP_BIN`` must actually choose the binary.

The structure clone behind ``schema:plan --rehearse`` shells out to
``pg_dump`` and ``psql``. It resolved both from PATH alone while both
deployables declared overrides that nothing read — so the host that most
needs a rehearsal (client tools outside PATH, or a PATH client older than the
server) had a setting that changed nothing, and the operator's only signal was
a refusal telling them to install tools they had already installed.
"""

from __future__ import annotations

import importlib

import pytest

from cara.exceptions import ScratchDatabaseException

_SCRATCH = importlib.import_module("cara.schema.Scratch")


def _resolve(monkeypatch, *, override, on_path="/usr/bin/pg_dump"):
    monkeypatch.setattr(
        _SCRATCH, "config", lambda _key, default=None: override or default
    )
    monkeypatch.setattr(
        _SCRATCH.shutil,
        "which",
        lambda name: on_path if name in ("pg_dump", str(override)) else None,
    )
    return _SCRATCH._resolve_client_binary("pg_dump", "database.pg_dump_bin")


def test_path_is_used_when_no_override_is_set(monkeypatch):
    assert _resolve(monkeypatch, override=None) == "/usr/bin/pg_dump"


def test_an_explicit_override_wins_over_path(monkeypatch):
    resolved = _resolve(
        monkeypatch, override="/opt/pgsql-16/bin/pg_dump", on_path="/opt/pgsql-16/bin/pg_dump"
    )

    assert resolved == "/opt/pgsql-16/bin/pg_dump"


def test_an_override_that_is_not_executable_is_refused_not_ignored(monkeypatch):
    """Falling back to PATH here would run a DIFFERENT binary than the
    operator named — the failure mode this whole knob exists to prevent."""
    monkeypatch.setattr(_SCRATCH, "config", lambda _key, default=None: "/typo/pg_dump")
    monkeypatch.setattr(_SCRATCH.shutil, "which", lambda name: None)

    with pytest.raises(ScratchDatabaseException) as raised:
        _SCRATCH._resolve_client_binary("pg_dump", "database.pg_dump_bin")

    assert "/typo/pg_dump" in str(raised.value)
    assert "not an executable" in str(raised.value)


def test_an_unbooted_config_still_falls_back_to_path(monkeypatch):
    """A rehearsal must not require a booted configuration container."""

    def _boom(_key, default=None):
        raise RuntimeError("configuration container not bootstrapped")

    monkeypatch.setattr(_SCRATCH, "config", _boom)
    monkeypatch.setattr(_SCRATCH.shutil, "which", lambda name: "/usr/bin/psql")

    assert _SCRATCH._resolve_client_binary("psql", "database.psql_bin") == "/usr/bin/psql"
