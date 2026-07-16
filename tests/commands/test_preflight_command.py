"""``check:deploy`` (preflight) command — registry, per-check verdicts, exit code.

Exercises the command with no live anything: ``config()`` is stubbed per-test via
a fake mapping, so each production-readiness check runs against controlled
values. Mirrors the existing command-test pattern (``application=None``,
``set_parsed_options``, mocked ``console``).
"""

from __future__ import annotations

import sys
from unittest.mock import MagicMock

# NOTE: ``cara.commands.core.__init__`` re-exports the ``PreflightCommand``
# CLASS, which shadows the same-named SUBMODULE on the package — so
# ``import cara.commands.core.PreflightCommand`` would bind the class, not the
# module, and ``monkeypatch.setattr(mod, "config", ...)`` would fail. Pull the
# real module object out of ``sys.modules`` instead (it's there once imported).
from cara.commands.core.PreflightCommand import (
    FAIL,
    OK,
    WARN,
    CheckResult,
    PreflightCommand,
    check_app_key_set,
    check_debug_off_in_prod,
    check_required_config_present,
    fail,
    ok,
    warn,
)

preflight_mod = sys.modules["cara.commands.core.PreflightCommand"]


# ── config stubbing ──────────────────────────────────────────────────────


def _install_config(monkeypatch, values: dict):
    """Replace the module-level ``config()`` with a dict-backed lookup.

    Keys are dotted config paths (e.g. ``app.key``, ``auth.guards``). Missing
    keys return the supplied default — exactly like the real ``config()``.
    """

    def fake_config(key, default=None):
        return values.get(key, default)

    monkeypatch.setattr(preflight_mod, "config", fake_config)


def _prodlike_ready() -> dict:
    """A config map representing a healthy production-like deploy."""
    return {
        "app.key": "base64:" + "x" * 43,
        "app.env": "production",
        "app.debug": False,
        "auth.default": "jwt",
        "auth.guards": {"jwt": {"secret": "z" * 48}},
        "database.default": "app",
        "database.drivers": {
            "app": {"driver": "postgres", "host": "db.internal", "database": "cheapa"}
        },
        "cache.default": "redis",
        "cache.drivers": {"redis": {"host": "redis.internal"}},
        "queue.default": "amqp",
        "queue.drivers": {"amqp": {"host": "rabbit.internal"}},
        "meilisearch.url": "http://meili.internal:7700",
    }


def _make_command(options=None) -> PreflightCommand:
    cmd = PreflightCommand(application=None)
    cmd.set_parsed_options(options or {})
    cmd.console = MagicMock()
    return cmd


def _printed(cmd) -> str:
    return " ".join(str(c.args) for c in cmd.console.print.call_args_list)


# ── CheckResult helpers ────────────────────────────────────────────────────


def test_result_helpers_classify():
    assert ok("a").status == OK and not ok("a").failed and not ok("a").warned
    assert warn("b").status == WARN and warn("b").warned and not warn("b").failed
    assert fail("c").status == FAIL and fail("c").failed and not fail("c").warned


# ── check_app_key_set ──────────────────────────────────────────────────────


def test_app_key_ok_when_real(monkeypatch):
    _install_config(monkeypatch, _prodlike_ready())
    assert check_app_key_set().status == OK


def test_app_key_fails_when_empty(monkeypatch):
    cfg = _prodlike_ready()
    cfg["app.key"] = ""
    _install_config(monkeypatch, cfg)
    r = check_app_key_set()
    assert r.failed and "app.key" in r.message.lower() or r.failed


def test_app_key_fails_on_placeholder(monkeypatch):
    cfg = _prodlike_ready()
    cfg["app.key"] = "changeme"
    _install_config(monkeypatch, cfg)
    assert check_app_key_set().failed


def test_app_key_fails_on_placeholder_guard_secret(monkeypatch):
    cfg = _prodlike_ready()
    cfg["auth.guards"] = {"jwt": {"secret": "your-secret-key"}}
    _install_config(monkeypatch, cfg)
    assert check_app_key_set().failed


def test_app_key_fails_on_short_guard_secret(monkeypatch):
    cfg = _prodlike_ready()
    cfg["auth.guards"] = {"jwt": {"secret": "tooshort"}}
    _install_config(monkeypatch, cfg)
    r = check_app_key_set()
    assert r.failed and "32" in r.message


def test_app_key_ok_when_no_guard_configured(monkeypatch):
    # A service with no auth guard (auth.default unset) should still pass on a
    # real app.key — the guard-secret arm is skipped.
    cfg = _prodlike_ready()
    cfg["auth.default"] = None
    cfg["auth.guards"] = {}
    _install_config(monkeypatch, cfg)
    assert check_app_key_set().status == OK


# ── check_required_config_present ───────────────────────────────────────────


def test_required_config_ok(monkeypatch):
    _install_config(monkeypatch, _prodlike_ready())
    assert check_required_config_present().status == OK


def test_required_config_fails_on_missing_db_host(monkeypatch):
    cfg = _prodlike_ready()
    cfg["database.drivers"] = {"app": {"driver": "postgres", "host": "", "database": "cheapa"}}
    _install_config(monkeypatch, cfg)
    r = check_required_config_present()
    assert r.failed and "database host" in r.message


def test_required_config_fails_on_missing_meili_url(monkeypatch):
    cfg = _prodlike_ready()
    cfg["meilisearch.url"] = ""
    _install_config(monkeypatch, cfg)
    r = check_required_config_present()
    assert r.failed and "meilisearch url" in r.message


def test_required_config_fails_on_missing_redis_host(monkeypatch):
    cfg = _prodlike_ready()
    cfg["cache.drivers"] = {"redis": {}}
    _install_config(monkeypatch, cfg)
    r = check_required_config_present()
    assert r.failed and "redis cache host" in r.message


def test_required_config_fails_on_missing_rabbitmq_host(monkeypatch):
    cfg = _prodlike_ready()
    cfg["queue.drivers"] = {"amqp": {}}
    _install_config(monkeypatch, cfg)
    r = check_required_config_present()
    assert r.failed and "rabbitmq/queue host" in r.message


# ── check_debug_off_in_prod ────────────────────────────────────────────────


def test_debug_off_in_prod_ok(monkeypatch):
    _install_config(monkeypatch, _prodlike_ready())
    assert check_debug_off_in_prod().status == OK


def test_debug_on_in_prod_fails(monkeypatch):
    cfg = _prodlike_ready()
    cfg["app.debug"] = True
    _install_config(monkeypatch, cfg)
    r = check_debug_off_in_prod()
    assert r.failed and "production-like" in r.message


def test_debug_on_in_staging_fails(monkeypatch):
    cfg = _prodlike_ready()
    cfg["app.env"] = "staging"
    cfg["app.debug"] = True
    _install_config(monkeypatch, cfg)
    assert check_debug_off_in_prod().failed


def test_debug_on_locally_is_ok(monkeypatch):
    cfg = _prodlike_ready()
    cfg["app.env"] = "local"
    cfg["app.debug"] = True
    _install_config(monkeypatch, cfg)
    assert check_debug_off_in_prod().status == OK


def test_debug_truthy_string_is_detected(monkeypatch):
    cfg = _prodlike_ready()
    cfg["app.debug"] = "true"
    _install_config(monkeypatch, cfg)
    assert check_debug_off_in_prod().failed


# ── handle(): exit codes + reporting ────────────────────────────────────────


def test_handle_all_pass_returns_none(monkeypatch):
    _install_config(monkeypatch, _prodlike_ready())
    cmd = _make_command()
    assert cmd.handle() is None  # no int return → exit 0
    assert "passed" in _printed(cmd).lower()


def test_handle_fails_loudly_with_nonzero_exit(monkeypatch):
    cfg = _prodlike_ready()
    cfg["app.debug"] = True
    cfg["app.key"] = ""
    _install_config(monkeypatch, cfg)
    cmd = _make_command()
    assert cmd.handle() == 1  # non-zero exit on failure
    printed = _printed(cmd).lower()
    assert "fail" in printed


def test_warn_only_downgrades_failures_to_warnings(monkeypatch):
    cfg = _prodlike_ready()
    cfg["app.debug"] = True  # would normally fail
    _install_config(monkeypatch, cfg)
    cmd = _make_command(options={"warn_only": True})
    # Downgraded → no non-zero exit.
    assert cmd.handle() is None
    assert "warn" in _printed(cmd).lower()


def test_only_runs_subset(monkeypatch):
    # Break app_key, but --only debug_off_in_prod (which passes) → exit 0.
    cfg = _prodlike_ready()
    cfg["app.key"] = ""
    _install_config(monkeypatch, cfg)
    cmd = _make_command(options={"only": "debug_off_in_prod"})
    assert cmd.handle() is None


def test_only_unknown_name_fails(monkeypatch):
    _install_config(monkeypatch, _prodlike_ready())
    cmd = _make_command(options={"only": "no_such_check"})
    assert cmd.handle() == 1
    assert "unknown" in _printed(cmd).lower()


# ── extensibility ───────────────────────────────────────────────────────────


def test_register_check_is_run(monkeypatch):
    _install_config(monkeypatch, _prodlike_ready())
    cmd = _make_command()
    cmd.register_check("custom_gate", lambda: fail("nope"))
    assert cmd.handle() == 1
    assert "custom_gate" in _printed(cmd)


def test_registering_does_not_mutate_default_registry(monkeypatch):
    _install_config(monkeypatch, _prodlike_ready())
    cmd = _make_command()
    cmd.register_check("custom_gate", lambda: ok("fine"))
    other = _make_command()
    assert "custom_gate" not in other.checks


def test_check_that_raises_is_treated_as_failure(monkeypatch):
    _install_config(monkeypatch, _prodlike_ready())
    cmd = _make_command()

    def _boom() -> CheckResult:
        raise RuntimeError("kaboom")

    cmd.register_check("boom_gate", _boom)
    assert cmd.handle() == 1
    assert "kaboom" in _printed(cmd) or "boom_gate" in _printed(cmd)
