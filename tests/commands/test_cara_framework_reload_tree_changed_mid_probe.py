"""A clean boot probe of a tree that changed under it does not license ``execv``.

Live 2026-11-13 15:44 and 17:22: the scheduler's reload probe passed, an edit
from another session landed while the probe was importing, ``execv`` ran into
the half-saved tree (``ImportError: emit_order_change_webhooks``) and the role
stayed down — no verify sweep from 15:20 to 16:05. The tree is now
fingerprinted around each probe, and a probe whose tree moved is run again.
"""

from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path
from types import SimpleNamespace

reload_module = importlib.import_module("cara.commands.ProcessReload")


def _wire(monkeypatch, outcomes, fingerprints):
    events: list[str] = []
    outcomes = list(outcomes)
    fingerprints = list(fingerprints)

    def run(argv, **_kwargs):
        events.append("probe")
        return SimpleNamespace(returncode=outcomes.pop(0), stdout="", stderr="boom")

    monkeypatch.setattr(reload_module.subprocess, "run", run)
    monkeypatch.setattr(
        reload_module,
        "source_fingerprint",
        lambda _root: fingerprints.pop(0),
        raising=False,
    )
    monkeypatch.setattr(reload_module.os, "execv", lambda *_a: events.append("execv"))
    monkeypatch.setattr(reload_module.time, "sleep", lambda _s: events.append("sleep"))
    monkeypatch.setattr(reload_module.signal, "signal", lambda *_a, **_k: None)
    monkeypatch.setattr(sys, "argv", ["craft", "schedule:work"])
    # ``execv`` sets the guard in os.environ; teardown restores it to unset.
    monkeypatch.setenv(reload_module.BOOT_GUARD_ENV, "0")
    return events


def test_a_tree_edited_during_a_clean_probe_is_probed_again(monkeypatch):
    # First probe passes, but the tree moved from A to B while it ran.
    events = _wire(monkeypatch, [0, 0], ["A", "B", "B", "B"])

    reload_module.replace_process_when_bootable()

    assert events == ["probe", "probe", "execv"]


def test_a_stable_clean_tree_is_executed_into_at_once(monkeypatch):
    events = _wire(monkeypatch, [0], ["A", "A"])

    reload_module.replace_process_when_bootable()

    assert events == ["probe", "execv"]


def test_the_fingerprint_sees_an_edit_and_follows_symlinks_once(tmp_path):
    kernel = tmp_path / "kernel"
    (kernel / "pkg").mkdir(parents=True)
    module = kernel / "pkg" / "mod.py"
    module.write_text("A = 1\n")
    app = tmp_path / "app"
    app.mkdir()
    (app / "kernel").symlink_to(kernel)
    (app / "kernel_again").symlink_to(kernel)
    (app / "venv").mkdir()
    (app / "venv" / "site.py").write_text("")

    before = reload_module.source_fingerprint(app)
    paths = {Path(entry[0]).name for entry in before}
    assert paths == {"mod.py"}
    assert len(before) == 1

    module.write_text("from nowhere import A\n")
    os.utime(module, ns=(1, 1))
    assert reload_module.source_fingerprint(app) != before


# The replacement interpreter imports the tree for as long as a probe does; an
# edit saved then killed it at ``from bootstrap import application`` (the 15:44
# traceback is that process's, not the probe's). It carries the boot guard.


def test_execv_hands_the_replacement_the_boot_guard(monkeypatch):
    _wire(monkeypatch, [0], ["A", "A"])

    reload_module.replace_process_when_bootable()

    assert os.environ[reload_module.BOOT_GUARD_ENV] == "1"


def test_the_probe_never_inherits_the_boot_guard(monkeypatch):
    seen: list[dict] = []
    monkeypatch.setenv(reload_module.BOOT_GUARD_ENV, "1")

    def run(argv, **kwargs):
        seen.append(kwargs.get("env"))
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(reload_module.subprocess, "run", run)

    assert reload_module.boot_failure(["python", "craft"]) is None
    assert seen[0] is not None
    assert reload_module.BOOT_GUARD_ENV not in seen[0]


def test_a_guarded_boot_that_dies_waits_for_a_bootable_tree(monkeypatch):
    calls: list[str] = []
    monkeypatch.setenv(reload_module.BOOT_GUARD_ENV, "1")
    monkeypatch.setattr(sys, "excepthook", sys.__excepthook__)
    monkeypatch.setattr(sys, "__excepthook__", lambda *_a: calls.append("printed"))
    monkeypatch.setattr(
        reload_module,
        "replace_process_when_bootable",
        lambda: calls.append("waiting"),
    )

    reload_module.guard_reloaded_boot()
    error = ImportError("cannot import name 'emit_order_change_webhooks'")
    sys.excepthook(ImportError, error, None)
    sys.excepthook(KeyboardInterrupt, KeyboardInterrupt(), None)

    assert calls == ["printed", "waiting", "printed"]


def test_a_running_role_drops_the_guard(monkeypatch):
    monkeypatch.setenv(reload_module.BOOT_GUARD_ENV, "1")
    monkeypatch.setattr(sys, "excepthook", sys.__excepthook__)
    reload_module.guard_reloaded_boot()

    reload_module.reloaded_boot_completed()

    assert reload_module.BOOT_GUARD_ENV not in os.environ
    assert sys.excepthook is sys.__excepthook__


def test_an_unguarded_process_keeps_its_excepthook(monkeypatch):
    monkeypatch.delenv(reload_module.BOOT_GUARD_ENV, raising=False)
    monkeypatch.setattr(sys, "excepthook", sys.__excepthook__)

    reload_module.guard_reloaded_boot()

    assert sys.excepthook is sys.__excepthook__
