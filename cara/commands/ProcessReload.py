"""Replace a reloading long-running command only with a tree that boots.

``schedule:work`` and ``queue:work`` reload (development auto-reload) by
stopping their loop and replacing the process with ``os.execv``, so every
service provider re-bootstraps. Replacing it with a tree saved mid-edit killed
the role outright: the new interpreter died at ``from bootstrap import
application`` and nothing started it again. Live 2026-11-11 20:04: a services
edit left a circular import for a moment, the scheduler re-executed into it and
stayed down until a manual restart at 01:51 — 5h47m with no recurring sync.

:func:`replace_process_when_bootable` probes the tree first — a child runs the
same entry point with ``--help``, which imports and registers what a real start
does — and only a clean probe lets ``execv`` happen. A broken tree is reported
once (again only when the failure changes) and probed every
:data:`PROBE_INTERVAL_SECONDS`, so the role comes back by itself once the edit
is finished. The loop runs under default signal dispositions, so a stop request
while it waits ends the process as it would have before.

A clean probe only speaks for the tree it imported. The probe takes seconds,
and an edit saved meanwhile replaced the role with a tree nobody had probed —
live 2026-11-13 15:44 (``ImportError: emit_order_change_webhooks``) and 17:22
(``release_referenced_notification_delivery``): the probe passed, the edit
landed, ``execv`` ran into it and the scheduler stayed down until a manual
restart (no verify sweep 15:20-16:05). So the source tree is fingerprinted
around every probe, and a probe whose tree changed under it is run again
before ``execv``.

That closes the probe's window, not the replacement's: the new interpreter
imports the tree for as long as a probe does, and an edit saved then still
killed it at ``from bootstrap import application`` with nothing to start it
again. So ``execv`` carries :data:`BOOT_GUARD_ENV`; :func:`guard_reloaded_boot`
(installed when ``cara.commands`` is first imported, before the entry script
imports ``bootstrap``) turns an uncaught boot failure of such a process back
into the wait-for-a-bootable-tree loop, and :func:`reloaded_boot_completed`
drops the guard once the role is running, so a later crash exits as before.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

from cara.facades import Log

#: Set across a reload's ``execv``; present only while that process boots.
BOOT_GUARD_ENV = "CARA_RELOAD_BOOTING"

#: Seconds between boot probes of a tree that did not import.
PROBE_INTERVAL_SECONDS = 5.0

#: A probe that has not finished by then counts as a failure.
PROBE_TIMEOUT_SECONDS = 180.0

#: Directories no boot imports from; skipped when fingerprinting the tree.
_UNIMPORTED_DIRS = frozenset(
    {"venv", "node_modules", "__pycache__", "storage", "tests", "logs"}
)


def source_fingerprint(root: Path) -> frozenset[tuple[str, int, int]]:
    """``(path, mtime_ns, size)`` of every ``.py`` file a boot from ``root``
    can import, following the kernel and framework symlinks once each."""
    entries: set[tuple[str, int, int]] = set()
    seen: set[str] = set()
    for directory, dirs, files in os.walk(root, followlinks=True):
        real = os.path.realpath(directory)
        if real in seen:
            dirs[:] = []
            continue
        seen.add(real)
        dirs[:] = [
            name
            for name in dirs
            if name not in _UNIMPORTED_DIRS and not name.startswith(".")
        ]
        for name in files:
            if not name.endswith(".py"):
                continue
            path = os.path.join(directory, name)
            try:
                stat = os.stat(path)
            except OSError:
                continue  # removed between listing and stat: the next walk sees it
            entries.add((path, stat.st_mtime_ns, stat.st_size))
    return frozenset(entries)


def boot_failure(argv: list[str]) -> str | None:
    """Why the entry point ``argv[:2]`` does not boot, or ``None`` when it does."""
    try:
        probe = subprocess.run(
            [*argv[:2], "--help"],
            capture_output=True,
            text=True,
            timeout=PROBE_TIMEOUT_SECONDS,
            check=False,
            # A probe that inherited the boot guard would itself wait for
            # a bootable tree instead of reporting that it is not one.
            env={k: v for k, v in os.environ.items() if k != BOOT_GUARD_ENV},
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return f"{type(exc).__name__}: {exc}"
    if probe.returncode == 0:
        return None
    lines = [
        line.strip()
        for line in (probe.stderr or probe.stdout or "").splitlines()
        if line.strip()
    ]
    return lines[-1][:500] if lines else f"exit status {probe.returncode}"


def replace_process_when_bootable() -> None:
    """``execv`` this command again with the same arguments, once it boots."""
    argv = [sys.executable, *sys.argv]
    root = Path(sys.argv[0]).resolve().parent
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    signal.signal(signal.SIGINT, signal.default_int_handler)
    reported: str | None = None
    while True:
        probed_tree = source_fingerprint(root)
        failure = boot_failure(argv)
        if source_fingerprint(root) != probed_tree:
            # The verdict is about a tree that is already gone (an edit
            # landed mid-probe): probe the tree as it is now.
            if reported != "changed":
                Log.info(
                    "Code reload: the tree changed while it was probed; probing "
                    "it again before the process restarts.",
                    category="cara.reload",
                )
                reported = "changed"
            continue
        if failure is None:
            break
        if failure != reported:
            Log.error(
                "Code reload is waiting: the edited tree does not boot (%s). "
                "Probing again every %ss; the process restarts by itself once "
                "it imports.",
                failure,
                PROBE_INTERVAL_SECONDS,
                category="cara.reload",
            )
            reported = failure
        time.sleep(PROBE_INTERVAL_SECONDS)
    os.environ[BOOT_GUARD_ENV] = "1"
    os.execv(sys.executable, argv)


def _reloaded_boot_failed(kind, value, traceback) -> None:
    """``sys.excepthook`` of a process a reload ``execv``'d into, while it boots."""
    sys.__excepthook__(kind, value, traceback)
    if os.environ.get(BOOT_GUARD_ENV) == "1" and issubclass(kind, Exception):
        replace_process_when_bootable()


def guard_reloaded_boot() -> None:
    """In a process a reload ``execv``'d into, a boot that dies on an uncaught
    exception waits for a bootable tree again instead of exiting."""
    if os.environ.get(BOOT_GUARD_ENV) == "1":
        sys.excepthook = _reloaded_boot_failed


def reloaded_boot_completed() -> None:
    """The role is running: from here a crash exits like any other."""
    os.environ.pop(BOOT_GUARD_ENV, None)
    if sys.excepthook is _reloaded_boot_failed:
        sys.excepthook = sys.__excepthook__


__all__ = [
    "BOOT_GUARD_ENV",
    "boot_failure",
    "guard_reloaded_boot",
    "reloaded_boot_completed",
    "replace_process_when_bootable",
]
