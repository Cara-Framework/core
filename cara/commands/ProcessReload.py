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
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time

from cara.facades import Log

#: Seconds between boot probes of a tree that did not import.
PROBE_INTERVAL_SECONDS = 5.0

#: A probe that has not finished by then counts as a failure.
PROBE_TIMEOUT_SECONDS = 180.0


def boot_failure(argv: list[str]) -> str | None:
    """Why the entry point ``argv[:2]`` does not boot, or ``None`` when it does."""
    try:
        probe = subprocess.run(
            [*argv[:2], "--help"],
            capture_output=True,
            text=True,
            timeout=PROBE_TIMEOUT_SECONDS,
            check=False,
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
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    signal.signal(signal.SIGINT, signal.default_int_handler)
    reported: str | None = None
    while (failure := boot_failure(argv)) is not None:
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
    os.execv(sys.executable, argv)


__all__ = ["boot_failure", "replace_process_when_bootable"]
