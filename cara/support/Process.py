"""Process — fluent subprocess runner.

Laravel 10's ``Illuminate\\Process\\Factory`` parity. Builds a
``subprocess`` invocation through a chainable API and returns a
:class:`ProcessResult` with structured success / output access::

    result = Process.command(["git", "rev-parse", "HEAD"]).path("/repo").timeout(5).run()
    assert result.successful()
    sha = result.output().strip()

    # Pipe stdin.
    Process.command(["jq", ".name"]).input('{"name":"x"}').run()

    # Throw on non-zero exit.
    Process.command(["mkdir", "/tmp/x"]).run().throw_on_failure()

Designed for short-running commands that fit in memory. For
long-running streamed work (e.g. tailing a log) drop down to
``subprocess.Popen`` directly — adding async streaming to this
facade is out of scope.
"""

from __future__ import annotations

import os
import subprocess
from collections.abc import Mapping, Sequence

from cara.exceptions import InvalidArgumentException

from .ProcessResult import ProcessResult as _ProcessResult


class Process:
    """Fluent subprocess builder."""

    __slots__ = ("_command", "_path", "_env", "_timeout", "_input")

    def __init__(self, command: Sequence[str]) -> None:
        if not command:
            raise InvalidArgumentException(
                "Process.command(...) needs at least one argument"
            )
        self._command: list = list(command)
        self._path: str | None = None
        self._env: dict | None = None
        self._timeout: float | None = None
        self._input: str | None = None

    @classmethod
    def command(cls, command: str | Sequence[str]) -> Process:
        """Start building a process from ``command``.

        Accepts a string (split on whitespace) or a pre-tokenised
        sequence. Prefer the sequence form — it sidesteps shell
        quoting bugs.
        """
        if isinstance(command, str):
            return cls(command.split())
        return cls(command)

    # ── Builders ────────────────────────────────────────────────────

    def path(self, cwd: str) -> Process:
        """Set the working directory."""
        self._path = cwd
        return self

    def env(self, env: Mapping[str, str], *, replace: bool = False) -> Process:
        """Set / merge environment variables.

        ``replace=True`` discards the inherited environment entirely
        (Laravel's ``env`` method overrides; here we expose both
        merge — the safer default — and full-replace).
        """
        if replace:
            self._env = dict(env)
        else:
            merged = dict(os.environ)
            merged.update(env)
            self._env = merged
        return self

    def timeout(self, seconds: float) -> Process:
        """Set a wall-clock timeout. ``0`` / negative disables."""
        self._timeout = seconds if seconds and seconds > 0 else None
        return self

    def input(self, data: str) -> Process:
        """Pipe ``data`` to the process's stdin."""
        self._input = data
        return self

    # ── Terminals ───────────────────────────────────────────────────

    def run(self) -> _ProcessResult:
        """Run the process synchronously and return a :class:`ProcessResult`."""
        try:
            completed = subprocess.run(
                self._command,
                cwd=self._path,
                env=self._env,
                input=self._input,
                capture_output=True,
                text=True,
                timeout=self._timeout,
                check=False,
            )
        except subprocess.TimeoutExpired as e:
            # Surface timeouts as a failed result so callers can branch
            # uniformly through ``failed()`` instead of catching two
            # different exception types.
            return _ProcessResult(
                self._command,
                exit_code=124,  # GNU timeout convention
                stdout=(
                    e.stdout.decode() if isinstance(e.stdout, bytes) else (e.stdout or "")
                ),
                stderr=f"Timeout after {self._timeout}s",
            )

        return _ProcessResult(
            self._command,
            exit_code=completed.returncode,
            stdout=completed.stdout or "",
            stderr=completed.stderr or "",
        )

    def must_run(self) -> _ProcessResult:
        """Run and raise on failure — Laravel ``mustRun()``."""
        return self.run().throw_on_failure()

    # ── Misc ────────────────────────────────────────────────────────

    def __repr__(self) -> str:  # pragma: no cover — debug aid
        return f"Process(cmd={self._command!r}, cwd={self._path!r})"


__all__ = ["Process"]
