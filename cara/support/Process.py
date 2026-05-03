"""Process — fluent subprocess runner.

Laravel 10's ``Illuminate\\Process\\Factory`` parity. Builds a
``subprocess`` invocation through a chainable API and returns a
:class:`ProcessResult` with structured success / output access::

    result = (
        Process.command(["git", "rev-parse", "HEAD"])
        .path("/repo")
        .timeout(5)
        .run()
    )
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
from typing import Mapping, Optional, Sequence, Union


class ProcessResult:
    """Structured result of a subprocess run — Laravel ``ProcessResult`` parity."""

    __slots__ = ("_command", "_exit_code", "_stdout", "_stderr")

    def __init__(
        self,
        command: Sequence[str],
        exit_code: int,
        stdout: str,
        stderr: str,
    ) -> None:
        self._command = list(command)
        self._exit_code = exit_code
        self._stdout = stdout
        self._stderr = stderr

    def command(self) -> list:
        return list(self._command)

    def exit_code(self) -> int:
        return self._exit_code

    def output(self) -> str:
        """stdout — Laravel ``output()``."""
        return self._stdout

    def error_output(self) -> str:
        """stderr — Laravel ``errorOutput()``."""
        return self._stderr

    def successful(self) -> bool:
        return self._exit_code == 0

    def failed(self) -> bool:
        return self._exit_code != 0

    def throw_on_failure(self) -> "ProcessResult":
        """Raise :class:`ProcessFailedException` if the process failed."""
        if self.failed():
            raise ProcessFailedException(self)
        return self

    def __repr__(self) -> str:  # pragma: no cover — debug aid
        return f"ProcessResult(exit={self._exit_code}, cmd={self._command!r})"


class ProcessFailedException(RuntimeError):
    """Raised by :meth:`ProcessResult.throw_on_failure` for non-zero exits."""

    def __init__(self, result: ProcessResult) -> None:
        self.result = result
        super().__init__(
            f"Process {result.command()!r} exited with code {result.exit_code()}: "
            f"{result.error_output().strip() or '<no stderr>'}"
        )


class Process:
    """Fluent subprocess builder."""

    __slots__ = ("_command", "_path", "_env", "_timeout", "_input")

    def __init__(self, command: Sequence[str]) -> None:
        if not command:
            raise ValueError("Process.command(...) needs at least one argument")
        self._command: list = list(command)
        self._path: Optional[str] = None
        self._env: Optional[dict] = None
        self._timeout: Optional[float] = None
        self._input: Optional[str] = None

    @classmethod
    def command(cls, command: Union[str, Sequence[str]]) -> "Process":
        """Start building a process from ``command``.

        Accepts a string (split on whitespace) or a pre-tokenised
        sequence. Prefer the sequence form — it sidesteps shell
        quoting bugs.
        """
        if isinstance(command, str):
            return cls(command.split())
        return cls(command)

    # ── Builders ────────────────────────────────────────────────────

    def path(self, cwd: str) -> "Process":
        """Set the working directory."""
        self._path = cwd
        return self

    def env(self, env: Mapping[str, str], *, replace: bool = False) -> "Process":
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

    def timeout(self, seconds: float) -> "Process":
        """Set a wall-clock timeout. ``0`` / negative disables."""
        self._timeout = seconds if seconds and seconds > 0 else None
        return self

    def input(self, data: str) -> "Process":
        """Pipe ``data`` to the process's stdin."""
        self._input = data
        return self

    # ── Terminals ───────────────────────────────────────────────────

    def run(self) -> ProcessResult:
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
            return ProcessResult(
                self._command,
                exit_code=124,  # GNU timeout convention
                stdout=(e.stdout.decode() if isinstance(e.stdout, bytes) else (e.stdout or "")),
                stderr=f"Timeout after {self._timeout}s",
            )

        return ProcessResult(
            self._command,
            exit_code=completed.returncode,
            stdout=completed.stdout or "",
            stderr=completed.stderr or "",
        )

    def must_run(self) -> ProcessResult:
        """Run and raise on failure — Laravel ``mustRun()``."""
        return self.run().throw_on_failure()

    # ── Misc ────────────────────────────────────────────────────────

    def __repr__(self) -> str:  # pragma: no cover — debug aid
        return f"Process(cmd={self._command!r}, cwd={self._path!r})"


__all__ = ["Process", "ProcessResult", "ProcessFailedException"]
