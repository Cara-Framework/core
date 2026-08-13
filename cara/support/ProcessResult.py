"""Canonical definition of ``ProcessResult``."""

from __future__ import annotations

from collections.abc import Sequence


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

    def throw_on_failure(self) -> ProcessResult:
        """Raise :class:`ProcessFailedException` if the process failed."""
        if self.failed():
            from .ProcessFailedException import (
                ProcessFailedException,  # local: cycle with cara.support.ProcessFailedException
            )

            raise ProcessFailedException(self)
        return self

    def __repr__(self) -> str:  # pragma: no cover — debug aid
        return f"ProcessResult(exit={self._exit_code}, cmd={self._command!r})"
