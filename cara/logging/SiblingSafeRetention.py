"""Loguru retention that survives a sibling process."""

from __future__ import annotations

from contextlib import suppress
from typing import Any

from loguru._file_sink import FileSink


class SiblingSafeRetention:
    """Loguru's own retention, tolerant of a sibling deleting the same file.

    Each process's retention sees every process's files, and at 00:00 they all
    run within milliseconds of each other. Loguru does not expect a file to
    vanish between its listing and its ``stat``: the error lands in the middle
    of a rotation, before the next file is opened, and that process then loses
    every line after it. Whoever lost the race leaves the rest to the winner.
    """

    def __init__(self, retention: Any) -> None:
        self._expire = FileSink._make_retention_function(retention)

    def __call__(self, logs: list[str]) -> None:
        with suppress(FileNotFoundError):
            self._expire(logs)


__all__ = ["SiblingSafeRetention"]
