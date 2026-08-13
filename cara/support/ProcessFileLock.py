"""Cross-platform advisory file locks shared by filesystem authorities."""

from __future__ import annotations

import errno
import os
import time
from importlib import import_module
from pathlib import Path
from types import TracebackType
from typing import BinaryIO, Self

_LOCK_API = import_module("msvcrt" if os.name == "nt" else "fcntl")
_CONTENTION_ERRNOS = frozenset({errno.EACCES, errno.EAGAIN, errno.EDEADLK})


class ProcessFileLock:
    """Own an exclusive advisory lock for the lifetime of a context.

    ``timeout_seconds=None`` waits indefinitely. A finite timeout raises
    ``TimeoutError``; unrelated filesystem errors propagate immediately.
    """

    def __init__(
        self,
        path: str | Path,
        *,
        timeout_seconds: int | float | None = None,
        poll_seconds: int | float = 0.05,
    ) -> None:
        if timeout_seconds is not None and (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, int | float)
            or timeout_seconds <= 0
        ):
            raise ValueError("timeout_seconds must be positive or None")
        if (
            isinstance(poll_seconds, bool)
            or not isinstance(poll_seconds, int | float)
            or poll_seconds <= 0
        ):
            raise ValueError("poll_seconds must be positive")
        self._path = Path(path)
        self._timeout_seconds = timeout_seconds
        self._poll_seconds = float(poll_seconds)
        self._handle: BinaryIO | None = None

    def __enter__(self) -> Self:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        handle = self._path.open("a+b")
        self._handle = handle
        try:
            if os.name == "nt":  # pragma: no cover - Windows CI only
                handle.seek(0)
                if handle.read(1) == b"":
                    handle.write(b"\0")
                    handle.flush()
            deadline = (
                None
                if self._timeout_seconds is None
                else time.monotonic() + float(self._timeout_seconds)
            )
            while True:
                try:
                    self._try_acquire(handle)
                    return self
                except (BlockingIOError, OSError) as exc:
                    if not isinstance(exc, BlockingIOError) and (
                        exc.errno not in _CONTENTION_ERRNOS
                    ):
                        raise
                    if deadline is not None and time.monotonic() >= deadline:
                        raise TimeoutError(
                            f"Timed out waiting for process file lock: {self._path}"
                        ) from exc
                    time.sleep(self._poll_seconds)
        except BaseException:
            handle.close()
            self._handle = None
            raise

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc, traceback
        handle = self._handle
        if handle is None:
            raise RuntimeError("ProcessFileLock is not acquired")
        try:
            self._release(handle)
        finally:
            handle.close()
            self._handle = None

    @staticmethod
    def _try_acquire(handle: BinaryIO) -> None:
        if os.name == "nt":  # pragma: no cover - Windows CI only
            handle.seek(0)
            _LOCK_API.locking(handle.fileno(), _LOCK_API.LK_NBLCK, 1)
            return
        _LOCK_API.flock(handle.fileno(), _LOCK_API.LOCK_EX | _LOCK_API.LOCK_NB)

    @staticmethod
    def _release(handle: BinaryIO) -> None:
        if os.name == "nt":  # pragma: no cover - Windows CI only
            handle.seek(0)
            _LOCK_API.locking(handle.fileno(), _LOCK_API.LK_UNLCK, 1)
            return
        _LOCK_API.flock(handle.fileno(), _LOCK_API.LOCK_UN)
