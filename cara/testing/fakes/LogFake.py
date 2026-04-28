"""In-memory fake for the ``Log`` facade — records every call by level.

Tests can assert on what was logged without polluting stdout or files.
Mirrors the surface used across the codebase: ``debug``, ``info``,
``warning``, ``error``, ``critical``, ``exception`` plus the optional
``category=`` kwarg Cara's logger accepts.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, List, Optional


@dataclass
class LogRecord:
    """One captured log call."""

    level: str
    message: str
    category: Optional[str] = None
    extra: dict = field(default_factory=dict)


class LogFake:
    """A drop-in replacement for the ``Log`` facade in tests."""

    LEVELS = ("debug", "info", "warning", "error", "critical", "exception")

    def __init__(self) -> None:
        self.records: List[LogRecord] = []

    # ── Facade-compatible methods ────────────────────────────────────

    def _record(self, level: str, message: Any, **kwargs: Any) -> None:
        category = kwargs.pop("category", None)
        self.records.append(
            LogRecord(level=level, message=str(message), category=category, extra=kwargs)
        )

    def debug(self, message: Any, **kwargs: Any) -> None:
        self._record("debug", message, **kwargs)

    def info(self, message: Any, **kwargs: Any) -> None:
        self._record("info", message, **kwargs)

    def warning(self, message: Any, **kwargs: Any) -> None:
        self._record("warning", message, **kwargs)

    def error(self, message: Any, **kwargs: Any) -> None:
        self._record("error", message, **kwargs)

    def critical(self, message: Any, **kwargs: Any) -> None:
        self._record("critical", message, **kwargs)

    def exception(self, message: Any, **kwargs: Any) -> None:
        self._record("exception", message, **kwargs)

    def withContext(self, **context: Any) -> "_FakeContextualLogger":
        """Return a scoped fake logger that appends context tags.

        Mirrors the real ``Logger.withContext`` so production code paths
        like ``BaseJob`` (``Log.withContext(job_id=...).info(...)``)
        round-trip under tests — every message gets a ``[k=v]`` suffix
        and lands in this fake's ``records`` list under the same level.
        """
        return _FakeContextualLogger(self, context)

    # ── Test-time helpers ────────────────────────────────────────────

    def recorded(self, level: Optional[str] = None) -> List[LogRecord]:
        """Return all records, optionally filtered by level."""
        if level is None:
            return list(self.records)
        return [r for r in self.records if r.level == level]

    def has(self, level: str, contains: str) -> bool:
        """``True`` if any record at ``level`` contains the substring."""
        return any(contains in r.message for r in self.records if r.level == level)

    def count(self, level: Optional[str] = None) -> int:
        return len(self.recorded(level))

    def assert_logged(self, level: str, contains: str) -> None:
        if not self.has(level, contains):
            msgs = "\n".join(f"[{r.level}] {r.message}" for r in self.records)
            raise AssertionError(
                f"Expected a {level} log containing {contains!r}.\n"
                f"Got {len(self.records)} record(s):\n{msgs or '  <none>'}"
            )

    def assert_nothing_logged(self) -> None:
        if self.records:
            msgs = "\n".join(f"[{r.level}] {r.message}" for r in self.records)
            raise AssertionError(f"Expected no logs, got:\n{msgs}")

    def clear(self) -> None:
        self.records.clear()


class _FakeContextualLogger:
    """Fake counterpart to :class:`cara.logging.Logger.ContextualLogger`."""

    __slots__ = ("_parent", "_suffix")

    def __init__(self, parent: LogFake, context: dict) -> None:
        self._parent = parent
        self._suffix = (
            " ".join(f"[{k}={v}]" for k, v in context.items()) if context else ""
        )

    def _fmt(self, message: Any) -> str:
        text = str(message)
        return f"{text} {self._suffix}" if self._suffix else text

    def debug(self, message: Any, **kwargs: Any) -> None:
        self._parent.debug(self._fmt(message), **kwargs)

    def info(self, message: Any, **kwargs: Any) -> None:
        self._parent.info(self._fmt(message), **kwargs)

    def warning(self, message: Any, **kwargs: Any) -> None:
        self._parent.warning(self._fmt(message), **kwargs)

    def error(self, message: Any, **kwargs: Any) -> None:
        self._parent.error(self._fmt(message), **kwargs)

    def critical(self, message: Any, **kwargs: Any) -> None:
        self._parent.critical(self._fmt(message), **kwargs)

    def exception(self, message: Any, **kwargs: Any) -> None:
        self._parent.exception(self._fmt(message), **kwargs)
