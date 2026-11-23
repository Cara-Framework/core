"""In-memory fake for the ``Log`` facade — records every call by level.

Tests can assert on what was logged without polluting stdout or files.
Mirrors the surface used across the codebase: ``debug``, ``info``,
``warning``, ``error``, ``critical``, ``exception`` plus the optional
``category=`` kwarg Cara's logger accepts.

Supports both direct string messages and printf-style format strings:
  Log.warning("something happened")
  Log.warning("value is %s and %d", val, num, category="x")
"""

from __future__ import annotations

from typing import Any

from .LogRecord import LogRecord


class LogFake:
    """A drop-in replacement for the ``Log`` facade in tests."""

    LEVELS = ("debug", "info", "warning", "error", "critical", "exception")

    def __init__(self) -> None:
        self.records: list[LogRecord] = []

    # ── Facade-compatible methods ────────────────────────────────────
    #
    # Each level takes the REAL ``Logger`` method's keywords and no others
    # (``tests/testing/test_fake_facade_parity.py`` pins the two together).
    # They used to take ``**kwargs``, so ``Log.error("failed", order_id=7)``
    # was recorded here while the real logger raised ``TypeError`` — on the
    # error path, where the log line was the only report of the failure.

    def _record(
        self,
        level: str,
        message: Any,
        args: tuple[Any, ...],
        category: str | None,
        **extra: Any,
    ) -> None:
        text = str(message)
        if args:
            try:
                text = text % args
            except TypeError, ValueError:
                text = f"{text} {' '.join(str(a) for a in args)}"
        self.records.append(
            LogRecord(
                level=level,
                message=text,
                category=category,
                extra={key: value for key, value in extra.items() if value is not None},
            )
        )

    def debug(
        self,
        message: Any,
        *args: Any,
        category: str | None = None,
        color: str | None = None,
        exc_info: Any = None,
        _module_override: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        del exc_info
        self._record(
            "debug",
            message,
            args,
            category,
            color=color,
            _module_override=_module_override,
            context=context,
        )

    def info(
        self,
        message: Any,
        *args: Any,
        category: str | None = None,
        color: str | None = None,
        exc_info: Any = None,
        _module_override: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        del exc_info
        self._record(
            "info",
            message,
            args,
            category,
            color=color,
            _module_override=_module_override,
            context=context,
        )

    def warning(
        self,
        message: Any,
        *args: Any,
        category: str | None = None,
        color: str | None = None,
        exc_info: Any = None,
        _module_override: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        del exc_info
        self._record(
            "warning",
            message,
            args,
            category,
            color=color,
            _module_override=_module_override,
            context=context,
        )

    def error(
        self,
        message: Any,
        *args: Any,
        category: str | None = None,
        color: str | None = None,
        exception: Exception | None = None,
        exc_info: Any = None,
        _module_override: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        # ``exception=`` is ``Logger.error``'s alone, as in production.
        del exc_info
        self._record(
            "error",
            message,
            args,
            category,
            color=color,
            exception=exception,
            _module_override=_module_override,
            context=context,
        )

    def critical(
        self,
        message: Any,
        *args: Any,
        category: str | None = None,
        color: str | None = None,
        exc_info: Any = None,
        _module_override: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        del exc_info
        self._record(
            "critical",
            message,
            args,
            category,
            color=color,
            _module_override=_module_override,
            context=context,
        )

    def exception(
        self,
        message: Any,
        *args: Any,
        category: str | None = None,
        exc_info: Any = True,
        _module_override: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        # No ``color``: the real ``Logger.exception`` does not take one.
        del exc_info
        self._record(
            "exception",
            message,
            args,
            category,
            _module_override=_module_override,
            context=context,
        )

    def with_context(self, **context: Any) -> _FakeContextualLogger:
        """Return a scoped fake logger that appends context tags.

        Mirrors the real ``Logger.with_context`` so production code paths
        like ``BaseJob`` (``Log.with_context(job_id=...).info(...)``)
        round-trip under tests — every message gets a ``[k=v]`` suffix
        and lands in this fake's ``records`` list under the same level.
        """
        return _FakeContextualLogger(self, context)

    # ── Test-time helpers ────────────────────────────────────────────

    def recorded(self, level: str | None = None) -> list[LogRecord]:
        """Return all records, optionally filtered by level."""
        if level is None:
            return list(self.records)
        return [r for r in self.records if r.level == level]

    def has(self, level: str, contains: str) -> bool:
        """``True`` if any record at ``level`` contains the substring."""
        return any(contains in r.message for r in self.records if r.level == level)

    def count(self, level: str | None = None) -> int:
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

    def debug(self, message: Any, *args: Any, **kwargs: Any) -> None:
        self._parent.debug(self._fmt(message), *args, **kwargs)

    def info(self, message: Any, *args: Any, **kwargs: Any) -> None:
        self._parent.info(self._fmt(message), *args, **kwargs)

    def warning(self, message: Any, *args: Any, **kwargs: Any) -> None:
        self._parent.warning(self._fmt(message), *args, **kwargs)

    def error(self, message: Any, *args: Any, **kwargs: Any) -> None:
        self._parent.error(self._fmt(message), *args, **kwargs)

    def critical(self, message: Any, *args: Any, **kwargs: Any) -> None:
        self._parent.critical(self._fmt(message), *args, **kwargs)

    def exception(self, message: Any, *args: Any, **kwargs: Any) -> None:
        self._parent.exception(self._fmt(message), *args, **kwargs)
