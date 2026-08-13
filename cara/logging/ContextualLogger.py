"""Canonical definition of ``ContextualLogger``."""

from __future__ import annotations

from typing import Any

from cara.logging.contracts import LoggerContract


class ContextualLogger:
    """Scoped logger that appends context tags to every message."""

    __slots__ = ("_parent", "_context", "_suffix")

    def __init__(self, parent: LoggerContract, context: dict) -> None:
        self._parent = parent
        self._context = context
        self._suffix = (
            " ".join(f"[{k}={v}]" for k, v in context.items()) if context else ""
        )

    def _fmt(self, message: str) -> str:
        return f"{message} {self._suffix}" if self._suffix else message

    def debug(self, message: str, *a: Any, **kw: Any) -> None:
        self._parent.debug(self._fmt(message), *a, **kw)

    def info(self, message: str, *a: Any, **kw: Any) -> None:
        self._parent.info(self._fmt(message), *a, **kw)

    def warning(self, message: str, *a: Any, **kw: Any) -> None:
        self._parent.warning(self._fmt(message), *a, **kw)

    def error(self, message: str, *a: Any, **kw: Any) -> None:
        self._parent.error(self._fmt(message), *a, **kw)

    def critical(self, message: str, *a: Any, **kw: Any) -> None:
        self._parent.critical(self._fmt(message), *a, **kw)

    def exception(self, message: str, *a: Any, **kw: Any) -> None:
        self._parent.exception(self._fmt(message), *a, **kw)
