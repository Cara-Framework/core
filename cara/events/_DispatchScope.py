"""Per-task event-dispatch cycle scope."""

from __future__ import annotations

import contextlib
from collections.abc import Iterator
from contextvars import ContextVar

_dispatch_stack: ContextVar[tuple[str, ...]] = ContextVar(
    "cara_event_dispatch_stack",
    default=(),
)


@contextlib.contextmanager
def _fresh_dispatch_scope() -> Iterator[None]:
    """Run a job boundary with a fresh dispatch stack, then restore it."""
    token = _dispatch_stack.set(())
    try:
        yield
    finally:
        _dispatch_stack.reset(token)
