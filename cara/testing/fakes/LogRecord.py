"""LogRecord."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class LogRecord:
    """One captured log call."""

    level: str
    message: str
    category: str | None = None
    extra: dict = field(default_factory=dict)
