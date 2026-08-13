"""QueuedJob."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class QueuedJob:
    job: Any
    queue: str | None = None
    delay: float | None = None
    payload: dict | None = None
