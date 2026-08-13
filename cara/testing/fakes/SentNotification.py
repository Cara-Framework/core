"""SentNotification."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class SentNotification:
    notifiable: Any
    notification: Any
    channels: list[str] | None = None
