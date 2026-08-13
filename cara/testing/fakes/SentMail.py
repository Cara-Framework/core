"""SentMail."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class SentMail:
    to: list[str] = field(default_factory=list)
    cc: list[str] = field(default_factory=list)
    bcc: list[str] = field(default_factory=list)
    subject: str | None = None
    body: str | None = None
    template: str | None = None
    context: dict = field(default_factory=dict)
    mailable: Any | None = None
