"""Result object returned by AI chat calls."""

from __future__ import annotations

from dataclasses import dataclass

from cara.ai.AIProvider import AIProvider


@dataclass
class AIResponse:
    """Result of a chat completion call."""

    content: str
    model: str
    provider: AIProvider
    tokens_in: int | None = None
    tokens_out: int | None = None
    duration_ms: int | None = None
    # Provider stop reason ("stop", "length", "content_filter", …) — callers
    # that split-retry oversized prompts key off ``finish_reason == "length"``.
    finish_reason: str | None = None
