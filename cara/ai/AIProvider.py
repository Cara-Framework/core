"""Supported AI providers for the Cara AI client."""

from __future__ import annotations

from enum import StrEnum


class AIProvider(StrEnum):
    """Selects which chat endpoint the AI client dispatches to."""

    OPENROUTER = "openrouter"
    OLLAMA = "ollama"
    OPENAI = "openai"  # generic OpenAI-compatible (LM Studio, vLLM, Together, …)
