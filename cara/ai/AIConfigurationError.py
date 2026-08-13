"""AIConfigurationError."""

from __future__ import annotations

from .AIException import AIException


class AIConfigurationError(AIException):
    """Raised when the AI client is asked to run without a resolvable model.

    A model name is a deployment decision (vendor, cost, capability), so the
    framework ships no default for the multi-vendor OpenRouter provider — an
    unconfigured client fails here instead of silently calling whatever model
    cara happened to be pinned to.
    """
