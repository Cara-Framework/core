"""
Validation Provider for the Cara framework.

This module provides the service provider that registers the validation subsystem,
including all validation rules.
"""

from __future__ import annotations

from cara.foundation import DeferredProvider
from cara.validation.Validation import Validation


class ValidationProvider(DeferredProvider):
    """
    Deferred provider for the Validation subsystem.

    Registers the validation service.
    """

    @classmethod
    def provides(cls) -> list[str]:
        return ["validation"]

    def register(self) -> None:
        """Register the validation service."""
        self.application.bind("validation", Validation())

    def boot(self) -> None:
        """No actions required at boot time."""
        pass
