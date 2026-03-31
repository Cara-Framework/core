"""
Validation Provider for the Cara framework.

This module provides the service provider that configures and registers the validation subsystem,
including all validation rules.
"""

from cara.configuration import config
from cara.foundation import DeferredProvider
from cara.validation import Validation


class ValidationProvider(DeferredProvider):
    """
    Deferred provider for the Validation subsystem.

    Reads configuration and registers the validation service.
    """

    @classmethod
    def provides(cls) -> list[str]:
        return ["validation"]

    def register(self) -> None:
        """Register validation services with configuration."""
        settings = config("validation", {})

        # Register validation service
        self._add_validation_service(settings)

    def _add_validation_service(self, settings: dict) -> None:
        """Register validation service with configuration."""
        # Create validation instance with optional configuration
        validation = Validation(
            # Add any validation configuration here if needed
            # e.g., custom_rules=settings.get("custom_rules", {}),
            # default_messages=settings.get("messages", {}),
        )

        self.application.bind("validation", validation)

    def boot(self) -> None:
        """No actions required at boot time."""
        pass
