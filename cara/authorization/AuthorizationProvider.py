"""
Authorization service provider.
"""

from cara.authorization import Gate
from cara.configuration import config
from cara.facades import Auth
from cara.foundation import DeferredProvider


class AuthorizationProvider(DeferredProvider):
    """
    Authorization service provider for dependency injection.
    """

    @classmethod
    def provides(cls) -> list[str]:
        return ["gate"]

    def register(self):
        """Register authorization services with configuration."""
        settings = config("authorization", {})

        # Register gate service
        self._add_gate_service(settings)

    def _add_gate_service(self, settings: dict) -> None:
        """Register Gate service with configuration."""
        gate = Gate(
            user_resolver=lambda: Auth.user(),
            # Add any gate configuration here if needed
            # e.g., default_policies=settings.get("policies", {}),
        )
        self.application.bind("gate", gate)
