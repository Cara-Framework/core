"""
Lifespan Conductor Provider for the Cara framework.

This module provides the service provider that bootstraps the lifespan event handling system. It
manages application startup and shutdown events through the ASGI lifespan protocol.

The provider ensures proper handling of application lifecycle events in accordance with ASGI
specifications.
"""

from cara.foundation import DeferredProvider
from cara.conductors.lifespan import LifespanConductor


class LifespanConductorProvider(DeferredProvider):
    @classmethod
    def provides(cls):
        return ["lifespan_conductor"]

    def __init__(self, application):
        self.application = application

    def register(self):
        """Register Lifespan Conductor."""
        self.application.bind(
            "lifespan_conductor",
            LifespanConductor(self.application),
        )
