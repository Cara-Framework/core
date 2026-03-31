"""
HTTP Conductor Provider for the Cara framework.

This module provides the service provider that bootstraps the HTTP conductor system. It is
responsible for registering the HTTP conductor in the application container and managing its
lifecycle.

The provider follows Laravel's service provider pattern, allowing for clean separation of HTTP
handling concerns.
"""

from cara.foundation import DeferredProvider
from cara.conductors.http import HttpConductor


class HttpConductorProvider(DeferredProvider):
    @classmethod
    def provides(cls):
        return ["http_conductor"]

    def __init__(self, application):
        self.application = application

    def register(self):
        """Register HTTP Conductor."""
        self.application.bind(
            "http_conductor",
            HttpConductor(self.application),
        )
