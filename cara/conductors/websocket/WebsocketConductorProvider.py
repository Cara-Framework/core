"""
Websocket Conductor Provider for the Cara framework.

This module provides the service provider that binds the WebsocketConductor into the application
container.
"""

from __future__ import annotations

from cara.conductors.websocket.WebsocketConductor import WebsocketConductor
from cara.foundation import DeferredProvider


class WebsocketConductorProvider(DeferredProvider):
    """Service provider to bind WebsocketConductor into the application container."""

    @classmethod
    def provides(cls):
        return ["websocket_conductor"]

    def __init__(self, application):
        self.application = application

    def register(self) -> None:
        self.application.bind(
            "websocket_conductor",
            WebsocketConductor(self.application),
        )
