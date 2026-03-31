"""
Route provider for registering and managing routes.

This module handles route registration, controller discovery, and route group management in the Cara
framework. It supports controller-based routing and route decorators.
"""

from typing import List

from cara.foundation import DeferredProvider
from cara.routing import Router
from cara.routing.loaders import (
    ControllerRouteLoader,
    ExplicitRouteLoader,
    FunctionRouteLoader,
)


class RouteProvider(DeferredProvider):
    """
    Deferred provider for the routing subsystem.

    Registers Router and loads routes from various sources.
    """

    @classmethod
    def provides(cls) -> List[str]:
        return ["router"]

    def register(self) -> None:
        """Register routing services and load all routes."""
        # Bind router instance
        self._bind_router()

        # Load routes from different sources
        all_routes = self._load_all_routes()

        # Register routes with router
        self.application.make("router").add(all_routes)

    def _bind_router(self) -> None:
        """Bind Router instance to application container."""
        router = Router(self.application)
        self.application.bind("router", router)

    def _load_all_routes(self) -> List:
        """Load routes from all sources."""
        explicit_loader = ExplicitRouteLoader(self.application)
        controller_loader = ControllerRouteLoader(self.application)
        function_loader = FunctionRouteLoader(self.application)

        # Load all routes
        explicit_routes = explicit_loader.load()
        controller_routes = controller_loader.load()
        function_routes = function_loader.load()

        # Clear function route decorators after loading
        function_loader.clear()

        return explicit_routes + controller_routes + function_routes
