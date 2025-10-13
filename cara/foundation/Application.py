"""
Core Application Module.

This module contains the Application class which serves as the central hub of the Cara framework. It
manages the container, service providers, and implements the ASGI interface for handling HTTP
requests and lifespan events. The Application class is responsible for bootstrapping the framework
and coordinating all its components.
"""

import inspect
from typing import Any, Dict, List, Optional, Type

from cara.container import Container
from cara.environment import LoadEnvironment

# Lazy import to avoid circular imports
from cara.foundation import DeferredProvider, Provider
from cara.support.PathManager import PathManager


class Application(Container):
    """
    Core Application class.

    Extends the Container so that:
      - Non‐deferred providers register immediately.
      - Deferred providers (those subclassing DeferredProvider) are stored in
        deferred_providers and only registered when you first request them,
        whether by string key or by type hint.

    Implements the ASGI interface and exposes `make(...)` for dependency resolution.
    """

    def __init__(self, base_path: Optional[str] = None):
        """Initialize the application (and its underlying Container)."""
        super().__init__()  # Initialize Container internals

        # Load environment variables early, before any providers
        LoadEnvironment()

        self.base_path = base_path
        self.storage_path = None

        # Set the base path in PathManager so paths() function works correctly
        if base_path:
            PathManager.set_base_path(base_path)

        # All providers that have been eagerly instantiated & registered
        self.providers: List[Provider] = []
        # Map of deferred binding key (string) → provider class
        self.deferred_providers: Dict[str, Type[DeferredProvider]] = {}

    def register_providers(self, *providers: Type[Any]) -> "Application":
        """
        Register framework‐internal service providers.

        - If provider_class is a DeferredProvider, store its provides() keys
          in deferred_providers (do NOT instantiate/register now).
        - Otherwise, instantiate and call register() immediately.
        """
        for provider_class in providers:
            if issubclass(provider_class, DeferredProvider):
                # For each key this provider "provides", defer registration
                for binding_key in provider_class.provides():
                    self.deferred_providers[binding_key] = provider_class
            else:
                provider = provider_class(self)
                provider.register()
                self.providers.append(provider)

        return self

    def add_providers(self, *providers: Type[Any]) -> "Application":
        """
        Register application‐specific providers.

        Same logic as register_providers: deferred go into deferred_providers,
        others register immediately.
        """
        for provider_class in providers:
            if issubclass(provider_class, DeferredProvider):
                for binding_key in provider_class.provides():
                    self.deferred_providers[binding_key] = provider_class
            else:
                provider = provider_class(self)
                provider.register()
                self.providers.append(provider)

        return self

    def make(self, name: Any, *arguments: Any) -> Any:
        """
        Override Container.make(...) so that:

          - If `name` is a string and is deferred, register+boot that provider now.
          - If `name` is a class and its lowercase name appears in deferred_providers,
            register+boot that provider now (allows type‐hint injection).
          - Then delegate to super().make(name, ...).

        This way, deferred providers fire when you first request them by key or by class.
        """
        # 1) If caller is requesting by class
        if inspect.isclass(name):
            # Convert class to lowercase key
            key_str = name.__name__.lower()
            if key_str in self.deferred_providers:
                provider_class = self.deferred_providers.pop(key_str)
                # Remove any other keys pointing to same provider_class
                for k, cls in list(self.deferred_providers.items()):
                    if cls is provider_class:
                        self.deferred_providers.pop(k, None)
                provider = provider_class(self)
                provider.register()
                self.providers.append(provider)
                if hasattr(provider, "boot"):
                    provider.boot()
            # Now that (maybe) registered, delegate to Container.make (which can resolve class)
            return super().make(name, *arguments)

        # 2) If caller is requesting by string
        if isinstance(name, str) and name in self.deferred_providers:
            provider_class = self.deferred_providers.pop(name)
            # Remove any other keys pointing to same provider_class
            for k, cls in list(self.deferred_providers.items()):
                if cls is provider_class:
                    self.deferred_providers.pop(k, None)
            provider = provider_class(self)
            provider.register()
            self.providers.append(provider)
            if hasattr(provider, "boot"):
                provider.boot()

        # 3) Delegate to base Container.make
        return super().make(name, *arguments)

    def has(self, name: Any) -> bool:
        """
        Check if a binding exists (including deferred providers).

        Override Container.has() to also check deferred_providers.
        """
        # Check regular bindings first
        if super().has(name):
            return True

        # Check deferred providers
        if isinstance(name, str) and name in self.deferred_providers:
            return True

        return False

    def boot_providers(self) -> None:
        """
        Call boot() on all already‐registered (non‐deferred) providers.
        Also initialize router to catch route registration errors early.
        """
        for provider in self.providers:
            if hasattr(provider, "boot"):
                provider.boot()

        # Initialize router early to catch route registration errors
        try:
            self.router = self.make("router")
        except Exception as e:
            # Check if it's a RouteRegistrationException
            from cara.exceptions import RouteRegistrationException

            if isinstance(e, RouteRegistrationException):
                raise RuntimeError(
                    f"Application startup failed due to route configuration: {e}"
                )
            else:
                raise RuntimeError(f"Application startup failed: {e}")

    async def __call__(self, scope: dict, receive: Any, send: Any) -> None:
        """ASGI application interface: delegate to HTTP or lifespan conductor."""
        self.initialize_conductors()
        self.add_app_to_scope(scope)
        await self.handle_request(scope, receive, send)

    def initialize_conductors(self):
        """Ensure http_conductor and lifespan_conductor are resolved (may fire deferred)."""
        # Request by string "http_conductor" → may register that deferred provider now
        self.http_conductor = self.make("http_conductor")
        if not self.http_conductor:
            raise Exception("HTTP conductor must be registered.")

        # Request by string "lifespan_conductor" → may register that deferred provider now
        self.lifespan_conductor = self.make("lifespan_conductor")
        if not self.lifespan_conductor:
            raise Exception("Lifespan conductor must be registered.")

        self.websocket_conductor = self.make("websocket_conductor")
        if not self.websocket_conductor:
            raise Exception("Websocket conductor must be registered.")

    def add_app_to_scope(self, scope: dict) -> None:
        """Attach application instance into ASGI scope for middleware/handlers."""
        scope["app"] = self

    async def handle_request(self, scope: dict, receive: Any, send: Any) -> None:
        """Delegate to appropriate conductor based on scope['type']."""
        scope_type = scope["type"]

        if scope_type == "http":
            await self.http_conductor.handle(scope, receive, send)
        elif scope_type == "lifespan":
            await self.lifespan_conductor.handle(scope, receive, send)
        elif scope_type == "websocket":
            await self.websocket_conductor.handle(scope, receive, send)
        else:
            # Unknown ASGI scope type, close connection
            await send({"type": "websocket.close", "code": 1002})
