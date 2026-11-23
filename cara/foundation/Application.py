"""
Core Application Module.

This module contains the Application class which serves as the central hub of the Cara framework. It
manages the container, service providers, and implements the ASGI interface for handling HTTP
requests and lifespan events. The Application class is responsible for bootstrapping the framework
and coordinating all its components.
"""

from __future__ import annotations

import inspect
from typing import Any

from cara.container import Container
from cara.environment import LoadEnvironment, PathManager

# Direct module imports — NOT ``from cara.foundation import ...``. Application
# is imported by ``cara.foundation.__init__`` BEFORE that package binds the
# ``Provider`` / ``DeferredProvider`` CLASSES, so the package import resolves
# to the SUBMODULES (modules, not classes) and any subclassing downstream
# raises "module() takes at most 2 arguments (3 given)". The direct paths are
# immune to ``__init__`` import ordering.
from cara.foundation.DeferredProvider import DeferredProvider
from cara.foundation.Provider import Provider


def _cara_exception(message: str):
    """Construct the framework root error after foundation boot completes."""
    from cara.exceptions.types.CaraException import (  # local: cycle with cara.exceptions
        CaraException,
    )

    return CaraException(message)


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

    def __init__(self, base_path: str | None = None):
        """Initialize the application (and its underlying Container)."""
        super().__init__()  # Initialize Container internals

        # Load environment variables early, before any providers
        LoadEnvironment(base_path=base_path)

        self.base_path = base_path
        self.storage_path = None

        # Set the base path in PathManager so paths() function works correctly
        if base_path:
            PathManager.set_base_path(base_path)

        # All providers that have been eagerly instantiated & registered
        self.providers: list[Provider] = []
        # Map of deferred binding key (string) → provider class
        self.deferred_providers: dict[str, type[DeferredProvider]] = {}

    def register_providers(self, *providers: type[Any]) -> Application:
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

    def add_providers(self, *providers: type[Any]) -> Application:
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
            register+boot that provider now (allows type-hint injection).
          - Then delegate to super().make(name, ...).

        This way, deferred providers fire when you first request them by key or by class.

        The deferred provider is loaded by ``Container.load_deferred``, which
        closes the race of 2026-04-24 (thread A has popped the key and is
        inside ``register()``; thread B finds the key in neither table and
        raises ``MissingContainerBindingException`` — the "Facade 'cache'
        could not resolve 'get'" spray under ``--concurrency=8``) by making B
        WAIT for A's registration. This method used to close it by holding
        one lock across the provider AND ``super().make()``, which serialised
        every resolution in the process behind whichever constructor was
        running; nothing is held across either now.
        """
        key = name.__name__.lower() if inspect.isclass(name) else name
        if isinstance(key, str):
            self.load_deferred(self.deferred_providers, key)
        return super().make(name, *arguments)

    def has(self, name: Any) -> bool:
        """
        Check if a binding exists (including deferred providers).

        Override Container.has() to also check deferred_providers.
        """
        # Check regular bindings first
        if super().has(name):
            return True

        # Check deferred providers — mirror make()'s resolution: a class
        # resolves through its lowercased name, so has(SomeClass) must
        # agree with make(SomeClass) instead of reporting False.
        # A provider mid-registration has left ``deferred_providers`` and is
        # not bound yet; it is in ``_loading``, and ``make`` would wait for it.
        key = name.__name__.lower() if inspect.isclass(name) else name
        if not isinstance(key, str):
            return False
        return key in self.deferred_providers or key in self._loading

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
            from cara.exceptions import (  # local: cycle with cara.exceptions
                RouteRegistrationException,
            )

            if isinstance(e, RouteRegistrationException):
                raise _cara_exception(
                    f"Application startup failed due to route configuration: {e}"
                ) from e
            else:
                raise _cara_exception(f"Application startup failed: {e}") from e

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
            raise _cara_exception("HTTP conductor must be registered.")

        # Request by string "lifespan_conductor" → may register that deferred provider now
        self.lifespan_conductor = self.make("lifespan_conductor")
        if not self.lifespan_conductor:
            raise _cara_exception("Lifespan conductor must be registered.")

        self.websocket_conductor = self.make("websocket_conductor")
        if not self.websocket_conductor:
            raise _cara_exception("Websocket conductor must be registered.")

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
